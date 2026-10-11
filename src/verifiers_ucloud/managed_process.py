"""Interactive processes as managed jobs, so the sandbox can park while they wait.

A live exec holds a connection to a running sandbox, so an ACP harness that
waits minutes for each model answer keeps its sandbox resident. Run instead as
a managed job, the gateway can pause or park the sandbox through those waits:
stdin goes through a mailbox directory in the sandbox (one file per write,
published by rename), and stdout/stderr are read from the job's logs.
"""

from __future__ import annotations

import asyncio
import time
import uuid

from ucloud_sandboxes_sdk import SandboxApiError, job_read_delay
from verifiers.v1.errors import SandboxError, TunnelError
from verifiers.v1.runtimes.base import RuntimeProcess

from .recovery import (
    RecoveryWindow,
    node_lost,
    read_managed_log_page,
)
from .supervision import with_relay

OFFLINE_PREFIX = "/opt/verifiers-offline/"

# Runs inside the sandbox with the program's own interpreter: feeds the mailbox
# to the child's stdin in order and forwards signals to its process group.
BRIDGE = r"""
import os, pathlib, signal, subprocess, sys, threading, time
mailbox = pathlib.Path(sys.argv[1])
child, pending = None, []
def stop(sig, frame):
    if child is None:
        pending.append(sig)
        return
    try: os.killpg(child.pid, sig)
    except ProcessLookupError: pass
# Installed before the child starts: a signal in between is forwarded once it runs.
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGUSR1, lambda sig, frame: stop(signal.SIGKILL, frame))
child = subprocess.Popen(sys.argv[2:], stdin=subprocess.PIPE, start_new_session=True)
for sig in pending:
    stop(sig, None)
def feed():
    index = 0
    while child.poll() is None:
        path = mailbox / (str(index) + ".ready")
        if not path.exists():
            time.sleep(0.1)
            continue
        data = path.read_bytes()
        try:
            child.stdin.write(data)
            child.stdin.flush()
        except BrokenPipeError:
            return
        index += 1
threading.Thread(target=feed, daemon=True).start()
code = child.wait()
sys.exit(128 - code if code < 0 else code)  # A signalled child exits 128+N.
"""

_TRANSITIONS = {"parked", "parking", "unparking", "resuming", "waking", "suspending"}
# How long a log read may be held at the gateway until there is output (the
# SDK's own follow() asks the same; the gateway caps a wait at 25 s).
_WAIT_SECONDS = 20.0
# Fallback pacing only: how long to pause after an answer that came back at once
# without output (a gateway that does not hold reads), and how often to ask such
# a gateway the job's status.
_POLL_SECONDS = 1.0
# How long a run of answers the gateway marks retryable is retried.
_RECOVERY_SECONDS = 120.0


async def _job_call(operation, what: str):
    """Await a gateway call for the managed process as a verifiers SandboxError
    (SandboxNodeLost for a lost node)."""
    try:
        return await with_relay(operation)
    except (SandboxError, TunnelError):
        raise
    except Exception as error:
        if (lost := node_lost(error)) is not None:
            raise lost from error
        raise SandboxError(
            f"Managed interactive process {what} failed: {error}"
        ) from error


def prepared_interpreter(runtime, interpreter: str) -> bool:
    """Whether `interpreter` is one this runtime staged: a uv script environment it
    prepared (in an open sandbox, a toolkit or the offline bundle), or anything
    under the offline bundle or the configured uv toolkit."""
    if interpreter in runtime._uv_interpreters.values():
        return True
    prefixes = [OFFLINE_PREFIX]
    if runtime.config.uv_toolkit is not None:
        prefixes.append(f"/opt/ucloud/toolkits/{runtime.config.uv_toolkit}/")
    return interpreter.startswith(tuple(prefixes))


class _Transition(Exception):
    """The sandbox is between lifecycle states: read again in 2 s."""


class ManagedProcess(RuntimeProcess):
    """A managed job as a live process.

    One follower task per process reads both streams: it follows stdout through
    log reads the gateway holds until there is output, and reads stderr (without
    waiting) whenever the record carried by a read shows it grew. Its terminal
    record answers `wait()`. That is one held request per process and no job
    status polling. A paused or parked sandbox stays so while its process waits
    for the model: a held read does not wake it, and bytes a record shows beyond
    what was read (which a parked image may still hold) are read only once the
    sandbox's status, which never wakes it, shows it running. Gateways before
    0.9.72 neither hold reads nor carry records: there the follower polls the
    job's status, once for both streams and `wait()`.
    """

    def __init__(self, runtime, job, mailbox: str) -> None:
        self.runtime, self.job, self.mailbox = runtime, job, mailbox
        self._index = 0
        self._input_failed: BaseException | None = None
        self._write_lock = asyncio.Lock()
        self._queues: dict[str, asyncio.Queue[bytes | None]] = {
            "stdout": asyncio.Queue(),
            "stderr": asyncio.Queue(),
        }
        self._follower: asyncio.Task | None = None
        self._done = asyncio.Event()
        self._record = None
        self._error: BaseException | None = None
        self.stdout = self._stream("stdout")
        self.stderr = self._stream("stderr")

    @classmethod
    async def start(cls, runtime, argv: list[str], env: dict[str, str]):
        sandbox = runtime.sandbox
        if sandbox is None:
            raise SandboxError("Managed process requires a started managed sandbox")
        mailbox = f"/tmp/vf-managed-{uuid.uuid4().hex}"
        result = await runtime.run(["mkdir", "-m", "700", mailbox], {})
        if result.exit_code:
            raise SandboxError("Could not create managed process mailbox")
        bridge = f"{mailbox}/bridge.py"
        await runtime.write(bridge, BRIDGE.encode())
        job = await _job_call(
            sandbox.start_agent(
                [argv[0], "-I", bridge, mailbox, *argv],
                env=runtime.process_env(env),
                working_dir=runtime.config.workdir,
                max_stdout_bytes=128 * 1024 * 1024,
                max_stderr_bytes=8 * 1024 * 1024,
            ),
            "start",
        )
        return cls(runtime, job, mailbox)

    def _follow(self) -> None:
        """Start the follower, once, when the output or the exit is first asked for."""
        if self._follower is None:
            self._follower = asyncio.create_task(self._run_follower())

    async def _stream(self, stream: str):
        self._follow()
        queue = self._queues[stream]
        while (data := await queue.get()) is not None:
            yield data
        if self._error is not None:
            raise self._error

    async def _run_follower(self) -> None:
        try:
            self._record = await self._follow_job()
        except asyncio.CancelledError:
            self._error = SandboxError("Managed interactive process follower cancelled")
            raise
        except Exception as error:
            if isinstance(error, (SandboxError, TunnelError)):
                self._error = error
            elif (lost := node_lost(error)) is not None:
                self._error = lost
            else:
                self._error = SandboxError(
                    f"Managed interactive process output failed: {error}"
                )
                self._error.__cause__ = error
        finally:
            self._done.set()
            for queue in self._queues.values():
                queue.put_nowait(None)

    async def _follow_job(self):
        """Feed both streams until the job ends; return its terminal record."""
        record = self.job.record
        offsets = {"stdout": 0, "stderr": 0}
        window = RecoveryWindow(self.job, _RECOVERY_SECONDS)
        while True:
            started = time.monotonic()
            chunk = await self._read(
                "stdout", offsets["stdout"], record, window, _WAIT_SECONDS
            )
            elapsed = time.monotonic() - started
            offsets["stdout"] = self._feed("stdout", offsets["stdout"], chunk, record)
            if chunk.job is None:
                # A gateway before 0.9.72: its reads carry no record and are not
                # held. Poll the job's status as before.
                return await self._poll_job(offsets, window)
            record = self._checked(chunk.job)
            await self._catch_up("stderr", offsets, record, window)
            # The SDK's pacing: None once the job ended and stdout is read to
            # its end; a parked sandbox's Retry-After; a pause after an answer
            # that came back at once without output; else read again at once.
            delay = job_read_delay(
                chunk,
                requested_seconds=_WAIT_SECONDS,
                elapsed_seconds=elapsed,
                poll_seconds=_POLL_SECONDS,
            )
            if delay is None:
                return record
            if delay > 0:
                await asyncio.sleep(delay)

    async def _poll_job(self, offsets: dict[str, int], window: RecoveryWindow):
        """Follow through status polls (gateways before 0.9.72): the log, whose
        reads wake the sandbox there, is read only once the status shows new
        bytes."""
        while True:
            try:
                record = await with_relay(self.job.refresh())
            except SandboxApiError as error:
                window.failed(error)
                await asyncio.sleep(_POLL_SECONDS)
                continue
            window.succeeded()
            # Byte counts are as of the status: after the job ends they are final.
            record = self._checked(record)
            for stream in ("stdout", "stderr"):
                await self._catch_up(stream, offsets, record, window)
            if record.terminal:
                return record
            await asyncio.sleep(_POLL_SECONDS)

    async def _catch_up(self, stream, offsets, record, window) -> None:
        """Read `stream` up to the byte count `record` shows, without waiting."""
        while offsets[stream] < _bytes(record, stream):
            chunk = await self._read(stream, offsets[stream], record, window, None)
            offsets[stream] = self._feed(stream, offsets[stream], chunk, record)

    def _feed(self, stream: str, offset: int, chunk, record) -> int:
        """Queue a read's bytes; return the stream's next offset."""
        if chunk.next_offset <= offset and offset < _bytes(record, stream):
            raise SandboxError("Managed process log cursor did not advance")
        if chunk.data:
            self._queues[stream].put_nowait(chunk.data)
        return max(offset, chunk.next_offset)

    @staticmethod
    def _checked(record):
        if record.stdout_truncated or record.stderr_truncated:
            raise SandboxError("Managed interactive process output truncated")
        return record

    async def _read(self, stream, offset, record, window, wait_seconds):
        """One log read at `offset`, repeated through lifecycle transitions and
        through answers the gateway marks retryable (for `window`)."""
        while True:
            try:
                # Bytes the last record shows beyond `offset` may exist only in
                # the parked image, and reading them would wake the sandbox.
                if offset < _bytes(record, stream):
                    await self._running()
                chunk = await with_relay(
                    read_managed_log_page(
                        self.job, stream, offset=offset, wait_seconds=wait_seconds
                    )
                )
            except _Transition:
                await asyncio.sleep(2)
                continue
            except SandboxApiError as error:
                transition = "lifecycle transition is in progress" in str(error)
                if error.status_code == 400 and transition:
                    await asyncio.sleep(2)
                    continue
                try:
                    window.failed(error)
                except SandboxApiError:
                    raise SandboxError(
                        f"Managed interactive process {stream} read failed: {error}"
                    ) from error
                await asyncio.sleep(_POLL_SECONDS)
                continue
            window.succeeded()
            return chunk

    async def _running(self) -> None:
        """Raise _Transition unless the sandbox is running (its status never
        wakes it)."""
        client = self.runtime._client_or_raise()
        state = await _job_call(
            client.get_sandbox_status(self.job.sandbox_id), "status"
        )
        if state is None:
            raise SandboxError("Managed process sandbox no longer exists")
        if state.get("state") in _TRANSITIONS:
            raise _Transition

    async def write(self, data: bytes) -> None:
        async with self._write_lock:
            if self._input_failed is not None:
                raise SandboxError(
                    "Managed process input stopped after a failed write"
                ) from self._input_failed
            path = f"{self.mailbox}/{self._index}"
            try:
                await self.runtime.write(path + ".tmp", data)
                result = await self.runtime.run(
                    ["mv", "--", path + ".tmp", path + ".ready"], {}
                )
                if result.exit_code:
                    raise SandboxError("Could not publish managed process input")
            except BaseException as error:
                # Whether this input reached the bridge is unknown, and the bridge
                # reads inputs strictly in order: publishing another could reuse
                # this index or leave a gap it waits on forever.
                self._input_failed = error
                raise
            self._index += 1

    async def wait(self) -> int:
        self._follow()
        # Waiting on the event never cancels the follower: a caller's timeout
        # (ACP's stop) leaves the streams followed.
        await with_relay(self._done.wait())
        if self._error is not None:
            raise self._error
        record = self._record
        assert record is not None  # the follower ends with a record or an error
        return record.exit_code if record.exit_code is not None else 1

    async def terminate(self) -> None:
        await _job_call(self.job.signal(15), "terminate")

    async def kill(self) -> None:
        # The bridge turns SIGUSR1 into SIGKILL for the child's process group.
        await _job_call(self.job.signal(10), "kill")


def _bytes(record, stream: str) -> int:
    return getattr(record, f"{stream}_bytes", 0) or 0
