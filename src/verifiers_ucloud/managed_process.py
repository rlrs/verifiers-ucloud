"""Interactive processes as managed jobs, so the sandbox can park while they wait.

A live exec holds a connection to a running sandbox, so an ACP harness that
waits minutes for each model answer keeps its sandbox resident. Run instead as
a managed job, the gateway can pause or park the sandbox through those waits:
stdin goes through a mailbox directory in the sandbox (one file per write,
published by rename), and stdout/stderr are read from the job's logs.
"""

from __future__ import annotations

import asyncio
import uuid

from ucloud_sandboxes_sdk import SandboxApiError
from verifiers.v1.errors import SandboxError, TunnelError
from verifiers.v1.runtimes.base import RuntimeProcess

from .recovery import node_lost, read_managed_logs, wait_for_managed_job
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
# How often a waiting stream asks the job's status (which never wakes the sandbox).
_POLL_SECONDS = 1.0


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


class ManagedProcess(RuntimeProcess):
    def __init__(self, runtime, job, mailbox: str) -> None:
        self.runtime, self.job, self.mailbox = runtime, job, mailbox
        self._index = 0
        self._input_failed: BaseException | None = None
        self._write_lock = asyncio.Lock()
        self.stdout = self._logs("stdout")
        self.stderr = self._logs("stderr")

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

    async def _logs(self, stream: str):
        """The stream's output as it is written. Waiting polls the job's status,
        which never wakes the sandbox; the log, whose reads do wake it, is read
        only once the status shows new bytes. A paused or parked sandbox
        therefore stays so while its process waits for the model."""
        client = self.runtime._client_or_raise()
        offset = 0
        while True:
            record = await _job_call(self.job.refresh(), "status")
            if record.stdout_truncated or record.stderr_truncated:
                raise SandboxError("Managed interactive process output truncated")
            # Byte counts are as of the status: after the job ends they are final.
            available = getattr(record, f"{stream}_bytes", 0)
            while offset < available:
                state = await _job_call(
                    client.get_sandbox_status(self.job.sandbox_id), "status"
                )
                if state is None:
                    raise SandboxError("Managed process sandbox no longer exists")
                if state.get("state") in _TRANSITIONS:
                    await asyncio.sleep(2)
                    continue
                try:
                    chunk = await with_relay(
                        read_managed_logs(self.job, stream, offset=offset)
                    )
                except SandboxApiError as error:
                    transition = "lifecycle transition is in progress" in str(error)
                    if error.status_code == 400 and transition:
                        await asyncio.sleep(2)
                        continue
                    raise SandboxError(
                        f"Managed interactive process {stream} read failed: {error}"
                    ) from error
                if chunk.next_offset <= offset:
                    raise SandboxError("Managed process log cursor did not advance")
                offset = chunk.next_offset
                yield chunk.data
            if record.terminal:
                return
            await asyncio.sleep(_POLL_SECONDS)

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
        record = await _job_call(wait_for_managed_job(self.job, poll_seconds=1), "wait")
        return record.exit_code if record.exit_code is not None else 1

    async def terminate(self) -> None:
        await _job_call(self.job.signal(15), "terminate")

    async def kill(self) -> None:
        # The bridge turns SIGUSR1 into SIGKILL for the child's process group.
        await _job_call(self.job.signal(10), "kill")
