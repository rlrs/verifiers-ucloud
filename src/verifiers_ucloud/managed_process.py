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
from verifiers.v1.errors import SandboxError
from verifiers.v1.runtimes.base import RuntimeProcess

from .recovery import read_managed_logs, wait_for_managed_job
from .supervision import with_relay

OFFLINE_PREFIX = "/opt/verifiers-offline/"

# Runs inside the sandbox with the program's own interpreter: feeds the mailbox
# to the child's stdin in order and forwards signals to its process group.
BRIDGE = r"""
import os, pathlib, signal, subprocess, sys, threading, time
mailbox = pathlib.Path(sys.argv[1])
child = subprocess.Popen(sys.argv[2:], stdin=subprocess.PIPE, start_new_session=True)
def stop(sig, frame):
    try: os.killpg(child.pid, sig)
    except ProcessLookupError: pass
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGUSR1, lambda sig, frame: stop(signal.SIGKILL, frame))
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
sys.exit(child.wait())
"""

_TRANSITIONS = {"parked", "parking", "unparking", "resuming", "waking", "suspending"}


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
        self._write_lock = asyncio.Lock()
        self.stdout = self._logs("stdout")
        self.stderr = self._logs("stderr")

    @classmethod
    async def start(cls, runtime, argv: list[str], env: dict[str, str]):
        sandbox = runtime.sandbox
        if sandbox is None:
            raise SandboxError("Managed process requires a started managed sandbox")
        # The bridge runs with the program's interpreter (argv[0]): ACP prepares
        # its script with activate=False, so argv starts with that Python.
        if not argv or not prepared_interpreter(runtime, argv[0]):
            raise SandboxError(
                "Managed interactive processes need an interpreter this runtime "
                "prepared (staged portable Python)"
            )
        mailbox = f"/tmp/vf-managed-{uuid.uuid4().hex}"
        result = await runtime.run(["mkdir", "-m", "700", mailbox], {})
        if result.exit_code:
            raise SandboxError("Could not create managed process mailbox")
        bridge = f"{mailbox}/bridge.py"
        await runtime.write(bridge, BRIDGE.encode())
        job = await with_relay(
            sandbox.start_agent(
                [argv[0], "-I", bridge, mailbox, *argv],
                env=runtime.process_env(env),
                working_dir=runtime.config.workdir,
                max_stdout_bytes=128 * 1024 * 1024,
                max_stderr_bytes=8 * 1024 * 1024,
            )
        )
        return cls(runtime, job, mailbox)

    async def _logs(self, stream: str):
        client = self.runtime._client_or_raise()
        offset = 0
        while True:
            state = await with_relay(client.get_sandbox_status(self.job.sandbox_id))
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
                in_transition = "lifecycle transition is in progress" in str(error)
                if error.status_code == 400 and in_transition:
                    await asyncio.sleep(2)
                    continue
                raise
            stalled = chunk.data and chunk.next_offset <= offset
            if chunk.next_offset < offset or stalled:
                raise SandboxError("Managed process log cursor did not advance")
            offset = chunk.next_offset
            if chunk.data:
                yield chunk.data
            if chunk.eof:
                record = await with_relay(self.job.refresh())
                if record.stdout_truncated or record.stderr_truncated:
                    raise SandboxError("Managed interactive process output truncated")
                if record.terminal:
                    return
            if not chunk.data:
                await asyncio.sleep(2)

    async def write(self, data: bytes) -> None:
        async with self._write_lock:
            path = f"{self.mailbox}/{self._index}"
            await self.runtime.write(path + ".tmp", data)
            result = await self.runtime.run(
                ["mv", "--", path + ".tmp", path + ".ready"], {}
            )
            if result.exit_code:
                raise SandboxError("Could not publish managed process input")
            self._index += 1

    async def wait(self) -> int:
        record = await with_relay(wait_for_managed_job(self.job, poll_seconds=1))
        return record.exit_code if record.exit_code is not None else 1

    async def terminate(self) -> None:
        await self.job.signal(15)

    async def kill(self) -> None:
        # The bridge turns SIGUSR1 into SIGKILL for the child's process group.
        await self.job.signal(10)
