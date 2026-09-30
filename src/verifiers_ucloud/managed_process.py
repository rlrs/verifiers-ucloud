"""Interactive processes backed by managed jobs, without a live exec connection."""

import asyncio
import uuid

from ucloud_sandboxes_sdk import SandboxApiError
from verifiers.v1.errors import SandboxError
from verifiers.v1.runtimes.base import RuntimeProcess

from .recovery import read_managed_logs, wait_for_managed_job
from .supervision import with_relay

# The mailbox and all child process state live inside the parkable sandbox.
# Only explicit writes use file/control operations; output uses managed job logs.
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


class ManagedProcess(RuntimeProcess):
    def __init__(self, runtime, job, mailbox):
        self.runtime, self.job, self.mailbox = runtime, job, mailbox
        self._index = 0
        self._write_lock = asyncio.Lock()
        self.stdout = self._logs("stdout")
        self.stderr = self._logs("stderr")

    @classmethod
    async def start(cls, runtime, argv, env):
        handle = runtime.info.sandbox_handle
        if handle is None:
            raise SandboxError("Managed process requires a validated parkable sandbox")
        mailbox = f"/tmp/vf-managed-{uuid.uuid4().hex}"
        # ACP's prepared argv starts with the pinned portable Python interpreter.
        if not argv or not argv[0].startswith("/opt/verifiers-offline/"):
            raise SandboxError(
                "Managed interactive processes require staged portable Python"
            )
        result = await runtime.run(["mkdir", "-m", "700", mailbox], {})
        if result.exit_code:
            raise SandboxError("Could not create managed process mailbox")
        bridge = f"{mailbox}/bridge.py"
        await runtime.write(bridge, BRIDGE.encode())
        job = await with_relay(
            handle.start_agent(
                [argv[0], "-I", bridge, mailbox, *argv],
                env=runtime.process_env(env),
                working_dir=runtime.config.workdir,
                max_stdout_bytes=128 * 1024 * 1024,
                max_stderr_bytes=8 * 1024 * 1024,
            )
        )
        return cls(runtime, job, mailbox)

    async def _logs(self, stream):
        offset = 0
        while True:
            state = await with_relay(
                self.runtime._client_or_raise().get_sandbox_status(self.job.sandbox_id)
            )
            if state is None:
                raise SandboxError("Managed process sandbox no longer exists")
            if state.get("state") in {
                "parked",
                "parking",
                "unparking",
                "resuming",
                "waking",
                "suspending",
            }:
                await asyncio.sleep(2)
                continue
            try:
                chunk = await with_relay(read_managed_logs(self.job, stream, offset=offset))
            except SandboxApiError as error:
                if (
                    error.status_code == 400
                    and "lifecycle transition is in progress" in str(error)
                ):
                    await asyncio.sleep(2)
                    continue
                raise
            if chunk.next_offset < offset or (
                chunk.data and chunk.next_offset <= offset
            ):
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

    async def write(self, data):
        async with self._write_lock:
            path = f"{self.mailbox}/{self._index}"
            await self.runtime.write(path + ".tmp", data)
            result = await self.runtime.run(
                ["mv", "--", path + ".tmp", path + ".ready"], {}
            )
            if result.exit_code:
                raise SandboxError("Could not publish managed process input")
            self._index += 1

    async def wait(self):
        record = await with_relay(wait_for_managed_job(self.job, poll_seconds=1))
        return record.exit_code if record.exit_code is not None else 1

    async def terminate(self):
        await self.job.signal(15)

    async def kill(self):
        await self.job.signal(10)
