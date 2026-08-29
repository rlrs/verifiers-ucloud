from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from typing import ClassVar, cast

import pytest
from verifiers.v1.configs.runtime import BaseRuntimeConfig
from verifiers.v1.interception import (
    BaseInterceptionConfig,
    find_interception_class,
    make_interception,
)
from verifiers.v1.runtimes import find_runtime_class, make_runtime
from verifiers.v1.session import RolloutSession

from verifiers_ucloud import (
    UCloudInterception,
    UCloudInterceptionConfig,
    UCloudRuntime,
    UCloudRuntimeConfig,
)


def test_verifiers_discovers_package_exports() -> None:
    find_runtime_class.cache_clear()
    find_interception_class.cache_clear()

    runtime_config = BaseRuntimeConfig.model_validate({"type": "ucloud", "cpu": 2})
    assert isinstance(runtime_config, UCloudRuntimeConfig)
    assert isinstance(make_runtime(runtime_config), UCloudRuntime)

    interception_config = BaseInterceptionConfig.model_validate({"type": "ucloud"})
    assert isinstance(interception_config, UCloudInterceptionConfig)
    assert isinstance(
        make_interception(interception_config, requires_tunnel=True),
        UCloudInterception,
    )


@dataclass
class _Result:
    exit_code: int | None = 0
    stdout: str = "ok"
    stderr: str = ""


class _ProcessStdin:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        pass


class _ProcessHandle:
    def __init__(self) -> None:
        self.terminated = False
        self.killed = False

    async def terminate(self) -> None:
        self.terminated = True

    async def kill(self) -> None:
        self.killed = True


class _Process:
    def __init__(self) -> None:
        self.stdin = _ProcessStdin()
        self.handle = _ProcessHandle()
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.stdout.feed_data(b"stdout")
        self.stdout.feed_eof()
        self.stderr.feed_data(b"stderr")
        self.stderr.feed_eof()

    async def wait(self) -> int:
        return 7


class _SandboxClient:
    instances: ClassVar[list[_SandboxClient]] = []

    def __init__(self, base_url: str, **kwargs) -> None:
        self.base_url = base_url
        self.kwargs = kwargs
        self.created = None
        self.create_kwargs: dict = {}
        self.execs: list[tuple[str, list[str], dict]] = []
        self.processes: list[tuple[str, list[str], dict, _Process]] = []
        self.uploads: list[tuple[str, str, bytes]] = []
        self.deleted: list[str] = []
        self.closed = False
        self.instances.append(self)

    @classmethod
    def from_env(cls, *, timeout_seconds: float):
        return cls("https://gateway.example", timeout_seconds=timeout_seconds)

    async def create_sandbox(self, spec, **kwargs):
        self.created = spec
        self.create_kwargs = kwargs
        return SimpleNamespace(id=spec.id)

    async def exec(self, sandbox_id: str, command: list[str], **kwargs):
        self.execs.append((sandbox_id, command, kwargs))
        return _Result()

    async def open_process(self, sandbox_id: str, command: list[str], **kwargs):
        process = _Process()
        self.processes.append((sandbox_id, command, kwargs, process))
        return process

    async def download_file(self, sandbox_id: str, path: str) -> bytes:
        return f"{sandbox_id}:{path}".encode()

    async def upload_file(self, sandbox_id: str, path: str, data: bytes) -> None:
        self.uploads.append((sandbox_id, path, data))

    async def delete_sandbox(self, sandbox_id: str) -> None:
        self.deleted.append(sandbox_id)

    async def close(self) -> None:
        self.closed = True


class _InterceptionServer:
    instances: ClassVar[list[_InterceptionServer]] = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.base_url = "http://127.0.0.1:1234"
        self.unregistered: list[tuple[str, str]] = []
        self.closed = False
        self.instances.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> None:
        self.closed = True

    def register(self, session) -> tuple[str, str]:
        return "model-secret", "state-secret"

    def unregister(self, model_secret: str, state_secret: str) -> None:
        self.unregistered.append((model_secret, state_secret))


class _RelaySession:
    def __init__(self, client: _RelayClient, rollout_id: str, **kwargs) -> None:
        self.client = client
        self.rollout_id = rollout_id
        self.kwargs = kwargs
        self.base_url = f"{client.relay_url}/managed/{rollout_id}"
        self.run_kwargs: dict | None = None

    async def __aenter__(self):
        self.client.registered.append(self.rollout_id)
        return self

    async def __aexit__(self, *exc) -> None:
        self.client.unregistered.append(self.rollout_id)

    async def run(self, **kwargs) -> None:
        self.run_kwargs = kwargs
        await kwargs["cancel"].wait()


class _RelayClient:
    instances: ClassVar[list[_RelayClient]] = []

    def __init__(self, relay_url: str, **kwargs) -> None:
        self.relay_url = relay_url.rstrip("/")
        self.kwargs = kwargs
        self.registered: list[str] = []
        self.unregistered: list[str] = []
        self.sessions: list[_RelaySession] = []
        self.closed = False
        self.instances.append(self)

    @classmethod
    def from_env(cls, *, env: dict[str, str], **kwargs):
        return cls(env["UCLOUD_RELAY_URL"], **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> None:
        self.closed = True

    def rollout_session(self, rollout_id: str, **kwargs) -> _RelaySession:
        session = _RelaySession(self, rollout_id, **kwargs)
        self.sessions.append(session)
        return session


@pytest.mark.asyncio
async def test_runtime_lifecycle_uses_gateway_sdk(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module

    _SandboxClient.instances.clear()
    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)

    runtime = UCloudRuntime(
        UCloudRuntimeConfig(cpu=2, memory=4, disk=8), name="sandbox-1"
    )
    runtime.env = {"RUNTIME": "yes"}
    await runtime.start()
    client = _SandboxClient.instances[-1]

    assert runtime.info.id == "sandbox-1"
    assert runtime.supports_live_processes
    assert client.created is not None
    spec = client.created.to_dict()
    assert spec["profile"] == "linux_host"
    assert spec["security"] is None
    assert spec["filesystem"] is None
    assert spec["command"] == []
    assert spec["cpus"] == 2
    assert spec["memory_mb"] == 4096
    assert spec["disk_mb"] == 8192
    assert spec["env"] == {"RUNTIME": "yes"}
    assert client.create_kwargs == {"request_timeout_seconds": 900.0}

    result = await runtime.run(["echo", "ok"], {"CALL": "yes"})
    assert result.stdout == "ok"
    assert client.execs[-1][2]["env"] == {"RUNTIME": "yes", "CALL": "yes"}

    process = await runtime.open_process(["agent"], {"PROCESS": "yes"})
    await process.write(b"request")
    assert b"".join([chunk async for chunk in process.stdout]) == b"stdout"
    assert b"".join([chunk async for chunk in process.stderr]) == b"stderr"
    assert await process.wait() == 7
    await process.terminate()
    await process.kill()
    sdk_process = client.processes[-1][3]
    assert sdk_process.stdin.writes == [b"request"]
    assert sdk_process.handle.terminated
    assert sdk_process.handle.killed

    await runtime.write("result.txt", b"done")
    assert client.uploads[-1] == ("sandbox-1", "/app/result.txt", b"done")
    assert await runtime.read("result.txt") == b"sandbox-1:/app/result.txt"

    await runtime.stop()
    assert client.deleted == ["sandbox-1"]
    assert client.closed


@pytest.mark.asyncio
async def test_interception_uses_managed_relay_session(monkeypatch) -> None:
    import verifiers_ucloud.interception as interception_module

    _InterceptionServer.instances.clear()
    _RelayClient.instances.clear()
    monkeypatch.setattr(interception_module, "InterceptionServer", _InterceptionServer)
    monkeypatch.setattr(interception_module, "AsyncRelayWorkerClient", _RelayClient)

    interception = UCloudInterception(
        UCloudInterceptionConfig(relay_url="https://relay.example/"),
        requires_tunnel=True,
        state_service_secrets=("shared-secret",),
    )
    await interception.start()
    session = cast(RolloutSession, SimpleNamespace(trace=SimpleNamespace(id="trace-1")))

    async with interception.acquire(session) as slot:
        assert slot == (
            "https://relay.example/managed/trace-1",
            "model-secret",
            "state-secret",
        )

    relay = _RelayClient.instances[-1]
    relay_session = relay.sessions[-1]
    server = _InterceptionServer.instances[-1]
    assert relay.registered == ["trace-1"]
    assert relay.unregistered == ["trace-1"]
    assert relay_session.kwargs["metadata"] == {"consumer": "verifiers"}
    assert relay_session.run_kwargs is not None
    assert relay_session.run_kwargs["upstream_base_url"] == server.base_url
    assert relay_session.run_kwargs["lease_seconds"] == 900.0
    assert server.unregistered == [("model-secret", "state-secret")]

    await interception.stop()
    assert relay.closed
    assert server.closed
