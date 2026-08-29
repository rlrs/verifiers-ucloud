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
from verifiers_ucloud.interception import _registration_token


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


def test_registration_token_is_explicitly_validated() -> None:
    assert (
        _registration_token({"rollout": {"registration_token": "secret"}}) == "secret"
    )
    with pytest.raises(RuntimeError, match="access token"):
        _registration_token({})


@dataclass
class _Result:
    exit_code: int | None = 0
    stdout: str = "ok"
    stderr: str = ""


class _SandboxClient:
    instances: ClassVar[list[_SandboxClient]] = []

    def __init__(self, base_url: str, **kwargs) -> None:
        self.base_url = base_url
        self.kwargs = kwargs
        self.created: dict | None = None
        self.execs: list[tuple[str, list[str], dict]] = []
        self.uploads: list[tuple[str, str, bytes]] = []
        self.deleted: list[str] = []
        self.closed = False
        self.instances.append(self)

    async def create_sandbox(self, **kwargs):
        self.created = kwargs
        return SimpleNamespace(id=kwargs["id"])

    async def get_sandbox(self, sandbox_id: str):
        return None

    async def exec(self, sandbox_id: str, command: list[str], **kwargs):
        self.execs.append((sandbox_id, command, kwargs))
        return _Result()

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


class _RelayClient:
    instances: ClassVar[list[_RelayClient]] = []

    def __init__(self, relay_url: str, **kwargs) -> None:
        self.relay_url = relay_url
        self.kwargs = kwargs
        self.registered: list[str] = []
        self.unregistered: list[str] = []
        self.closed = False
        self.instances.append(self)

    async def register_tunnel(self, tunnel_id: str, **kwargs) -> dict:
        self.registered.append(tunnel_id)
        return {"rollout": {"registration_token": "access/token"}}

    async def unregister_tunnel(self, tunnel_id: str) -> None:
        self.unregistered.append(tunnel_id)

    async def poll(self, tunnel_id: str, **kwargs):
        await asyncio.Event().wait()

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_runtime_lifecycle_uses_gateway_sdk(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module

    _SandboxClient.instances.clear()
    monkeypatch.setenv("UCLOUD_SANDBOX_API_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)

    runtime = UCloudRuntime(
        UCloudRuntimeConfig(cpu=2, memory=4, disk=8), name="sandbox-1"
    )
    runtime.env = {"RUNTIME": "yes"}
    await runtime.start()
    client = _SandboxClient.instances[-1]

    assert runtime.info.id == "sandbox-1"
    assert client.created is not None
    assert client.created["cpus"] == 2
    assert client.created["memory_mb"] == 4096
    assert client.created["disk_mb"] == 8192
    assert client.created["env"] == {"RUNTIME": "yes"}

    result = await runtime.run(["echo", "ok"], {"CALL": "yes"})
    assert result.stdout == "ok"
    assert client.execs[-1][2]["env"] == {"RUNTIME": "yes", "CALL": "yes"}

    await runtime.write("result.txt", b"done")
    assert client.uploads[-1] == ("sandbox-1", "/app/result.txt", b"done")
    assert await runtime.read("result.txt") == b"sandbox-1:/app/result.txt"

    await runtime.stop()
    assert client.deleted == ["sandbox-1"]
    assert client.closed


@pytest.mark.asyncio
async def test_interception_registers_relay_slot_and_cleans_up(monkeypatch) -> None:
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
    session = cast(
        RolloutSession, SimpleNamespace(trace=SimpleNamespace(id="trace /1"))
    )

    async with interception.acquire(session) as slot:
        assert slot == (
            "https://relay.example/tunnels/trace%20%2F1/_relay/access%2Ftoken",
            "model-secret",
            "state-secret",
        )

    relay = _RelayClient.instances[-1]
    server = _InterceptionServer.instances[-1]
    assert relay.registered == ["trace /1"]
    assert relay.unregistered == ["trace /1"]
    assert server.unregistered == [("model-secret", "state-secret")]

    await interception.stop()
    assert relay.closed
    assert server.closed
