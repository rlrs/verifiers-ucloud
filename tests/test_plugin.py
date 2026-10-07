from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from typing import ClassVar, cast

import pytest
from verifiers.v1.configs.harness import HarnessConfig
from verifiers.v1.configs.runtime import BaseRuntimeConfig
from verifiers.v1.errors import SandboxError
from verifiers.v1.harness import Harness
from verifiers.v1.interception import (
    BaseInterceptionConfig,
    find_interception_class,
    make_interception,
)
from verifiers.v1.runtimes import find_runtime_class, make_runtime
from verifiers.v1.runtimes.base import Runtime
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
        self.batches: list[tuple[str, dict[str, bytes], dict]] = []
        self.batch_error: Exception | None = None
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

    async def upload_files(self, sandbox_id: str, files, **kwargs) -> dict:
        self.batches.append((sandbox_id, dict(files), kwargs))
        if self.batch_error is not None:
            raise self.batch_error
        return {"ok": True, "files": len(files)}

    async def delete_sandbox(self, sandbox_id: str) -> None:
        self.deleted.append(sandbox_id)

    async def close(self) -> None:
        self.closed = True


class _SkillsHarness(Harness[HarnessConfig]):
    SUPPORTS_SKILLS = True

    async def launch(self, *args, **kwargs):
        raise NotImplementedError


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
        self.base_url = f"{client.relay_url}/managed/{rollout_id}/"
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
@pytest.mark.parametrize("image_kind", ["registry", "name"])
async def test_runtime_lifecycle_uses_gateway_sdk(monkeypatch, image_kind) -> None:
    import verifiers_ucloud.runtime as runtime_module

    _SandboxClient.instances.clear()
    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)

    runtime = UCloudRuntime(
        UCloudRuntimeConfig(cpu=2, memory=4, disk=8, image_reference_type=image_kind), name="sandbox-1"
    )
    runtime.env = {"RUNTIME": "yes"}
    await runtime.start()
    client = _SandboxClient.instances[-1]

    assert runtime.info.id == "sandbox-1"
    assert runtime.supports_live_processes
    assert client.created is not None
    assert (client.created.image.name is not None) == (image_kind == "name")
    assert (client.created.image.tag is not None) == (image_kind == "registry")
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


async def _started_runtime(monkeypatch) -> tuple[UCloudRuntime, _SandboxClient]:
    import verifiers_ucloud.runtime as runtime_module

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    runtime = UCloudRuntime(UCloudRuntimeConfig(group_create=False), name="sandbox-1")
    await runtime.start()
    return runtime, _SandboxClient.instances[-1]


@pytest.mark.asyncio
async def test_write_many_sends_one_archive_upload(monkeypatch) -> None:
    runtime, client = await _started_runtime(monkeypatch)

    await runtime.write_many(
        {
            "/skills/a/SKILL.md": b"a",
            "notes/b.txt": b"b",
            "/app//notes/b.txt": b"last",
            "/etc/c": b"c",
        }
    )

    # Relative paths resolve against the workdir; the last of two paths naming
    # one file wins, as with sequential writes; the default mode matches `write`.
    assert client.batches == [
        (
            "sandbox-1",
            {
                "/skills/a/SKILL.md": b"a",
                "/app//notes/b.txt": b"last",
                "/etc/c": b"c",
            },
            {"base_dir": "/"},
        )
    ]
    assert client.uploads == []


@pytest.mark.asyncio
async def test_write_many_without_files_sends_nothing(monkeypatch) -> None:
    runtime, client = await _started_runtime(monkeypatch)

    await runtime.write_many({})

    assert client.batches == []
    assert client.uploads == []


@pytest.mark.asyncio
async def test_write_many_wraps_errors(monkeypatch) -> None:
    runtime, client = await _started_runtime(monkeypatch)
    client.batch_error = ValueError("file path '/a' is a parent directory of '/a/b'")

    with pytest.raises(SandboxError, match=r"write 2 files: .*parent directory"):
        await runtime.write_many({"/a": b"", "/a/b": b""})

    runtime.info.id = None
    await runtime.write_many({})
    with pytest.raises(SandboxError, match="no id"):
        await runtime.write_many({"/a": b""})
    assert len(client.batches) == 1


@pytest.mark.asyncio
@pytest.mark.skipif(
    not hasattr(Runtime, "write_many"),
    reason="this verifiers installs skills with one write per file",
)
async def test_install_skills_is_one_archive_upload(monkeypatch, tmp_path) -> None:
    runtime, client = await _started_runtime(monkeypatch)
    skill = tmp_path / "alpha"
    (skill / "scripts").mkdir(parents=True)
    (skill / "SKILL.md").write_bytes(b"alpha")
    (skill / "scripts" / "run.sh").write_bytes(b"#!/bin/sh\n")
    (skill / "scripts" / "run.sh").chmod(0o755)

    harness = _SkillsHarness(HarnessConfig(skills=[skill]))
    await harness.install_skills(runtime, "/skills")

    assert client.batches == [
        (
            "sandbox-1",
            {
                "/skills/alpha/SKILL.md": b"alpha",
                "/skills/alpha/scripts/run.sh": b"#!/bin/sh\n",
            },
            {"base_dir": "/"},
        )
    ]
    assert client.uploads == []
    assert [command for _, command, _ in client.execs] == [
        ["chmod", "+x", "/skills/alpha/scripts/run.sh"]
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("relay_prefix", ["", "/relay"])
@pytest.mark.parametrize("guest_url", [None, "http://gateway-private:8092"])
@pytest.mark.parametrize("parkable", [False, True])
@pytest.mark.parametrize("restricted", [False, True])
async def test_interception_uses_managed_relay_session(monkeypatch, guest_url, parkable, restricted, relay_prefix) -> None:
    import verifiers_ucloud.interception as interception_module

    _InterceptionServer.instances.clear()
    _RelayClient.instances.clear()
    monkeypatch.setattr(interception_module, "InterceptionServer", _InterceptionServer)
    monkeypatch.setattr(interception_module, "AsyncRelayWorkerClient", _RelayClient)

    interception = UCloudInterception(
        UCloudInterceptionConfig(relay_url=f"https://relay.example{relay_prefix}/", guest_relay_url=guest_url),
        requires_tunnel=True,
        state_service_secrets=("shared-secret",),
    )
    await interception.start()
    handle = SimpleNamespace(id="sandbox-1", record={"generation": 1, "spec": {"parkable": True, "managed_process": True}})
    runtime_info = SimpleNamespace(parkable=parkable, sandbox_handle=handle, network_restricted=restricted)
    session = cast(RolloutSession, SimpleNamespace(trace=SimpleNamespace(id="trace-1", agent=SimpleNamespace(runtime=runtime_info))))

    async with interception.acquire(session) as slot:
        assert slot == (
            f"https://relay.example{relay_prefix}/managed/trace-1",
            "model-secret",
            "state-secret",
        )

    from verifiers.v1.interception.tunnel import using_host_tunnel
    runtime = UCloudRuntime(UCloudRuntimeConfig(
        image="python:3.11-slim", allow=[] if restricted else ["*"],
        guest_relay_url=guest_url,
    ))
    public = f"https://relay.example{relay_prefix}/managed/trace-1/mcp?state=capability"
    with using_host_tunnel(interception.config.host_tunnel()):
        expected = public.replace(f"https://relay.example{relay_prefix}", guest_url) if restricted and guest_url else public
        assert runtime.host_url(public) == expected
        assert runtime.host_url("https://unrelated.example/mcp") == "https://unrelated.example/mcp"
        if relay_prefix:
            assert runtime.host_url("https://relay.example/relay-other/mcp") == "https://relay.example/relay-other/mcp"

    relay = _RelayClient.instances[-1]
    assert relay.relay_url == f"https://relay.example{relay_prefix}"
    relay_session = relay.sessions[-1]
    server = _InterceptionServer.instances[-1]
    assert relay.registered == ["trace-1"]
    assert relay.unregistered == ["trace-1"]
    assert relay_session.kwargs["metadata"] == {"consumer": "verifiers"}
    assert relay_session.kwargs["sandbox"] is (handle if parkable else None)
    assert relay_session.run_kwargs is not None
    assert relay_session.run_kwargs["upstream_base_url"] == server.base_url
    assert relay_session.run_kwargs["lease_seconds"] == 900.0
    assert server.unregistered == [("model-secret", "state-secret")]

    await interception.stop()
    assert relay.closed
    assert server.closed

@pytest.mark.asyncio
async def test_runtime_cleans_up_after_loopback_setup_failure(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module
    from verifiers.v1.errors import SandboxError

    class FailedHostsClient(_SandboxClient):
        async def exec(self, sandbox_id, command, **kwargs):
            return _Result(exit_code=1, stdout="", stderr="read-only hosts file")

    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", FailedHostsClient)
    runtime = UCloudRuntime(UCloudRuntimeConfig(), name="failed-hosts")
    with pytest.raises(SandboxError, match="loopback hosts setup failed"):
        await runtime.start()
    client = FailedHostsClient.instances[-1]
    assert client.deleted == ["failed-hosts"]
    assert client.closed


@pytest.mark.asyncio
async def test_relay_worker_failure_logged_before_parent_cancellation(monkeypatch, caplog):
    import verifiers_ucloud.interception as module

    async def fail(self, **kwargs):
        await asyncio.sleep(0.01)
        raise RuntimeError("relay failure sentinel")

    monkeypatch.setattr(module, "InterceptionServer", _InterceptionServer)
    monkeypatch.setattr(module, "AsyncRelayWorkerClient", _RelayClient)
    monkeypatch.setattr(_RelaySession, "run", fail)
    interception = UCloudInterception(
        UCloudInterceptionConfig(relay_url="https://relay.example")
    )
    await interception.start()
    assert _RelayClient.instances[-1].kwargs["forward_timeout_seconds"] == 7200
    session = cast(RolloutSession, SimpleNamespace(trace=SimpleNamespace(id="failure-test")))
    from verifiers_ucloud.supervision import with_relay
    from verifiers.v1.errors import TunnelError
    try:
        with pytest.raises(TunnelError, match="relay failure sentinel"):
            async with interception.acquire(session):
                await with_relay(asyncio.sleep(1))
        assert "UCloud relay worker failed: rollout=failure-test" in caplog.text
        assert "relay failure sentinel" in caplog.text
    finally:
        await interception.stop()


@pytest.mark.asyncio
async def test_relay_failure_does_not_cancel_sibling_and_external_cancel_propagates():
    from verifiers_ucloud.supervision import relay_worker, with_relay
    from verifiers.v1.errors import TunnelError

    async def fail():
        await asyncio.sleep(0)
        raise RuntimeError("worker failed")

    async def affected():
        worker = asyncio.create_task(fail())
        token = relay_worker.set(worker)
        try:
            with pytest.raises(TunnelError, match="worker failed"):
                await with_relay(asyncio.sleep(10))
        finally:
            relay_worker.reset(token)

    sibling = asyncio.create_task(asyncio.sleep(0.05, result="completed"))
    await affected()
    assert await sibling == "completed"
    worker = asyncio.create_task(asyncio.sleep(10))
    token = relay_worker.set(worker)
    operation = asyncio.create_task(with_relay(asyncio.sleep(10)))
    try:
        await asyncio.sleep(0)
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert not worker.done()
    finally:
        relay_worker.reset(token)
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_parkable_runtime_validates_gateway_handle_without_serializing_it(monkeypatch):
    import verifiers_ucloud.runtime as runtime_module

    class ManagedClient(_SandboxClient):
        async def create_sandbox(self, spec, **kwargs):
            self.created = spec
            return SimpleNamespace(id=spec.id, record={'generation': 3, 'spec': spec.to_dict()})

    monkeypatch.setattr(runtime_module, 'AsyncSandboxClient', ManagedClient)
    runtime = UCloudRuntime(UCloudRuntimeConfig(parkable=True), name='owned-managed-probe')
    await runtime.start()
    try:
        spec = runtime.info.sandbox_handle.record['spec']
        assert spec['parkable'] is True and spec['managed_process'] is True
        assert spec['profile'] == 'container'
        assert runtime.info.sandbox_generation == 3
        assert 'sandbox_handle' not in runtime.info.model_dump_json()
        with pytest.raises(Exception, match='staged portable Python'):
            await runtime.open_process(['echo', 'no-streaming-exec'], {})
    finally:
        await runtime.stop()


def test_relay_commit_retries_only_transport_with_identical_payload(monkeypatch):
    from ucloud_sandboxes_sdk import AsyncRelayWorkerClient
    from ucloud_sandboxes_sdk.relay import RelayApiError
    from verifiers_ucloud.recovery import ResilientRelayWorkerClient
    calls = []
    async def request(self, method, path, **kwargs):
        calls.append((method, path, kwargs))
        if len(calls) == 1:
            raise RelayApiError("ack lost") from TimeoutError()
        return {"ok": True}
    async def no_sleep(_):
        pass
    monkeypatch.setattr(AsyncRelayWorkerClient, "_request_json", request)
    monkeypatch.setattr("verifiers_ucloud.recovery.asyncio.sleep", no_sleep)
    client = ResilientRelayWorkerClient("http://localhost", timeout_seconds=30)
    body = {"request_id": "same-request", "body": "same-result"}
    assert asyncio.run(client._request_json("POST", "/worker/respond", payload=body)) == {"ok": True}
    assert len(calls) == 2 and calls[0] == calls[1]
    assert calls[0][2]["timeout_seconds"] == 120
    calls.clear()
    with pytest.raises(RelayApiError):
        asyncio.run(client._request_json("POST", "/worker/register", payload=body))
    assert len(calls) == 1


def test_managed_job_route_recovery_does_not_restart(monkeypatch):
    from ucloud_sandboxes_sdk import SandboxApiError
    from verifiers_ucloud.recovery import wait_for_managed_job
    calls = []
    async def refresh():
        calls.append(1)
        if len(calls) == 1:
            raise SandboxApiError("sandbox route not found", status_code=404)
        return SimpleNamespace(terminal=True)
    job = SimpleNamespace(refresh=refresh, sandbox_id="sandbox", job_id="job")
    assert asyncio.run(wait_for_managed_job(job, poll_seconds=0)).terminal
    assert len(calls) == 2


def test_completed_renewal_race_does_not_mask_other_errors(monkeypatch):
    from ucloud_sandboxes_sdk import AsyncRelayWorkerClient
    from ucloud_sandboxes_sdk.relay import RelayApiError
    from verifiers_ucloud.recovery import ResilientRelayWorkerClient
    request = object()
    reason = ["request is already completed"]
    async def renew(self, *args, **kwargs):
        raise RelayApiError(reason[0], status_code=410)
    monkeypatch.setattr(AsyncRelayWorkerClient, "renew_request", renew)
    client = ResilientRelayWorkerClient("http://localhost")
    assert asyncio.run(client.renew_request(request)) is request
    reason[0] = "lease revoked"
    with pytest.raises(RelayApiError, match="lease revoked"):
        asyncio.run(client.renew_request(request))


def test_sdk_worker_survives_completed_renewal_during_delivery(monkeypatch):
    from ucloud_sandboxes_sdk import AsyncRelayWorkerClient
    from ucloud_sandboxes_sdk.relay import RelayApiError, _handle_async_request
    from verifiers_ucloud.recovery import ResilientRelayWorkerClient
    async def scenario():
        renewed = asyncio.Event()
        forwarded = []
        async def renew(self, *args, **kwargs):
            renewed.set()
            raise RelayApiError("request is already completed", status_code=410)
        async def forward(self, request, upstream):
            forwarded.append(request)
            await asyncio.wait_for(renewed.wait(), timeout=2)
        monkeypatch.setattr(AsyncRelayWorkerClient, "renew_request", renew)
        monkeypatch.setattr(AsyncRelayWorkerClient, "forward_to", forward)
        client = ResilientRelayWorkerClient("http://localhost")
        request = SimpleNamespace(endpoint="/v1/chat/completions", body={})
        await _handle_async_request(client, request, handler=None, upstream_base_url="http://localhost", worker_id="worker", lease_seconds=1, renewal_interval_seconds=0.001)
        assert forwarded == [request]
    asyncio.run(scenario())


@pytest.mark.parametrize("word", ["load", "pressure"])
def test_file_admission_retries_both_cpu_messages(monkeypatch, word):
    from ucloud_sandboxes_sdk import SandboxApiError
    from verifiers_ucloud.runtime import _retry_file_admission
    calls=[]
    async def operation():
        calls.append(1)
        if len(calls)==1:
            raise SandboxApiError(f"direct node CPU {word} blocks active admission", status_code=503)
        return "ok"
    async def sleep(delay):
        pass
    monkeypatch.setattr("verifiers_ucloud.runtime.asyncio.sleep", sleep)
    assert asyncio.run(_retry_file_admission(operation)) == "ok"
    assert len(calls)==2


def test_node_lost_is_terminal_for_job_and_relay(monkeypatch):
    from ucloud_sandboxes_sdk import AsyncRelayWorkerClient, SandboxApiError
    from ucloud_sandboxes_sdk.relay import RelayApiError
    from verifiers_ucloud.recovery import SandboxNodeLost, ResilientRelayWorkerClient, wait_for_managed_job
    calls = []
    async def refresh():
        calls.append("refresh")
        raise SandboxApiError("sandbox route not found", status_code=404, body={"error_code": "node_lost"})
    async def request(*args, **kwargs):
        calls.append("relay")
        raise RelayApiError("wake failed", status_code=503, body={"error_code": "node_lost", "retryable": True})
    monkeypatch.setattr(AsyncRelayWorkerClient, "_request_json", request)
    job = SimpleNamespace(refresh=refresh, sandbox_id="sandbox", job_id="job")
    with pytest.raises(SandboxNodeLost, match="node_lost"):
        asyncio.run(wait_for_managed_job(job, poll_seconds=0))
    client = ResilientRelayWorkerClient("http://localhost")
    with pytest.raises(SandboxNodeLost, match="node_lost"):
        asyncio.run(client._request_json("POST", "/worker/respond"))
    assert calls == ["refresh", "relay"]


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_managed_log_read_retries_same_cursor(monkeypatch, stream):
    from ucloud_sandboxes_sdk import SandboxApiError
    from verifiers_ucloud.recovery import read_managed_logs

    calls = []
    sleeps = []
    chunk = SimpleNamespace(data=b"next bytes", next_offset=133)

    async def logs(stream, *, offset):
        calls.append((stream, offset))
        if len(calls) < 3:
            raise SandboxApiError("retry read", status_code=503, body={
                "error_code": "managed_process_read_unavailable", "retryable": True,
            })
        return chunk

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("verifiers_ucloud.recovery.asyncio.sleep", sleep)
    job = SimpleNamespace(logs=logs, sandbox_id="sandbox", job_id="job")
    assert asyncio.run(read_managed_logs(job, stream, offset=123)) is chunk
    assert calls == [(stream, 123)] * 3
    assert sleeps == [1, 2]


@pytest.mark.parametrize("kind,expected_calls", [
    ("exhausted", 5), ("nonretryable", 1), ("unrelated", 1),
    ("node_lost", 1), ("cancelled", 1), ("missing_workdir", 1),
])
def test_managed_log_read_failure_boundaries(monkeypatch, kind, expected_calls):
    from ucloud_sandboxes_sdk import SandboxApiError
    from verifiers_ucloud.recovery import SandboxNodeLost, read_managed_logs

    body = {"error_code": "managed_process_read_unavailable", "retryable": True}
    if kind == "nonretryable":
        body["retryable"] = False
    elif kind in ("unrelated", "node_lost"):
        body["error_code"] = kind
    if kind == "missing_workdir":
        body["error"] = "failed to find initial working directory: no such file or directory"
    error = asyncio.CancelledError() if kind == "cancelled" else SandboxApiError(
        "failed", status_code=503, body=body,
    )
    calls = []

    async def logs(stream, *, offset):
        calls.append((stream, offset))
        raise error

    async def sleep(delay):
        pass

    monkeypatch.setattr("verifiers_ucloud.recovery.asyncio.sleep", sleep)
    job = SimpleNamespace(logs=logs, sandbox_id="sandbox", job_id="job")
    expected = SandboxNodeLost if kind == "node_lost" else type(error)
    with pytest.raises(expected):
        asyncio.run(read_managed_logs(job, "stdout", offset=42))
    assert calls == [("stdout", 42)] * expected_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["image", "poll"])
async def test_confirmed_image_failure_keeps_its_type(monkeypatch, tmp_path, kind):
    from verifiers_ucloud.image_builds import ImageBuildFailure, ImageBuildPollingError
    error_type = ImageBuildFailure if kind == "image" else ImageBuildPollingError

    async def failed_build(*args):
        raise error_type("confirmed failed recipe")

    monkeypatch.setattr("verifiers_ucloud.image_builds.prepare_image_async", failed_build)
    runtime = UCloudRuntime(UCloudRuntimeConfig(
        parkable=True, image_recipe_db=tmp_path / "recipes.sqlite", image_build_cache=tmp_path / "builds",
    ), name="failed-image-probe")
    with pytest.raises(error_type, match="confirmed failed recipe"):
        await runtime.start()
    assert runtime.info.id is None
