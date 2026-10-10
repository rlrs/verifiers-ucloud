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


class _Job:
    def __init__(self, argv, kwargs) -> None:
        self.argv, self.kwargs, self.signals = argv, kwargs, []
        self.sandbox_id, self.job_id, self.refreshes = "sandbox-1", "job-1", 0
        self.record = SimpleNamespace(terminal=False)

    async def refresh(self):
        self.refreshes += 1
        self.record = SimpleNamespace(
            state="exited",
            exit_code=3,
            terminal=True,
            stdout_truncated=False,
            stderr_truncated=False,
        )
        return self.record

    async def logs(self, stream: str, *, offset: int = 0):
        data = {"stdout": b"agent out", "stderr": b"agent err"}[stream]
        return SimpleNamespace(data=data[offset:], next_offset=len(data), eof=True)

    async def signal(self, signal: int = 15):
        self.signals.append(signal)


class _Sandbox:
    def __init__(self, id: str, record: dict) -> None:
        self.id, self.record, self.jobs = id, record, []

    async def start_agent(self, argv, **kwargs):
        self.jobs.append(_Job(argv, kwargs))
        return self.jobs[-1]


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
        return _Sandbox(spec.id, {"spec": spec.to_dict(), "generation": 1})

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
        # As the SDK's tunnel URL does, this one ends in "/".
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
    # A managed agent sandbox: parkable, container profile, linux_host's security.
    assert spec["profile"] == "container"
    assert spec["managed_process"] is True and spec["parkable"] is True
    assert spec["security"]["user"] == "0:0"
    assert spec["security"]["cap_drop"] == [] and spec["security"]["pids_limit"] is None
    assert spec["command"] == []
    assert spec["cpus"] == 2
    assert spec["memory_mb"] == 4096
    assert spec["disk_mb"] == 8192
    assert "toolkits" not in spec  # None asked for: the request is unchanged.
    assert spec["env"] == {"RUNTIME": "yes"}
    assert client.create_kwargs == {"request_timeout_seconds": 900.0}

    result = await runtime.run(["echo", "ok"], {"CALL": "yes"})
    assert result.stdout == "ok"
    assert client.execs[-1][2]["env"] == {"RUNTIME": "yes", "CALL": "yes"}

    # The rollout's main program is the sandbox's managed primary, not an exec.
    program = await runtime.run_program(["harness", "--go"], {"MAIN": "yes"})
    result = (program.exit_code, program.stdout, program.stderr)
    assert result == (3, "agent out", "agent err")
    job = runtime.sandbox.jobs[-1]
    assert job.argv == ["harness", "--go"]
    env = {"RUNTIME": "yes", "MAIN": "yes"}
    assert job.kwargs == {"env": env, "working_dir": "/app"}
    assert job.refreshes == 1 and job.signals == []  # ended: never signalled
    assert len(client.execs) == 1

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
async def test_linux_host_runtime_runs_its_program_as_an_exec(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(managed_agent=False, group_create=False), name="sandbox-1"
    )
    await runtime.start()
    client = _SandboxClient.instances[-1]
    spec = client.created.to_dict()
    assert spec["profile"] == "linux_host" and spec["security"] is None
    assert not spec.get("managed_process") and not spec.get("parkable")
    assert (await runtime.run_program(["harness"], {})).stdout == "ok"
    assert client.execs[-1][1] == ["harness"] and runtime.sandbox.jobs == []


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
async def test_interception_uses_managed_relay_session(monkeypatch) -> None:
    import verifiers_ucloud.interception as interception_module

    _InterceptionServer.instances.clear()
    _RelayClient.instances.clear()
    monkeypatch.setattr(interception_module, "InterceptionServer", _InterceptionServer)
    monkeypatch.setattr(interception_module, "AsyncRelayWorkerClient", _RelayClient)

    interception = UCloudInterception(
        UCloudInterceptionConfig(
            relay_url="https://relay.example/",
            max_inflight_requests=17,
            forward_timeout_seconds=123.0,
        ),
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
    assert relay.kwargs["max_inflight_requests"] == 17
    assert relay.kwargs["forward_timeout_seconds"] == 123.0
    relay_session = relay.sessions[-1]
    server = _InterceptionServer.instances[-1]
    assert relay.registered == ["trace-1"]
    assert relay.unregistered == ["trace-1"]
    assert relay_session.kwargs["metadata"] == {"consumer": "verifiers"}
    assert relay_session.run_kwargs is not None
    assert relay_session.run_kwargs["upstream_base_url"] == server.base_url
    assert relay_session.run_kwargs["lease_seconds"] == 900.0
    assert server.unregistered == [("model-secret", "state-secret")]
    assert relay_session.kwargs["sandbox"] is None  # no runtime: an unbound session

    # A managed agent runtime's sandbox is bound, so its model calls are its waits.
    import verifiers_ucloud.runtime as runtime_module

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    for managed in (True, False):
        runtime = UCloudRuntime(
            UCloudRuntimeConfig(managed_agent=managed, group_create=False), name="sb"
        )
        await runtime.start()
        async with interception.acquire(session, runtime):
            pass
        bound = relay.sessions[-1].kwargs["sandbox"]
        assert bound is (runtime.sandbox if managed else None)

    await interception.stop()
    assert relay.closed
    assert server.closed


def test_interception_request_budget_must_be_positive() -> None:
    with pytest.raises(ValueError):
        UCloudInterceptionConfig(max_inflight_requests=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["normal", "cancel", "failure", "hint_failure"])
async def test_resource_phase_hints_are_optional_fenced_and_non_authoritative(
    monkeypatch, outcome
):
    import verifiers_ucloud.interception as module

    monkeypatch.setattr(module, "InterceptionServer", _InterceptionServer)
    monkeypatch.setattr(module, "AsyncRelayWorkerClient", _RelayClient)
    monkeypatch.setattr(_RelaySession, "registration_token", "a" * 32, raising=False)
    calls = []

    async def update(self, rollout_id, **kwargs):
        calls.append((rollout_id, kwargs))
        if outcome == "hint_failure":
            raise TimeoutError("unavailable")
        return {"accepted": True}

    monkeypatch.setattr(_RelayClient, "update_resource_phase", update, raising=False)
    interception = UCloudInterception(
        UCloudInterceptionConfig(
            relay_url="https://relay.example", resource_phase_hints=True
        )
    )
    await interception.start()
    session = cast(
        RolloutSession, SimpleNamespace(trace=SimpleNamespace(id="phase-run"))
    )

    async def exercise():
        async with interception.acquire(session):
            await interception.report_resource_phase(
                "phase-run", "model_wait", expected_remaining_wait_seconds=20
            )
            if outcome == "cancel":
                raise asyncio.CancelledError()
            if outcome == "failure":
                raise RuntimeError("tool failed")

    try:
        if outcome == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await exercise()
        elif outcome == "failure":
            # The rollout's own error, not wrapped in an ExceptionGroup.
            with pytest.raises(RuntimeError, match="tool failed"):
                await exercise()
        else:
            await exercise()
        assert [call[1]["sequence"] for call in calls] == list(range(1, len(calls) + 1))
        assert all(call[1]["registration_token"] == "a" * 32 for call in calls)
        phases = [call[1]["phase"] for call in calls]
        assert phases[:2] == ["tool", "model_wait"]
        assert ("rollout_complete" in phases) == (outcome in {"normal", "hint_failure"})
        assert not interception._phase_sessions
        assert _RelayClient.instances[-1].unregistered == ["phase-run"]
    finally:
        await interception.stop()


def test_toolkits_reach_the_sandbox_spec_in_both_sandbox_shapes() -> None:
    for managed in (True, False):
        runtime = UCloudRuntime(
            UCloudRuntimeConfig(toolkits=["vf-harness:v1"], managed_agent=managed),
            name="sandbox-1",
        )
        assert runtime._spec().to_dict()["toolkits"] == ["vf-harness:v1"]


def test_a_uv_toolkit_prepares_uv_scripts_and_must_be_requested() -> None:
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(toolkits=["vf-harness:v1"], uv_toolkit="vf-harness"),
        name="sandbox-1",
    )
    assert runtime.uv_env["UV_INSTALL_DIR"] == "/opt/ucloud/toolkits/vf-harness/bin"
    assert runtime.uv_env["UV_PYTHON_PREFERENCE"] == "only-managed"
    assert runtime.env == {}  # Task commands see none of it.
    assert UCloudRuntime(UCloudRuntimeConfig(), name="s").uv_env == {}
    with pytest.raises(ValueError, match="uv_toolkit"):
        UCloudRuntimeConfig(uv_toolkit="vf-harness")


def test_unknown_runtime_and_interception_settings_are_rejected() -> None:
    with pytest.raises(ValueError, match="parkable"):
        UCloudRuntimeConfig(parkable=True)
    with pytest.raises(ValueError, match="no_such_setting"):
        UCloudInterceptionConfig(no_such_setting=1)


@pytest.mark.asyncio
async def test_bounded_reads_use_the_verifiers_capped_read(monkeypatch) -> None:
    from verifiers.v1.runtimes.base import Runtime

    import verifiers_ucloud.runtime as runtime_module

    capped = []

    async def capped_read(self, path, max_bytes=None):
        capped.append((path, max_bytes))
        return b"x" * 3

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    monkeypatch.setattr(Runtime, "_read", capped_read)
    runtime = UCloudRuntime(UCloudRuntimeConfig(group_create=False), name="sandbox-1")
    await runtime.start()
    try:
        assert await runtime.read("log.txt", max_bytes=10) == b"xxx"
        assert capped == [("log.txt", 11)]  # verifiers asks for one extra byte
        assert await runtime.read("log.txt") == b"sandbox-1:/app/log.txt"
    finally:
        await runtime.stop()


def test_images_resolve_by_registry_reference_or_gateway_name() -> None:
    by_registry = UCloudRuntime(UCloudRuntimeConfig(image="python:3.12"), name="a")
    assert by_registry._spec().image.tag is not None
    by_name = UCloudRuntime(
        UCloudRuntimeConfig(image="tmax:task_000001", image_reference_type="name"),
        name="b",
    )
    image = by_name._spec().image
    assert image.name == "tmax:task_000001" and image.tag is None


def test_relay_only_sandboxes_ask_the_gateway_for_the_named_relay() -> None:
    restricted = UCloudRuntime(UCloudRuntimeConfig(allow=[]), name="a")
    assert restricted.network_restricted
    policy = restricted._spec().to_dict()["network_policy"]
    assert policy == {"egress": "relay", "relay": "default"}
    open_box = UCloudRuntime(UCloudRuntimeConfig(), name="b")
    assert not open_box.network_restricted
    assert "network_policy" not in open_box._spec().to_dict()

    with pytest.raises(ValueError, match="framework-only"):
        UCloudRuntimeConfig(allow=["pypi.org"])
    with pytest.raises(ValueError, match="bridge transport"):
        UCloudRuntimeConfig(allow=[], network_access=False)
    with pytest.raises(ValueError, match="HTTP"):
        UCloudRuntimeConfig(allow=[], guest_relay_url="http://relay:8092/path")


def test_relay_only_guests_reach_the_public_relay_at_its_guest_origin(
    monkeypatch,
) -> None:
    monkeypatch.setenv("UCLOUD_RELAY_URL", "https://relay.example/base")
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(allow=[], guest_relay_url="http://10.0.0.2:8092"), name="a"
    )
    url = "https://relay.example/base/managed/r1/v1?cap=1"
    assert runtime.host_url(url) == "http://10.0.0.2:8092/managed/r1/v1?cap=1"
    # Other origins, and paths outside the relay's prefix, are left alone.
    assert runtime.host_url("https://other.example/v1") == "https://other.example/v1"
    outside = "https://relay.example/elsewhere"
    assert runtime.host_url(outside) == outside
    open_box = UCloudRuntime(
        UCloudRuntimeConfig(guest_relay_url="http://g:1"), name="b"
    )
    assert open_box.host_url(url) == url


@pytest.mark.asyncio
async def test_relay_only_execution_admits_only_relay_routes(monkeypatch) -> None:
    monkeypatch.setenv("UCLOUD_RELAY_URL", "https://relay.example")
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(allow=[], guest_relay_url="http://10.0.0.2:8092"), name="a"
    )
    await runtime.prepare_execution(["http://10.0.0.2:8092/managed/r1/v1"])
    with pytest.raises(SandboxError, match="outside"):
        await runtime.prepare_execution(["http://10.0.0.2:9000/v1"])
    with pytest.raises(SandboxError, match="immutable"):
        await runtime.prepare_execution(None)
    # An open sandbox has nothing to enforce.
    await UCloudRuntime(UCloudRuntimeConfig(), name="b").prepare_execution(None)


@pytest.mark.parametrize(
    "reason",
    ["CPU load", "CPU pressure", "memory pressure"],
)
def test_file_transfers_retry_node_admission_refusals(monkeypatch, reason) -> None:
    from ucloud_sandboxes_sdk import SandboxApiError

    from verifiers_ucloud.runtime import _retry_admission

    calls = []

    async def operation():
        calls.append(1)
        if len(calls) == 1:
            raise SandboxApiError(
                f"direct node {reason} blocks active admission", status_code=503
            )
        return "ok"

    async def no_sleep(delay):
        pass

    monkeypatch.setattr("verifiers_ucloud.runtime.asyncio.sleep", no_sleep)
    assert asyncio.run(_retry_admission(operation)) == "ok"
    assert len(calls) == 2


def test_other_transfer_failures_are_not_retried(monkeypatch) -> None:
    from ucloud_sandboxes_sdk import SandboxApiError

    from verifiers_ucloud.runtime import _retry_admission

    calls = []

    async def operation():
        calls.append(1)
        raise SandboxApiError("sandbox route not found", status_code=503)

    with pytest.raises(SandboxApiError):
        asyncio.run(_retry_admission(operation))
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_loopback_hosts_are_repaired_only_when_asked(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    plain = UCloudRuntime(UCloudRuntimeConfig(group_create=False), name="plain")
    await plain.start()
    assert _SandboxClient.instances[-1].execs == []
    await plain.stop()

    repaired = UCloudRuntime(
        UCloudRuntimeConfig(group_create=False, repair_loopback_hosts=True),
        name="repaired",
    )
    await repaired.start()
    (_, command, _) = _SandboxClient.instances[-1].execs[-1]
    assert command[:2] == ["sh", "-c"] and "getent hosts localhost" in command[2]
    await repaired.stop()


@pytest.mark.asyncio
async def test_a_failed_loopback_repair_deletes_the_sandbox(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module

    class FailedHostsClient(_SandboxClient):
        async def exec(self, sandbox_id, command, **kwargs):
            return _Result(exit_code=1, stdout="", stderr="read-only hosts file")

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", FailedHostsClient)
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(group_create=False, repair_loopback_hosts=True),
        name="failed-hosts",
    )
    with pytest.raises(SandboxError, match="loopback hosts setup failed"):
        await runtime.start()
    client = FailedHostsClient.instances[-1]
    assert client.deleted == ["failed-hosts"] and client.closed


@pytest.mark.asyncio
async def test_a_failed_relay_worker_fails_its_rollouts_operation(
    monkeypatch, caplog
) -> None:
    from verifiers.v1.errors import TunnelError

    import verifiers_ucloud.interception as module
    from verifiers_ucloud.supervision import with_relay

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
    session = cast(
        RolloutSession, SimpleNamespace(trace=SimpleNamespace(id="failure-test"))
    )
    try:
        with pytest.raises(TunnelError, match="relay failure sentinel"):
            async with interception.acquire(session):
                await with_relay(asyncio.sleep(1))
        assert "UCloud relay worker failed: rollout=failure-test" in caplog.text
    finally:
        await interception.stop()


@pytest.mark.asyncio
async def test_relay_failure_stays_with_its_rollout_and_cancel_propagates() -> None:
    from verifiers.v1.errors import TunnelError

    from verifiers_ucloud.supervision import relay_worker, with_relay

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


def test_relay_commit_retries_only_transport_with_identical_payload(
    monkeypatch,
) -> None:
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
    result = asyncio.run(client._request_json("POST", "/worker/respond", payload=body))
    assert result == {"ok": True}
    assert len(calls) == 2 and calls[0] == calls[1]
    assert calls[0][2]["timeout_seconds"] == 120
    calls.clear()
    with pytest.raises(RelayApiError):
        asyncio.run(client._request_json("POST", "/worker/register", payload=body))
    assert len(calls) == 1


def test_a_briefly_unreachable_worker_is_polled_again_not_restarted() -> None:
    from ucloud_sandboxes_sdk import SandboxApiError

    from verifiers_ucloud.recovery import wait_for_managed_job

    calls = []

    async def refresh():
        calls.append(1)
        if len(calls) == 1:
            raise SandboxApiError(
                "sandbox worker heartbeat is stale or unavailable",
                status_code=503,
                body={"error_code": "sandbox_worker_unreachable", "retryable": True},
            )
        return SimpleNamespace(terminal=True)

    job = SimpleNamespace(refresh=refresh, sandbox_id="sandbox", job_id="job")
    assert asyncio.run(wait_for_managed_job(job, poll_seconds=0)).terminal
    assert len(calls) == 2


def test_a_deleted_sandbox_route_is_not_polled_again() -> None:
    from ucloud_sandboxes_sdk import SandboxApiError

    from verifiers_ucloud.recovery import wait_for_managed_job

    calls = []

    async def refresh():
        calls.append(1)
        raise SandboxApiError(
            "sandbox route not found",
            status_code=404,
            body={"error": "sandbox route not found"},
        )

    job = SimpleNamespace(refresh=refresh, sandbox_id="sandbox", job_id="job")
    with pytest.raises(SandboxApiError):
        asyncio.run(wait_for_managed_job(job, poll_seconds=0))
    assert len(calls) == 1


def test_node_loss_is_terminal_for_jobs_and_relay_commits(monkeypatch) -> None:
    from ucloud_sandboxes_sdk import AsyncRelayWorkerClient, SandboxApiError
    from ucloud_sandboxes_sdk.relay import RelayApiError

    from verifiers_ucloud.recovery import (
        ResilientRelayWorkerClient,
        SandboxNodeLost,
        wait_for_managed_job,
    )

    calls = []

    async def refresh():
        calls.append("refresh")
        raise SandboxApiError(
            "sandbox route not found", status_code=404, body={"error_code": "node_lost"}
        )

    async def request(*args, **kwargs):
        calls.append("relay")
        raise RelayApiError(
            "wake failed",
            status_code=503,
            body={"error_code": "node_lost", "retryable": True},
        )

    monkeypatch.setattr(AsyncRelayWorkerClient, "_request_json", request)
    job = SimpleNamespace(refresh=refresh, sandbox_id="sandbox", job_id="job")
    with pytest.raises(SandboxNodeLost, match="node_lost"):
        asyncio.run(wait_for_managed_job(job, poll_seconds=0))
    client = ResilientRelayWorkerClient("http://localhost")
    with pytest.raises(SandboxNodeLost, match="node_lost"):
        asyncio.run(client._request_json("POST", "/worker/respond"))
    assert calls == ["refresh", "relay"]
    assert issubclass(SandboxNodeLost, SandboxError)


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_managed_log_reads_retry_at_the_same_offset(monkeypatch, stream) -> None:
    from ucloud_sandboxes_sdk import SandboxApiError

    from verifiers_ucloud.recovery import read_managed_logs

    calls, sleeps = [], []
    chunk = SimpleNamespace(data=b"next bytes", next_offset=133)

    async def logs(stream, *, offset):
        calls.append((stream, offset))
        if len(calls) < 3:
            raise SandboxApiError(
                "retry read",
                status_code=503,
                body={
                    "error_code": "managed_process_read_unavailable",
                    "retryable": True,
                },
            )
        return chunk

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("verifiers_ucloud.recovery.asyncio.sleep", sleep)
    job = SimpleNamespace(logs=logs, sandbox_id="sandbox", job_id="job")
    assert asyncio.run(read_managed_logs(job, stream, offset=123)) is chunk
    assert calls == [(stream, 123)] * 3
    assert sleeps == [1, 2]


@pytest.mark.parametrize(
    "kind,expected_calls",
    [
        ("exhausted", 5),
        ("nonretryable", 1),
        ("unrelated", 1),
        ("node_lost", 1),
        ("cancelled", 1),
        ("missing_workdir", 1),
    ],
)
def test_managed_log_read_failure_boundaries(monkeypatch, kind, expected_calls) -> None:
    from ucloud_sandboxes_sdk import SandboxApiError

    from verifiers_ucloud.recovery import SandboxNodeLost, read_managed_logs

    body = {"error_code": "managed_process_read_unavailable", "retryable": True}
    if kind == "nonretryable":
        body["retryable"] = False
    elif kind in ("unrelated", "node_lost"):
        body["error_code"] = kind
    if kind == "missing_workdir":
        body["error"] = "failed to find initial working directory: no such file"
    error = (
        asyncio.CancelledError()
        if kind == "cancelled"
        else SandboxApiError("failed", status_code=503, body=body)
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
async def test_a_lost_node_ends_the_agent_without_signalling_it(monkeypatch) -> None:
    from ucloud_sandboxes_sdk import SandboxApiError

    import verifiers_ucloud.runtime as runtime_module
    from verifiers_ucloud.recovery import SandboxNodeLost

    class LostJob(_Job):
        record = SimpleNamespace(terminal=False)

        async def refresh(self):
            raise SandboxApiError(
                "gone", status_code=410, body={"error_code": "node_lost"}
            )

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    runtime = UCloudRuntime(UCloudRuntimeConfig(group_create=False), name="lost")
    await runtime.start()
    try:

        async def start_agent(argv, **kwargs):
            runtime.sandbox.jobs.append(LostJob(argv, kwargs))
            return runtime.sandbox.jobs[-1]

        runtime.sandbox.start_agent = start_agent
        with pytest.raises(SandboxNodeLost):
            await runtime.run_program(["harness"], {})
        assert runtime.sandbox.jobs[-1].signals == []
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_truncated_agent_output_is_an_error(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module

    class TruncatedJob(_Job):
        record = SimpleNamespace(terminal=True)

        async def refresh(self):
            return SimpleNamespace(
                terminal=True,
                exit_code=0,
                stdout_truncated=True,
                stderr_truncated=False,
            )

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    runtime = UCloudRuntime(UCloudRuntimeConfig(group_create=False), name="truncated")
    await runtime.start()
    try:

        async def start_agent(argv, **kwargs):
            return TruncatedJob(argv, kwargs)

        runtime.sandbox.start_agent = start_agent
        with pytest.raises(SandboxError, match="log limit"):
            await runtime.run_program(["harness"], {})
    finally:
        await runtime.stop()


def test_parking_interactive_processes_needs_a_managed_sandbox() -> None:
    with pytest.raises(ValueError, match="park_interactive"):
        UCloudRuntimeConfig(park_interactive=True, managed_agent=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uv_toolkit,interpreter",
    [
        (None, "/opt/verifiers-offline/abc/python/bin/python3.12"),
        ("vf-harness", "/opt/ucloud/toolkits/vf-harness/uv-cache/env/bin/python"),
    ],
)
async def test_interactive_processes_park_as_managed_jobs(
    monkeypatch, uv_toolkit, interpreter
) -> None:
    import verifiers_ucloud.runtime as runtime_module

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    toolkits = ["vf-harness:v1"] if uv_toolkit else []
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(
            group_create=False,
            park_interactive=True,
            toolkits=toolkits,
            uv_toolkit=uv_toolkit,
        ),
        name="acp",
    )
    await runtime.start()
    client = _SandboxClient.instances[-1]
    try:
        # Refused before the sandbox's one managed primary is claimed.
        other = "/opt/ucloud/toolkits/other/bin/python"
        for foreign in (other, "/usr/bin/python3"):
            with pytest.raises(SandboxError, match="staged portable Python"):
                await runtime.open_process([foreign, "/tmp/acp.py"], {})
        process = await runtime.open_process([interpreter, "/tmp/acp.py"], {"A": "1"})
        job = runtime.sandbox.jobs[-1]
        assert job.argv[0] == interpreter and job.argv[1] == "-I"
        bridge, mailbox = job.argv[2], job.argv[3]
        assert bridge == f"{mailbox}/bridge.py"
        assert job.argv[4:] == [interpreter, "/tmp/acp.py"]
        assert job.kwargs["env"] == {"A": "1"}
        assert client.processes == []  # no live exec holds the sandbox
        await process.write(b"request")
        assert client.uploads[-1][1:] == (f"{mailbox}/0.tmp", b"request")
        mv = client.execs[-1][1]
        assert mv == ["mv", "--", f"{mailbox}/0.tmp", f"{mailbox}/0.ready"]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_a_sandbox_runs_one_managed_primary_then_execs(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(group_create=False, park_interactive=True), name="acp"
    )
    await runtime.start()
    client = _SandboxClient.instances[-1]
    try:
        # In an open sandbox, the uv environment this runtime prepared qualifies.
        prepared = "/root/.cache/uv/environments-v2/acp-0123/bin/python"
        runtime._uv_interpreters["acp-digest"] = prepared
        await runtime.open_process([prepared, "/tmp/acp.py"], {})
        assert runtime.sandbox.jobs[-1].argv[0] == prepared
        # The gateway allows one primary per sandbox generation, even after it
        # exits: a restarted ACP process and a later program run as execs.
        await runtime.open_process([prepared, "/tmp/acp.py"], {})
        assert len(runtime.sandbox.jobs) == 1
        assert client.processes[-1][1] == [prepared, "/tmp/acp.py"]
        result = await runtime.run_program(["harness"], {})
        assert len(runtime.sandbox.jobs) == 1
        assert client.execs[-1][1] == ["harness"] and result.exit_code == 0
    finally:
        await runtime.stop()


def _bundle(tmp_path, name: str, members: dict[str, bytes], **manifest) -> object:
    import hashlib
    import io
    import json
    import tarfile

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for member, data in members.items():
            info = tarfile.TarInfo(member)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    archive = tmp_path / name
    archive.write_bytes(buffer.getvalue())
    manifest.setdefault("sha256", hashlib.sha256(buffer.getvalue()).hexdigest())
    (tmp_path / f"{name}.json").write_text(json.dumps(manifest))
    return archive


@pytest.mark.asyncio
@pytest.mark.parametrize("uv_toolkit", [None, "vf-harness"])
async def test_relay_only_uv_scripts_use_the_toolkit_or_the_offline_bundle(
    monkeypatch, uv_toolkit
) -> None:
    from verifiers.v1.runtimes.base import Runtime

    import verifiers_ucloud.offline as offline_module

    calls = []

    async def framework(self, script, env=None, *, activate=True):
        calls.append("toolkit")
        return ["python", "script.py"]

    async def offline(runtime, script, env=None, *, activate=True):
        calls.append("offline")
        return ["python", "script.py"]

    monkeypatch.setattr(Runtime, "prepare_uv_script", framework)
    monkeypatch.setattr(offline_module, "prepare_script", offline)
    config = UCloudRuntimeConfig(
        allow=[],
        toolkits=["vf-harness:v1"] if uv_toolkit else [],
        uv_toolkit=uv_toolkit,
    )
    await UCloudRuntime(config, name="relay-only").prepare_uv_script("print(1)")
    assert calls == ["toolkit" if uv_toolkit else "offline"]


@pytest.mark.asyncio
async def test_offline_python_runs_only_the_scripts_it_was_built_for(
    monkeypatch, tmp_path
) -> None:
    import hashlib

    import verifiers_ucloud.offline as offline_module
    import verifiers_ucloud.runtime as runtime_module

    script = "print('harness')"
    digest = hashlib.sha256(script.encode()).hexdigest()
    bundle = _bundle(
        tmp_path,
        "python.tar.gz",
        {"python/bin/python3.12": b"#!"},
        python="python/bin/python3.12",
        scripts=[digest],
    )
    offline_module.read_bundle.cache_clear()
    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(allow=[], group_create=False, offline_python_bundle=bundle),
        name="offline",
    )
    await runtime.start()
    client = _SandboxClient.instances[-1]
    try:
        argv = await runtime.prepare_uv_script(script, activate=False)
        sha = offline_module.read_bundle(str(bundle))[1]["sha256"]
        interpreter = f"/opt/verifiers-offline/{sha}/python/bin/python3.12"
        assert argv == [interpreter, "-I", f"{runtime.scripts_dir}/{digest}.py"]
        setup = client.execs[-1][1][2]
        assert "sha256sum -c" in setup and "tar --no-same-owner" in setup
        await runtime.prepare_uv_script(script, activate=False)
        assert len(client.execs) == 1  # the bundle is staged once per sandbox
        with pytest.raises(SandboxError, match="absent"):
            await runtime.prepare_uv_script("print('other')")
    finally:
        await runtime.stop()

    (tmp_path / "python.tar.gz").write_bytes(b"tampered")
    offline_module.read_bundle.cache_clear()
    with pytest.raises(SandboxError, match="checksum"):
        offline_module.read_bundle(str(bundle))


@pytest.mark.asyncio
async def test_harness_bundles_unpack_only_known_paths(monkeypatch, tmp_path) -> None:
    import verifiers_ucloud.offline as offline_module
    import verifiers_ucloud.runtime as runtime_module

    good = _bundle(
        tmp_path, "agents.tar.gz", {"var/tmp/vf-opencode/bin/opencode": b"#!"}
    )
    bad = _bundle(tmp_path, "bad.tar.gz", {"etc/passwd": b"x"})
    unsafe = _bundle(tmp_path, "unsafe.tar.gz", {"var/tmp/vf-node/../../x": b"x"})
    offline_module.read_harness_bundle.cache_clear()
    with pytest.raises(SandboxError, match="Unexpected harness archive member"):
        offline_module.read_harness_bundle(str(bad))
    with pytest.raises(SandboxError, match="Unsafe"):
        offline_module.read_harness_bundle(str(unsafe))

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(group_create=False, offline_harness_bundle=good),
        name="agents",
    )
    await runtime.start()
    client = _SandboxClient.instances[-1]
    try:
        assert client.uploads[-1][1].startswith("/var/tmp/vf-harness-")
        assert "tar --no-same-owner -xzf" in client.execs[-1][1][3]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_host_tool_servers_reach_sandboxes_through_the_relay(
    monkeypatch,
) -> None:
    from verifiers.v1.interception.tunnel import using_host_tunnel

    import verifiers_ucloud.tunnel as tunnel_module
    from verifiers_ucloud.tunnel import UCloudTunnel

    monkeypatch.setattr(tunnel_module, "ResilientRelayWorkerClient", _RelayClient)
    config = UCloudInterceptionConfig(
        relay_url="https://relay.example", guest_relay_url="http://10.0.0.2:8092"
    )
    tunnel = config.host_tunnel()
    assert isinstance(tunnel, UCloudTunnel)
    async with tunnel.expose(7001) as url:
        session = _RelayClient.instances[-1].sessions[-1]
        assert url == f"https://relay.example/managed/{session.rollout_id}"
        assert session.kwargs["metadata"] == {"consumer": "verifiers-shared-tools"}
        await asyncio.sleep(0)
        assert session.run_kwargs["upstream_base_url"] == "http://127.0.0.1:7001"
    # A relay-only guest dials the tool server's relay URL at the guest origin.
    runtime = UCloudRuntime(UCloudRuntimeConfig(allow=[]), name="tools")
    with using_host_tunnel(tunnel):
        guest = runtime.host_url(f"{url}/mcp")
    assert guest == f"http://10.0.0.2:8092/managed/{session.rollout_id}/mcp"

    with pytest.raises(ValueError, match="HTTP"):
        UCloudInterceptionConfig(guest_relay_url="relay:8092")


@pytest.mark.asyncio
async def test_relay_only_execution_admits_the_origin_host_url_hands_guests(
    monkeypatch,
) -> None:
    from verifiers.v1.interception.tunnel import using_host_tunnel

    # The relay URLs come from the interception alone.
    monkeypatch.delenv("UCLOUD_RELAY_URL", raising=False)
    interception = UCloudInterceptionConfig(
        relay_url="https://relay.example/base", guest_relay_url="http://10.0.0.2:8092"
    )
    runtime = UCloudRuntime(UCloudRuntimeConfig(allow=[]), name="a")
    with using_host_tunnel(interception.host_tunnel):
        endpoint = runtime.host_url("https://relay.example/base/managed/r1/v1")
        assert endpoint == "http://10.0.0.2:8092/managed/r1/v1"
        await runtime.prepare_execution([endpoint])
        with pytest.raises(SandboxError, match="outside"):
            await runtime.prepare_execution(["https://relay.example/base/v1"])
    # Without a guest origin, a public relay URL's path prefix is not its origin.
    monkeypatch.setenv("UCLOUD_RELAY_URL", "https://relay.example/base")
    await runtime.prepare_execution(["https://relay.example/base/managed/r1/v1"])
    monkeypatch.delenv("UCLOUD_RELAY_URL")
    with pytest.raises(SandboxError, match="outside"):
        await runtime.prepare_execution(["https://relay.example/managed/r1/v1"])


@pytest.mark.asyncio
async def test_a_cancelled_program_is_terminated_not_killed(monkeypatch) -> None:
    import verifiers_ucloud.runtime as runtime_module

    class RunningJob(_Job):
        async def refresh(self):
            return self.record  # never ends on its own

    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "https://gateway.example")
    monkeypatch.setattr(runtime_module, "AsyncSandboxClient", _SandboxClient)
    runtime = UCloudRuntime(
        UCloudRuntimeConfig(group_create=False, agent_poll_seconds=0.01), name="c"
    )
    await runtime.start()
    try:
        started = asyncio.Event()

        async def start_agent(argv, **kwargs):
            runtime.sandbox.jobs.append(RunningJob(argv, kwargs))
            started.set()
            return runtime.sandbox.jobs[-1]

        runtime.sandbox.start_agent = start_agent
        task = asyncio.create_task(runtime.run_program(["harness"], {}))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # The managed-process supervisor refuses SIGKILL (and SIGSTOP).
        assert runtime.sandbox.jobs[-1].signals == [15]
    finally:
        await runtime.stop()


def _managed_process(job, **runtime_attrs):
    from verifiers_ucloud.managed_process import ManagedProcess

    async def status(sandbox_id):
        return {"state": "running"}

    client = SimpleNamespace(get_sandbox_status=status)
    runtime = SimpleNamespace(_client_or_raise=lambda: client, **runtime_attrs)
    return ManagedProcess(runtime, job, "/tmp/mailbox")


@pytest.mark.asyncio
async def test_interactive_output_is_read_to_its_end_after_the_process_exits() -> None:
    reads = [b"first", b"", b"last reply"]
    log = b"".join(reads)

    class ExitingJob:
        sandbox_id, job_id = "sandbox", "job"

        def __init__(self) -> None:
            self.reads, self.refreshes = 0, 0

        async def logs(self, stream, *, offset=0):
            end = len(b"".join(reads[: self.reads + 1]))
            self.reads += 1
            return SimpleNamespace(data=log[offset:end], next_offset=end, eof=True)

        async def refresh(self):
            # The process exits after its first output; its last reply lands in
            # the log after the read that found the log's end.
            self.refreshes += 1
            return SimpleNamespace(
                terminal=self.refreshes > 1,
                stdout_truncated=False,
                stderr_truncated=False,
            )

    process = _managed_process(ExitingJob())
    assert b"".join([chunk async for chunk in process.stdout]) == b"firstlast reply"


@pytest.mark.asyncio
async def test_input_stops_after_a_write_whose_outcome_is_unknown() -> None:
    uploads, runs = [], []

    async def write(path, data):
        uploads.append(path)

    async def run(argv, env):
        runs.append(argv)
        raise TimeoutError("the mv's answer was lost")

    process = _managed_process(SimpleNamespace(), write=write, run=run)
    with pytest.raises(TimeoutError):
        await process.write(b"one")
    # The bridge may already have read input 0: never publish another.
    with pytest.raises(SandboxError, match="failed write"):
        await process.write(b"two")
    assert uploads == ["/tmp/mailbox/0.tmp"] and len(runs) == 1


def test_bridge_feeds_stdin_in_order_and_reports_signals(tmp_path) -> None:
    import signal
    import subprocess
    import sys
    import time

    from verifiers_ucloud.managed_process import BRIDGE

    bridge = tmp_path / "bridge.py"
    bridge.write_text(BRIDGE)
    (tmp_path / "1.ready").write_bytes(b"two\n")
    (tmp_path / "0.ready").write_bytes(b"one\n")
    child = "import sys; print(sys.stdin.readline() + sys.stdin.readline(), end='')"
    done = subprocess.run(
        [sys.executable, str(bridge), str(tmp_path), sys.executable, "-c", child],
        capture_output=True,
        timeout=30,
    )
    assert (done.returncode, done.stdout) == (0, b"one\ntwo\n")

    sleeper = subprocess.Popen(
        [sys.executable, str(bridge), str(tmp_path / "empty"), "sleep", "30"]
    )
    time.sleep(0.5)
    sleeper.send_signal(signal.SIGTERM)
    assert sleeper.wait(timeout=30) == 128 + signal.SIGTERM


def test_exec_worker_loss_is_node_loss() -> None:
    from ucloud_sandboxes_sdk import SandboxApiError

    from verifiers_ucloud.recovery import SandboxNodeLost, node_lost

    error = SandboxApiError(
        "exec worker was lost; the accepted command cannot resume",
        status_code=410,
        body={"error_code": "exec_worker_lost", "retryable": False},
    )
    assert isinstance(node_lost(error), SandboxNodeLost)


@pytest.mark.asyncio
async def test_tool_relay_keeps_the_tunnel_contract(monkeypatch) -> None:
    from verifiers.v1.errors import TunnelError

    import verifiers_ucloud.tunnel as tunnel_module

    monkeypatch.setattr(tunnel_module, "ResilientRelayWorkerClient", _RelayClient)
    monkeypatch.setattr(tunnel_module, "_RESTART_SECONDS", (0,))
    config = UCloudInterceptionConfig(relay_url="https://relay.example")
    tunnel = config.host_tunnel()

    # The caller's own error comes out unchanged, not in an ExceptionGroup.
    with pytest.raises(KeyError):
        async with tunnel.expose(7001):
            raise KeyError("caller")

    # A failed worker is restarted on the same session; the scope stays up.
    runs, restarted = [], asyncio.Event()

    async def flaky_run(self, **kwargs):
        runs.append(1)
        if len(runs) == 1:
            raise RuntimeError("one request's commit was refused")
        restarted.set()
        await kwargs["cancel"].wait()

    monkeypatch.setattr(_RelaySession, "run", flaky_run)
    async with tunnel.expose(7001):
        await asyncio.wait_for(restarted.wait(), 5)

    # A setup failure is a TunnelError.
    class Unreachable(_RelayClient):
        async def __aenter__(self):
            raise OSError("relay unreachable")

    monkeypatch.setattr(tunnel_module, "ResilientRelayWorkerClient", Unreachable)
    with pytest.raises(TunnelError, match="failed to start"):
        async with tunnel.expose(7001):
            pass


def test_offline_bundles_over_the_upload_limit_are_refused(
    monkeypatch, tmp_path
) -> None:
    import verifiers_ucloud.offline as offline_module

    archive = _bundle(tmp_path, "big.tar.gz", {"var/tmp/vf-node/x": b"x" * 64})
    monkeypatch.setattr(offline_module, "MAX_FILE_BODY_BYTES", 16)
    with pytest.raises(SandboxError, match="upload limit"):
        offline_module.read_harness_bundle(str(archive))


class _IndexClient:
    base_url = "https://gateway.example"
    summary: ClassVar[dict] = {
        "environments": {
            "(none)": {"names": 2, "tasks": 0, "states": {"ready": 2}},
            "tmax": {"names": 3, "tasks": 3, "states": {"not_built": 2, "failed": 1}},
            "r2e-gym": {"names": 1, "tasks": 1, "states": {"ready": 1}},
        },
        "totals": {"names": 6, "tasks": 4},
    }

    @classmethod
    def from_env(cls, *, timeout_seconds):
        return cls()

    def image_index_summary(self):
        return self.summary

    def image_index_task_ids(self, environment):
        assert environment != "(none)"
        ids = {"tmax": ["task_1", "task_2"], "r2e-gym": ["abc123"]}[environment]
        excluded = {"failed": 1} if environment == "tmax" else {}
        return {"environment": environment, "task_ids": ids, "excluded": excluded}

    def image_index_names(self, *, environment=None, state=None, page_size=500):
        assert (environment, state) == ("tmax", "ready")
        return iter([{"name": "prime/tmax:task_2", "state": "ready"}])

    def image_index_name(self, name):
        return {"name": name, "task_ids": ["task_2"]}


def test_task_ids_command_writes_each_environments_task_ids_file(
    monkeypatch, tmp_path, capsys
) -> None:
    import json

    import verifiers_ucloud.cli as cli

    monkeypatch.setattr(cli, "SandboxClient", _IndexClient)
    out = tmp_path / "allowlists"
    assert cli.main(["task-ids", str(out)]) == 0
    assert json.loads((out / "tmax.task-ids.json").read_text()) == ["task_1", "task_2"]
    assert json.loads((out / "r2e-gym.task-ids.json").read_text()) == ["abc123"]
    assert not (out / "(none).task-ids.json").exists()
    report = json.loads((out / "summary.json").read_text())
    assert report["environments"]["tmax"] == {"task_ids": 2, "excluded": {"failed": 1}}
    assert "excluded {'failed': 1}" in capsys.readouterr().out

    one = tmp_path / "one"
    assert cli.main(["task-ids", str(one), "-e", "r2e-gym"]) == 0
    assert [path.name for path in sorted(one.iterdir())] == [
        "r2e-gym.task-ids.json",
        "summary.json",
    ]
    assert cli.main(["task-ids", str(one), "-e", "nope"]) == 2

    assert cli.main(["summary"]) == 0
    table = capsys.readouterr().out
    assert "tmax" in table and "failed" in table


def test_task_ids_ready_only_keeps_built_tasks(monkeypatch, tmp_path) -> None:
    import json

    import verifiers_ucloud.cli as cli

    monkeypatch.setattr(cli, "SandboxClient", _IndexClient)
    out = tmp_path / "ready"
    assert cli.main(["task-ids", str(out), "--ready-only"]) == 0
    # tmax has unbuilt and failed names: only its ready name's tasks remain.
    assert json.loads((out / "tmax.task-ids.json").read_text()) == ["task_2"]
    # r2e-gym is all ready: its full list, as without --ready-only.
    assert json.loads((out / "r2e-gym.task-ids.json").read_text()) == ["abc123"]
    report = json.loads((out / "summary.json").read_text())
    assert report["ready_only"] is True
    assert report["environments"]["tmax"]["excluded"] == {"not_built": 2, "failed": 1}
