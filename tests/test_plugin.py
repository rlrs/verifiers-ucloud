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


def test_a_briefly_missing_job_route_is_polled_again_not_restarted() -> None:
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
