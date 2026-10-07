"""UCloud sandbox runtime."""

from __future__ import annotations

from ._resources import ensure_file_descriptor_capacity
from .image_builds import ImageBuildFailure, ImageBuildPollingError

import asyncio
import contextlib
import dataclasses
import logging
import json
import os
from urllib.parse import urlsplit, urlunsplit
import shlex
from collections.abc import Mapping

from .supervision import with_relay
from .recovery import SandboxNodeLost, is_node_lost
from verifiers.v1.errors import TunnelError
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, ClassVar, Literal

from pydantic import Field, model_validator
from ucloud_sandboxes_sdk import (
    AsyncSandboxClient,
    AsyncSandboxHandle,
    AsyncSandboxProcess,
    Image,
    SandboxClient,
    SandboxApiError,
    SandboxSpec,
    SandboxNetworkPolicy,
    SandboxSecuritySpec,
    SandboxFilesystemSpec,
)
from verifiers.v1.configs.runtime import NetworkPolicyConfig
from verifiers.v1.errors import SandboxError
from verifiers.v1.runtimes.base import (
    BaseRuntimeInfo,
    ProgramResult,
    Runtime,
    RuntimeProcess,
    parse_gpu,
)
from verifiers.v1.runtimes.limiters import creation_limiter

from ._groups import GroupPolicy, group_creates

logger = logging.getLogger(__name__)


async def _retry_file_admission(operation):
    """Retry only idempotent file transfers rejected by CPU admission."""
    for attempt in range(8):
        try:
            return await operation()
        except SandboxApiError as exc:
            cpu_admission = any(message in str(exc) for message in (
                "direct node CPU load blocks active admission",
                "direct node CPU pressure blocks active admission",
            ))
            if exc.status_code != 503 or not cpu_admission or attempt == 7:
                raise
            delay = min(2 ** attempt, 8)
            logger.warning("Sandbox file transfer rejected by CPU admission; retrying in %ss", delay)
            await asyncio.sleep(delay)

_DEFAULT_REQUEST_TIMEOUT_SECONDS = 300.0
_DEFAULT_CREATE_TIMEOUT_SECONDS = 900.0


class UCloudRuntimeConfig(NetworkPolicyConfig):
    """Configuration for one gateway-managed sandbox."""

    type: Literal["ucloud"] = "ucloud"
    image: str = "python:3.11-slim"
    workdir: str = "/app"
    network_access: bool = True
    offline_python_bundle: Path | None = None
    offline_harness_bundle: Path | None = None
    image_reference_type: Literal["registry", "name"] = "registry"
    image_manifest: Path | None = None
    image_recipe_db: Path | None = None
    image_build_cache: Path | None = None
    guest_relay_url: str | None = None
    relay_name: str = Field(default="default", pattern=r"^[a-z][a-z0-9-]{0,31}$")
    toolkits: list[str] = Field(default_factory=list, max_length=4)
    """Read-only toolkits the gateway stacks on the image (``name:tag`` or
    ``name@sha256:<root>``); their files are under ``/opt/ucloud/toolkits/<name>``."""
    uv_toolkit: str | None = None
    """The toolkit (one of ``toolkits``, by name) whose prebuilt uv, Python and script
    environments prepare every harness and task uv script, also in relay-only
    sandboxes. Task commands keep the image's own tools."""

    @model_validator(mode="after")
    def validate_relay_policy(self):
        if self.network_restricted:
            if self.allow:
                raise ValueError("UCloud supports framework-only relay access, not custom allow/block lists")
            if not self.network_access:
                raise ValueError("Framework-only UCloud access requires bridge transport for the relay")
        names = [ref.split("@")[0].split(":")[0] for ref in self.toolkits]
        if self.uv_toolkit is not None and self.uv_toolkit not in names:
            raise ValueError("uv_toolkit must name one of the requested toolkits")
        return self
    cpu: float = 1.0
    memory: float = 2.0
    gpu: str | None = None
    disk: float = 5.0
    ttl_seconds: int = 24 * 60 * 60
    labels: dict[str, str] = Field(default_factory=dict)
    parkable: bool = False
    creates_per_sec: float | None = None
    request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS
    create_timeout_seconds: float = _DEFAULT_CREATE_TIMEOUT_SECONDS
    group_create: bool = True
    """Create the sandboxes of rollouts that start together with one spec in one
    gateway group create. Single creates remain for a lone rollout and for a
    gateway that cannot create groups."""
    group_window_seconds: float = Field(0.05, ge=0, le=5)
    """How long the first create of a spec waits for others to join its group."""
    group_max_size: int = Field(32, ge=2, le=512)
    """A group is sent at once when it reaches this size."""
    group_placement: Literal["pack", "spread"] = "pack"


class UCloudRuntimeInfo(UCloudRuntimeConfig, BaseRuntimeInfo):
    sandbox_generation: int | None = None
    sandbox_handle: Any = Field(default=None, exclude=True, repr=False)


def _same_file_key(path: str) -> str:
    """The file an absolute path names, compared as the SDK compares archive
    members. A `..` component stays in the key so the SDK still rejects it."""
    return "/".join(part for part in path.split("/") if part not in ("", "."))


async def _read_stream(reader: asyncio.StreamReader) -> AsyncIterator[bytes]:
    while chunk := await reader.read(64 * 1024):
        yield chunk


class UCloudProcess(RuntimeProcess):
    """Adapt an SDK process to the verifiers live-process contract."""

    def __init__(self, process: AsyncSandboxProcess) -> None:
        self._process = process
        self.stdout = _read_stream(process.stdout)
        self.stderr = _read_stream(process.stderr)

    async def write(self, data: bytes) -> None:
        self._process.stdin.write(data)
        await self._process.stdin.drain()

    async def wait(self) -> int:
        return await self._process.wait()

    async def terminate(self) -> None:
        await self._process.handle.terminate()

    async def kill(self) -> None:
        await self._process.handle.kill()


class UCloudRuntime(Runtime):
    config_cls = UCloudRuntimeConfig
    info_cls = UCloudRuntimeInfo
    is_local: ClassVar[bool] = False
    config: UCloudRuntimeConfig
    info: UCloudRuntimeInfo

    def __init__(self, config: UCloudRuntimeConfig, name: str | None = None) -> None:
        super().__init__(name)
        self.config = config
        self.info = UCloudRuntimeInfo(**config.model_dump())
        self._client: AsyncSandboxClient | None = None
        if config.uv_toolkit is not None:
            if not hasattr(self, "uv_env"):
                raise SandboxError("uv_toolkit needs a verifiers with Runtime.uv_env")
            root = f"/opt/ucloud/toolkits/{config.uv_toolkit}"
            self.uv_env = {
                "UV_INSTALL_DIR": f"{root}/bin",
                "UV_CACHE_DIR": f"{root}/uv-cache",
                "UV_PYTHON_INSTALL_DIR": f"{root}/python",
                "UV_PYTHON_PREFERENCE": "only-managed",
            }
        self._offline_setup_lock = asyncio.Lock()
        self._offline_prefix: str | None = None

    def _client_or_raise(self) -> AsyncSandboxClient:
        if self._client is None:
            raise SandboxError("ucloud runtime has not been started")
        return self._client

    def _resolved_image(self):
        if self.config.image_manifest is None:
            return (Image.from_name(self.config.image) if self.config.image_reference_type == "name"
                    else Image.from_registry(self.config.image))
        manifest = json.loads(self.config.image_manifest.read_text())
        if manifest.get("version") != 1:
            raise SandboxError("Unsupported prepared image manifest version")
        entry = manifest.get("images", {}).get(self.config.image)
        if not isinstance(entry, dict) or entry.get("validated") is not True or not entry.get("image"):
            raise SandboxError(f"No validated prepared image for {self.config.image!r}")
        return Image.from_name(entry["image"])

    async def start(self) -> None:
        ensure_file_descriptor_capacity()
        gpu_type, gpu_count = parse_gpu(self.config.gpu)
        if gpu_type or gpu_count:
            raise SandboxError("ucloud runtime currently supports CPU-only sandboxes")

        try:
            if self.config.image_recipe_db is not None:
                from .image_builds import prepare_image_async
                if self.config.image_build_cache is None:
                    raise ValueError("image_recipe_db requires image_build_cache")
                image = await prepare_image_async(self.config.image, self.config.image_recipe_db, self.config.image_build_cache)
            else:
                image = self._resolved_image()
            self._client = AsyncSandboxClient.from_env(
                timeout_seconds=self.config.request_timeout_seconds
            )
            spec_factory = SandboxSpec if self.config.parkable else SandboxSpec.benchmark
            parking = {}
            if self.config.parkable:
                parking = {
                    "parkable": True, "managed_process": True, "profile": "container",
                    "security": SandboxSecuritySpec(user="0:0", cap_drop=(), cap_add=(),
                        no_new_privileges=False, pids_limit=None, read_only_rootfs=False),
                    "filesystem": SandboxFilesystemSpec(tmpfs_mb=256, run_tmpfs_mb=64),
                }
            spec = spec_factory(
                **parking,
                id=self.name,
                image=image,
                env=self.env,
                working_dir=self.config.workdir,
                cpus=self.config.cpu,
                memory_mb=round(self.config.memory * 1024),
                disk_mb=round(self.config.disk * 1024),
                network="bridge" if self.config.network_access else "none",
                network_policy=(SandboxNetworkPolicy.relay_only(self.config.relay_name)
                                if self.network_restricted else SandboxNetworkPolicy()),
                ttl_seconds=self.config.ttl_seconds,
                labels=self.config.labels,
            )
            if self.config.toolkits:
                spec = dataclasses.replace(spec, toolkits=tuple(self.config.toolkits))
            member = (
                await group_creates().create(spec, self._group_policy())
                if self.config.group_create
                else None
            )
            if member is None:
                async with (
                    creation_limiter(self.config.creates_per_sec, "ucloud-sandbox")
                    or contextlib.nullcontext()
                ):
                    handle = await self._client.create_sandbox(
                        spec,
                        request_timeout_seconds=self.config.create_timeout_seconds,
                    )
            else:
                logger.debug("ucloud runtime %s is sandbox %s", self.name, member.id)
                record = dict(member.record)
                if member.generation is not None:
                    record.setdefault("generation", member.generation)
                handle = AsyncSandboxHandle(client=self._client, id=member.id, record=record)
            self.info.id = handle.id
            if self.config.parkable:
                record = handle.record
                if not record.get("spec", {}).get("parkable") or not record.get("spec", {}).get("managed_process"):
                    raise SandboxError("Gateway did not create the requested parkable managed sandbox")
                generation = record.get("generation")
                if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
                    raise SandboxError("Gateway did not return a positive sandbox generation")
                self.info.sandbox_handle = handle
                self.info.sandbox_generation = generation
            # Extracted images can have an empty hosts file on this gateway.
            # Preserve existing entries and restore standard loopback names.
            hosts = await self.run(
                ["sh", "-c", "if ! getent hosts localhost >/dev/null 2>&1; then "
                 "printf '\\n127.0.0.1 localhost\\n::1 localhost ip6-localhost ip6-loopback\\n' >> /etc/hosts; fi; "
                 "sandbox_hostname=$(cat /proc/sys/kernel/hostname); "
                 'if ! getent hosts "$sandbox_hostname" >/dev/null 2>&1; then '
                 'printf "\\n127.0.0.1 %s\\n" "$sandbox_hostname" >> /etc/hosts; fi'],
                {},
            )
            if hosts.exit_code:
                raise SandboxError(f"ucloud loopback hosts setup failed: {hosts.stderr}")
            if self.config.offline_harness_bundle is not None:
                from .offline import prepare_harness_bundle
                await prepare_harness_bundle(self)
        except Exception as exc:
            client, self._client = self._client, None
            if client is not None:
                if self.info.id is not None:
                    with contextlib.suppress(Exception):
                        await client.delete_sandbox(self.info.id)
                with contextlib.suppress(Exception):
                    await client.close()
            if isinstance(exc, SandboxNodeLost) or is_node_lost(exc):
                raise SandboxNodeLost(f"node_lost: {exc}") from exc
            if isinstance(exc, (ImageBuildFailure, ImageBuildPollingError)):
                raise
            raise SandboxError(f"ucloud sandbox provisioning failed: {exc}") from exc

    def host_url(self, url: str) -> str:
        # Shared host services publish the external relay capability. Only restricted
        # guests need its private origin; preserve the full capability path/query.
        from verifiers.v1.interception.tunnel import configured_host_tunnel

        transport = getattr(configured_host_tunnel(), "config", None)
        guest_url = self.config.guest_relay_url or getattr(transport, "guest_relay_url", None)
        if self.network_restricted and guest_url:
            public = urlsplit(getattr(transport, "relay_url", None) or os.environ.get("UCLOUD_RELAY_URL", ""))
            parsed = urlsplit(url)
            if public.netloc and (parsed.scheme, parsed.netloc) == (public.scheme, public.netloc):
                guest = urlsplit(guest_url)
                prefix = public.path.rstrip("/")
                if prefix and parsed.path != prefix and not parsed.path.startswith(prefix + "/"):
                    return super().host_url(url)
                path = guest.path.rstrip("/") + parsed.path[len(prefix):]
                return urlunsplit((guest.scheme, guest.netloc, path, parsed.query, parsed.fragment))
        return super().host_url(url)

    async def prepare_uv_script(self, script, env=None, *, activate=True):
        # A uv toolkit's prebuilt environments prepare scripts without the network.
        if not self.network_restricted or self.config.uv_toolkit is not None:
            return await super().prepare_uv_script(script, env, activate=activate)
        from .offline import prepare_script
        return await prepare_script(self, script, env, activate=activate)

    async def prepare_execution(self, routes: list[str] | None) -> None:
        if not self.network_restricted:
            return
        if routes is None:
            raise SandboxError("Relay-only UCloud networking is immutable; setup must use baked or staged dependencies")
        relay = urlsplit(self.config.guest_relay_url or os.environ.get("UCLOUD_RELAY_URL", ""))
        def origin(url):
            parsed = urlsplit(url)
            return parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)
        if not relay.hostname or any(origin(route) != origin(relay.geturl()) for route in routes):
            raise SandboxError("Framework route is outside the configured UCloud relay origin")
        # The gateway enforces the named relay policy from sandbox creation onward.

    def _group_policy(self) -> GroupPolicy:
        return GroupPolicy(
            window_seconds=self.config.group_window_seconds,
            max_size=self.config.group_max_size,
            placement=self.config.group_placement,
            request_timeout_seconds=self.config.request_timeout_seconds,
            create_timeout_seconds=self.config.create_timeout_seconds,
            creates_per_sec=self.config.creates_per_sec,
        )

    async def run(self, argv: list[str], env: dict[str, str]) -> ProgramResult:
        if self.info.id is None:
            raise SandboxError("ucloud sandbox has no id")
        try:
            result = await with_relay(self._client_or_raise().exec(
                self.info.id,
                argv,
                env=self.process_env(env),
                working_dir=self.config.workdir,
            ))
        except (TunnelError, SandboxNodeLost):
            raise
        except Exception as exc:
            raise SandboxError(f"ucloud exec failed: {exc}") from exc
        return ProgramResult(
            exit_code=result.exit_code if result.exit_code is not None else 1,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    async def run_program(self, argv: list[str], env: dict[str, str]) -> ProgramResult:
        if not self.config.parkable:
            return await self.run(argv, env)
        handle = self.info.sandbox_handle
        if handle is None:
            raise SandboxError("Parkable runtime has no validated sandbox handle")
        job = None
        try:
            # Start exactly once. Polling never restarts the agent program.
            job = await handle.start_agent(argv, env=self.process_env(env), working_dir=self.config.workdir)
            from .recovery import read_managed_logs, wait_for_managed_job
            record = await with_relay(wait_for_managed_job(job))
            outputs = []
            for stream in ("stdout", "stderr"):
                chunks = []
                offset = 0
                while True:
                    chunk = await with_relay(read_managed_logs(job, stream, offset=offset))
                    chunks.append(chunk.data)
                    if chunk.eof:
                        break
                    if chunk.next_offset <= offset:
                        raise SandboxError("Managed job log cursor did not advance")
                    offset = chunk.next_offset
                outputs.append(b"".join(chunks).decode("utf-8", errors="replace"))
            if record.stdout_truncated or record.stderr_truncated:
                raise SandboxError("Managed agent output exceeded its log limit")
            return ProgramResult(exit_code=record.exit_code if record.exit_code is not None else 1,
                                 stdout=outputs[0], stderr=outputs[1])
        except (TunnelError, SandboxNodeLost) as exc:
            if is_node_lost(exc):
                job = None  # The lost node cannot receive a process signal.
            raise
        except Exception as exc:
            raise SandboxError(f"ucloud managed agent failed: {exc}") from exc
        finally:
            if job is not None and not job.record.terminal:
                with contextlib.suppress(Exception):
                    await asyncio.shield(job.signal(9))

    async def open_process(
        self, argv: list[str], env: dict[str, str]
    ) -> RuntimeProcess:
        if self.config.parkable:
            from .managed_process import ManagedProcess
            return await ManagedProcess.start(self, argv, env)
        if self.info.id is None:
            raise SandboxError("ucloud sandbox has no id")
        try:
            process = await self._client_or_raise().open_process(
                self.info.id,
                argv,
                env=self.process_env(env),
                working_dir=self.config.workdir,
            )
        except Exception as exc:
            if isinstance(exc, SandboxNodeLost) or is_node_lost(exc):
                raise SandboxNodeLost(f"node_lost: {exc}") from exc
            raise SandboxError(f"ucloud live process failed to start: {exc}") from exc
        return UCloudProcess(process)

    async def run_background(
        self, argv: list[str], env: dict[str, str], log: str
    ) -> None:
        command = f"nohup {shlex.join(argv)} > {shlex.quote(log)} 2>&1 &"
        result = await self.run(["sh", "-c", command], env)
        if result.exit_code != 0:
            raise SandboxError(
                f"ucloud background launch failed: {result.stderr.strip()}"
            )

    def _absolute(self, path: str) -> str:
        if path.startswith("/"):
            return path
        return f"{self.config.workdir.rstrip('/')}/{path}"

    async def _read(self, path: str, max_bytes: int | None = None) -> bytes:
        if max_bytes is not None:
            return await super()._read(path, max_bytes=max_bytes)
        if self.info.id is None:
            raise SandboxError("ucloud sandbox has no id")
        try:
            return await _retry_file_admission(lambda: self._client_or_raise().download_file(
                self.info.id, self._absolute(path)
            ))
        except Exception as exc:
            if isinstance(exc, SandboxNodeLost) or is_node_lost(exc):
                raise SandboxNodeLost(f"node_lost: {exc}") from exc
            raise SandboxError(f"read {path!r}: {exc}") from exc

    async def write(self, path: str, data: bytes) -> None:
        if self.info.id is None:
            raise SandboxError("ucloud sandbox has no id")
        # The upload creates missing parent directories: one request per file.
        try:
            target = self._absolute(path)
            await _retry_file_admission(lambda: self._client_or_raise().upload_file(self.info.id, target, data))
        except Exception as exc:
            if isinstance(exc, SandboxNodeLost) or is_node_lost(exc):
                raise SandboxNodeLost(f"node_lost: {exc}") from exc
            raise SandboxError(f"write {path!r}: {exc}") from exc

    async def write_many(self, files: Mapping[str, bytes]) -> None:
        # Paths that name one file keep the last data, as sequential writes would.
        batch: dict[str, tuple[str, bytes]] = {}
        for path, data in files.items():
            absolute = self._absolute(path)
            batch[_same_file_key(absolute)] = (absolute, data)
        if not batch:
            return
        if self.info.id is None:
            raise SandboxError("ucloud sandbox has no id")
        # One archive request; the SDK's default mode is the one `write` produces.
        try:
            await _retry_file_admission(lambda: self._client_or_raise().upload_files(
                self.info.id, dict(batch.values()), base_dir="/"
            ))
        except Exception as exc:
            if isinstance(exc, SandboxNodeLost) or is_node_lost(exc):
                raise SandboxNodeLost(f"node_lost: {exc}") from exc
            raise SandboxError(f"write {len(batch)} files: {exc}") from exc

    def cleanup(self) -> None:
        sandbox_id = self.info.id
        if self._client is None or sandbox_id is None:
            return
        try:
            client = SandboxClient.from_env(
                timeout_seconds=self.config.request_timeout_seconds
            )
            client.delete_sandbox(sandbox_id)
        except Exception:
            logger.exception("ucloud synchronous cleanup failed for %s", sandbox_id)
        self._client = None

    async def teardown(self) -> None:
        client = self._client
        if client is None:
            return
        if self.info.id is not None:
            try:
                await client.delete_sandbox(self.info.id)
            except Exception as exc:
                logger.warning(
                    "ucloud failed to delete sandbox %s: %s", self.info.id, exc
                )
        self._client = None
        with contextlib.suppress(Exception):
            await client.close()
