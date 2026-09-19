"""UCloud sandbox runtime."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import shlex
from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from typing import ClassVar, Literal

from pydantic import Field
from ucloud_sandboxes_sdk import (
    AsyncSandboxClient,
    AsyncSandboxProcess,
    Image,
    SandboxClient,
    SandboxSpec,
)
from verifiers.v1.configs.runtime import BaseRuntimeConfig
from verifiers.v1.errors import SandboxError
from verifiers.v1.runtimes.base import (
    BaseRuntimeInfo,
    ProgramResult,
    Runtime,
    RuntimeProcess,
    parse_gpu,
)
from verifiers.v1.runtimes.limiters import creation_limiter

from ._resources import ensure_file_descriptor_capacity

logger = logging.getLogger(__name__)

_DEFAULT_REQUEST_TIMEOUT_SECONDS = 300.0
_DEFAULT_CREATE_TIMEOUT_SECONDS = 900.0


class UCloudRuntimeConfig(BaseRuntimeConfig):
    """Configuration for one gateway-managed sandbox."""

    type: Literal["ucloud"] = "ucloud"
    image: str = "python:3.11-slim"
    workdir: str = "/app"
    network_access: bool = True
    cpu: float = 1.0
    memory: float = 2.0
    gpu: str | None = None
    disk: float = 5.0
    ttl_seconds: int = 24 * 60 * 60
    labels: dict[str, str] = Field(default_factory=dict)
    creates_per_sec: float | None = None
    request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS
    create_timeout_seconds: float = _DEFAULT_CREATE_TIMEOUT_SECONDS


class UCloudRuntimeInfo(UCloudRuntimeConfig, BaseRuntimeInfo):
    pass


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

    def _client_or_raise(self) -> AsyncSandboxClient:
        if self._client is None:
            raise SandboxError("ucloud runtime has not been started")
        return self._client

    async def start(self) -> None:
        ensure_file_descriptor_capacity()
        gpu_type, gpu_count = parse_gpu(self.config.gpu)
        if gpu_type or gpu_count:
            raise SandboxError("ucloud runtime currently supports CPU-only sandboxes")

        try:
            self._client = AsyncSandboxClient.from_env(
                timeout_seconds=self.config.request_timeout_seconds
            )
            spec = SandboxSpec.benchmark(
                id=self.name,
                image=Image.from_registry(self.config.image),
                env=self.env,
                working_dir=self.config.workdir,
                cpus=self.config.cpu,
                memory_mb=round(self.config.memory * 1024),
                disk_mb=round(self.config.disk * 1024),
                network="bridge" if self.config.network_access else "none",
                ttl_seconds=self.config.ttl_seconds,
                labels=self.config.labels,
            )
            async with (
                creation_limiter(self.config.creates_per_sec, "ucloud-sandbox")
                or contextlib.nullcontext()
            ):
                handle = await self._client.create_sandbox(
                    spec,
                    request_timeout_seconds=self.config.create_timeout_seconds,
                )
            self.info.id = handle.id
        except Exception as exc:
            client, self._client = self._client, None
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.close()
            raise SandboxError(f"ucloud sandbox provisioning failed: {exc}") from exc

    async def run(self, argv: list[str], env: dict[str, str]) -> ProgramResult:
        if self.info.id is None:
            raise SandboxError("ucloud sandbox has no id")
        try:
            result = await self._client_or_raise().exec(
                self.info.id,
                argv,
                env=self.process_env(env),
                working_dir=self.config.workdir,
            )
        except Exception as exc:
            raise SandboxError(f"ucloud exec failed: {exc}") from exc
        return ProgramResult(
            exit_code=result.exit_code if result.exit_code is not None else 1,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    async def open_process(
        self, argv: list[str], env: dict[str, str]
    ) -> RuntimeProcess:
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

    async def _read(self, path: str) -> bytes:
        if self.info.id is None:
            raise SandboxError("ucloud sandbox has no id")
        try:
            return await self._client_or_raise().download_file(
                self.info.id, self._absolute(path)
            )
        except Exception as exc:
            raise SandboxError(f"read {path!r}: {exc}") from exc

    async def write(self, path: str, data: bytes) -> None:
        if self.info.id is None:
            raise SandboxError("ucloud sandbox has no id")
        target = self._absolute(path)
        parent = str(PurePosixPath(target).parent)
        mkdir = await self.run(["mkdir", "-p", parent], {})
        if mkdir.exit_code != 0:
            raise SandboxError(f"write {path!r}: {mkdir.stderr.strip()}")
        try:
            await self._client_or_raise().upload_file(self.info.id, target, data)
        except Exception as exc:
            raise SandboxError(f"write {path!r}: {exc}") from exc

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
