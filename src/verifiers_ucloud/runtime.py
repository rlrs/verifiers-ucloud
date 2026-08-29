"""UCloud sandbox runtime."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shlex
from pathlib import PurePosixPath
from typing import ClassVar, Literal

from pydantic import Field
from ucloud_sandboxes_sdk import (
    AsyncSandboxClient,
    Image,
    SandboxApiError,
    SandboxClient,
)
from verifiers.v1.configs.runtime import BaseRuntimeConfig
from verifiers.v1.errors import SandboxError
from verifiers.v1.runtimes.base import (
    BaseRuntimeInfo,
    ProgramResult,
    Runtime,
    parse_gpu,
)
from verifiers.v1.runtimes.limiters import creation_limiter

logger = logging.getLogger(__name__)

_DEFAULT_REQUEST_TIMEOUT_SECONDS = 300.0
_DEFAULT_CREATE_TIMEOUT_SECONDS = 900.0
_TRANSIENT_STATUS_CODES = {502, 503, 504}


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


def _sandbox_api_url() -> str:
    value = next(
        (
            os.environ[name]
            for name in (
                "UCLOUD_SANDBOX_API_URL",
                "UCLOUD_SANDBOX_URL",
                "UCLOUD_SANDBOX_BASE_URL",
            )
            if os.environ.get(name)
        ),
        None,
    )
    if value is None:
        raise RuntimeError("set UCLOUD_SANDBOX_API_URL to the UCloud sandbox gateway")
    return value


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
        gpu_type, gpu_count = parse_gpu(self.config.gpu)
        if gpu_type or gpu_count:
            raise SandboxError("ucloud runtime currently supports CPU-only sandboxes")

        try:
            self._client = AsyncSandboxClient(
                _sandbox_api_url(),
                api_token=os.environ.get("UCLOUD_SANDBOX_API_TOKEN"),
                timeout_seconds=self.config.request_timeout_seconds,
            )
            async with (
                creation_limiter(self.config.creates_per_sec, "ucloud-sandbox")
                or contextlib.nullcontext()
            ):
                handle = await self._create()
            self.info.id = handle.id
            await self._wait_until_ready()
        except Exception as exc:
            raise SandboxError(f"ucloud sandbox provisioning failed: {exc}") from exc

    async def _create(self):
        client = self._client_or_raise()
        deadline = (
            asyncio.get_running_loop().time() + self.config.create_timeout_seconds
        )
        while True:
            try:
                return await client.create_sandbox(
                    id=self.name,
                    image=Image.from_registry(self.config.image),
                    command=["tail", "-f", "/dev/null"],
                    env=self.env,
                    working_dir=self.config.workdir,
                    cpus=self.config.cpu,
                    memory_mb=round(self.config.memory * 1024),
                    disk_mb=round(self.config.disk * 1024),
                    network="bridge" if self.config.network_access else "none",
                    ttl_seconds=self.config.ttl_seconds,
                    labels=self.config.labels,
                    request_timeout_seconds=self.config.request_timeout_seconds,
                )
            except Exception as exc:
                if not _transient(exc) or asyncio.get_running_loop().time() >= deadline:
                    raise
                # Creation is keyed by a stable sandbox id. If the response was
                # lost, accept the existing sandbox instead of duplicating it.
                with contextlib.suppress(Exception):
                    if await client.get_sandbox(self.name) is not None:
                        from ucloud_sandboxes_sdk import AsyncSandboxHandle

                        return AsyncSandboxHandle(client, self.name)
                await asyncio.sleep(1)

    async def _wait_until_ready(self) -> None:
        deadline = (
            asyncio.get_running_loop().time() + self.config.create_timeout_seconds
        )
        while True:
            try:
                result = await self.run(["mkdir", "-p", self.config.workdir], {})
                if result.exit_code == 0:
                    return
                detail = result.stderr.strip() or f"exit code {result.exit_code}"
                exc: Exception = RuntimeError(detail)
            except Exception as error:
                exc = error
            if asyncio.get_running_loop().time() >= deadline:
                raise exc
            await asyncio.sleep(1)

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
            client = SandboxClient(
                _sandbox_api_url(),
                api_token=os.environ.get("UCLOUD_SANDBOX_API_TOKEN"),
                timeout_seconds=self.config.request_timeout_seconds,
            )
            with contextlib.suppress(Exception):
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


def _transient(exc: Exception) -> bool:
    return isinstance(exc, (TimeoutError, OSError)) or (
        isinstance(exc, SandboxApiError) and exc.status_code in _TRANSIENT_STATUS_CODES
    )
