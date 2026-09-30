"""Relay-backed UCloud interception."""

from __future__ import annotations

from ._resources import ensure_file_descriptor_capacity

import asyncio
import logging
import os
import socket
import uuid
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from typing import Literal
from urllib.parse import urlsplit

from pydantic import field_validator

from .recovery import ResilientRelayWorkerClient as AsyncRelayWorkerClient

from .supervision import relay_worker
from verifiers.v1.interception.base import BaseInterceptionConfig, Interception, Slot
from verifiers.v1.interception.server import InterceptionServer
from verifiers.v1.session import RolloutSession


class UCloudInterceptionConfig(BaseInterceptionConfig):
    """Expose the local interception server through the polling relay."""

    type: Literal["ucloud"] = "ucloud"
    relay_url: str | None = None
    guest_relay_url: str | None = None

    @field_validator("guest_relay_url")
    @classmethod
    def validate_guest_relay_url(cls, value):
        if value is not None:
            parsed = urlsplit(value)
            if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                    or parsed.username or parsed.password or parsed.query or parsed.fragment
                    or parsed.path not in {"", "/"}):
                raise ValueError("guest_relay_url must be an HTTP(S) origin")
            _ = parsed.port
        return value
    def host_tunnel(self):
        from .tunnel import UCloudTunnel

        return UCloudTunnel(self)

    poll_timeout_seconds: float = 10.0
    lease_seconds: float = 900.0
    forward_timeout_seconds: float = 7200.0


class UCloudInterception(Interception):
    config_cls = UCloudInterceptionConfig

    def __init__(
        self,
        config: UCloudInterceptionConfig,
        requires_tunnel: bool = False,
        state_service_secrets: Collection[str] = (),
    ) -> None:
        super().__init__()
        del requires_tunnel
        self.config = config
        self.server = InterceptionServer(
            requires_tunnel=False,
            state_service_secrets=state_service_secrets,
        )
        self.relay: AsyncRelayWorkerClient | None = None
        self.worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

    async def start(self) -> None:
        ensure_file_descriptor_capacity()
        await self.stack.enter_async_context(self.server)
        env = dict(os.environ)
        if self.config.relay_url is not None:
            env["UCLOUD_RELAY_URL"] = self.config.relay_url
        relay = AsyncRelayWorkerClient.from_env(
            env=env,
            timeout_seconds=max(30.0, self.config.poll_timeout_seconds),
            forward_timeout_seconds=self.config.forward_timeout_seconds,
        )
        self.relay = await self.stack.enter_async_context(relay)

    @asynccontextmanager
    async def acquire(self, session: RolloutSession) -> AsyncIterator[Slot]:
        relay = self.relay
        if relay is None:
            raise RuntimeError("ucloud interception has not been started")

        rollout_id = session.trace.id
        runtime_info = getattr(getattr(session.trace, "agent", None), "runtime", None)
        sandbox = None
        if getattr(runtime_info, "parkable", False):
            sandbox = runtime_info.sandbox_handle
            if sandbox is None:
                raise RuntimeError("Parkable rollout has no validated sandbox handle")
        model_secret, state_secret = self.server.register(session)
        cancel = asyncio.Event()
        try:
            async with relay.rollout_session(
                rollout_id,
                worker_id=self.worker_id,
                metadata={"consumer": "verifiers"},
                sandbox=sandbox,
            ) as tunnel:
                async def run_worker() -> None:
                    try:
                        await tunnel.run(
                            upstream_base_url=self.server.base_url,
                            cancel=cancel,
                            max_concurrency=8,
                            poll_timeout_seconds=self.config.poll_timeout_seconds,
                            lease_seconds=self.config.lease_seconds,
                        )
                    except Exception:
                        # Log before cleanup; with_relay records this as a
                        # TunnelError on the affected rollout.
                        logging.getLogger(__name__).exception(
                            "UCloud relay worker failed: rollout=%s", rollout_id
                        )
                        raise

                worker = asyncio.create_task(
                    run_worker(), name=f"ucloud-relay-{rollout_id}"
                )
                token = relay_worker.set(worker)
                try:
                    await asyncio.sleep(0)
                    base_url = tunnel.base_url.rstrip("/")
                    # Keep host state/tool callbacks on the public relay origin.
                    # UCloudRuntime.host_url translates only the guest's URLs.
                    yield base_url, model_secret, state_secret
                finally:
                    relay_worker.reset(token)
                    cancel.set()
                    worker.cancel()
                    await asyncio.gather(worker, return_exceptions=True)
        finally:
            self.server.unregister(model_secret, state_secret)
