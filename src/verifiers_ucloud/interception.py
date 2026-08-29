"""Relay-backed UCloud interception."""

from __future__ import annotations

import asyncio
import os
import socket
import uuid
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from typing import Literal

from ucloud_sandboxes_sdk import AsyncRelayWorkerClient
from verifiers.v1.interception.base import BaseInterceptionConfig, Interception, Slot
from verifiers.v1.interception.server import InterceptionServer
from verifiers.v1.session import RolloutSession


class UCloudInterceptionConfig(BaseInterceptionConfig):
    """Expose the local interception server through the polling relay."""

    type: Literal["ucloud"] = "ucloud"
    relay_url: str | None = None
    poll_timeout_seconds: float = 10.0
    lease_seconds: float = 900.0
    forward_timeout_seconds: float = 300.0


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
        await self.stack.enter_async_context(self.server)
        env = dict(os.environ)
        if self.config.relay_url is not None:
            env["UCLOUD_RELAY_URL"] = self.config.relay_url
        relay = AsyncRelayWorkerClient.from_env(
            env=env,
            timeout_seconds=max(
                self.config.poll_timeout_seconds,
                self.config.forward_timeout_seconds,
            ),
        )
        self.relay = await self.stack.enter_async_context(relay)

    @asynccontextmanager
    async def acquire(self, session: RolloutSession) -> AsyncIterator[Slot]:
        relay = self.relay
        if relay is None:
            raise RuntimeError("ucloud interception has not been started")

        rollout_id = session.trace.id
        model_secret, state_secret = self.server.register(session)
        cancel = asyncio.Event()
        try:
            async with relay.rollout_session(
                rollout_id,
                worker_id=self.worker_id,
                metadata={"consumer": "verifiers"},
            ) as tunnel:
                async with asyncio.TaskGroup() as workers:
                    worker = workers.create_task(
                        tunnel.run(
                            upstream_base_url=self.server.base_url,
                            cancel=cancel,
                            max_concurrency=8,
                            poll_timeout_seconds=self.config.poll_timeout_seconds,
                            lease_seconds=self.config.lease_seconds,
                        ),
                        name=f"ucloud-relay-{rollout_id}",
                    )
                    await asyncio.sleep(0)
                    try:
                        yield tunnel.base_url, model_secret, state_secret
                    finally:
                        cancel.set()
                        worker.cancel()
        finally:
            self.server.unregister(model_secret, state_secret)
