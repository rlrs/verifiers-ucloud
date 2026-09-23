"""Relay-backed UCloud interception."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal

from pydantic import PositiveInt
from ucloud_sandboxes_sdk import AsyncRelayWorkerClient
from verifiers.v1.interception.base import BaseInterceptionConfig, Interception, Slot
from verifiers.v1.interception.server import InterceptionServer
from verifiers.v1.session import RolloutSession

from ._resources import ensure_file_descriptor_capacity

logger = logging.getLogger(__name__)


@dataclass
class _PhaseSession:
    registration_token: str
    sequence: int = 0


class UCloudInterceptionConfig(BaseInterceptionConfig):
    """Expose the local interception server through the polling relay."""

    type: Literal["ucloud"] = "ucloud"
    relay_url: str | None = None
    poll_timeout_seconds: float = 10.0
    lease_seconds: float = 900.0
    forward_timeout_seconds: float = 300.0
    max_inflight_requests: PositiveInt = 512
    resource_phase_hints: bool = False


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
        self._phase_sessions: dict[str, _PhaseSession] = {}
        self.worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

    async def start(self) -> None:
        ensure_file_descriptor_capacity()
        await self.stack.enter_async_context(self.server)
        env = dict(os.environ)
        if self.config.relay_url is not None:
            env["UCLOUD_RELAY_URL"] = self.config.relay_url
        relay = AsyncRelayWorkerClient.from_env(
            env=env,
            max_inflight_requests=self.config.max_inflight_requests,
            forward_timeout_seconds=self.config.forward_timeout_seconds,
            timeout_seconds=max(
                self.config.poll_timeout_seconds,
                self.config.forward_timeout_seconds,
            ),
        )
        self.relay = await self.stack.enter_async_context(relay)

    async def report_resource_phase(
        self,
        rollout_id: str,
        phase: str,
        *,
        ttl_seconds: float = 60,
        expected_remaining_wait_seconds: float | None = None,
    ) -> bool:
        """Report integration knowledge without changing sandbox lifecycle.

        Model/training hooks may call this when they know an actual phase. Do
        not infer training pauses from cancellation or quiet tool output.
        """
        if not self.config.resource_phase_hints:
            return False
        entry = self._phase_sessions.get(rollout_id)
        if entry is None or self.relay is None:
            raise RuntimeError("resource phase has no active rollout registration")
        entry.sequence += 1
        token = entry.registration_token
        try:
            result = await asyncio.wait_for(
                self.relay.update_resource_phase(
                    rollout_id,
                    sequence=entry.sequence,
                    phase=phase,
                    ttl_seconds=ttl_seconds,
                    expected_remaining_wait_seconds=expected_remaining_wait_seconds,
                    registration_token=token,
                ),
                timeout=1.0,
            )
            return bool(result.get("accepted"))
        except Exception as exc:
            # Advice is disposable. Its loss cannot fail an otherwise healthy
            # rollout, and revocation/expiry still bound a cancelled HTTP write.
            logger.warning("resource phase hint unavailable (%s)", type(exc).__name__)
            return False

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
                if self.config.resource_phase_hints:
                    registration_token = tunnel.registration_token
                    if registration_token is None:
                        raise RuntimeError("relay session has no active registration")
                    self._phase_sessions[rollout_id] = _PhaseSession(
                        registration_token
                    )
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
                        await self.report_resource_phase(rollout_id, "tool")
                        yield tunnel.base_url, model_secret, state_secret
                    finally:
                        cancel.set()
                        worker.cancel()
                await self.report_resource_phase(rollout_id, "rollout_complete")
        finally:
            self._phase_sessions.pop(rollout_id, None)
            self.server.unregister(model_secret, state_secret)
