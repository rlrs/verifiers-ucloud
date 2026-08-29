"""Relay-backed UCloud interception."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import uuid
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from typing import Literal
from urllib.parse import quote

from ucloud_sandboxes_sdk import AsyncRelayWorkerClient, RelayRequest
from verifiers.v1.interception.base import BaseInterceptionConfig, Interception, Slot
from verifiers.v1.interception.server import InterceptionServer
from verifiers.v1.session import RolloutSession

logger = logging.getLogger(__name__)


class UCloudInterceptionConfig(BaseInterceptionConfig):
    """Expose the local interception server through the polling relay."""

    type: Literal["ucloud"] = "ucloud"
    relay_url: str | None = None
    poll_timeout_seconds: float = 10.0
    lease_seconds: float = 900.0
    forward_timeout_seconds: float = 300.0


def _relay_url(configured: str | None) -> str:
    value = configured or next(
        (
            os.environ[name]
            for name in ("UCLOUD_RELAY_URL", "VF_UCLOUD_RELAY_URL", "VF_RELAY_URL")
            if os.environ.get(name)
        ),
        None,
    )
    if value is None:
        raise RuntimeError("set UCLOUD_RELAY_URL to the UCloud interception relay")
    return value.rstrip("/")


def _registration_token(payload: dict) -> str:
    record = payload.get("rollout")
    token = record.get("registration_token") if isinstance(record, dict) else None
    if not isinstance(token, str) or not token:
        raise RuntimeError("relay registration did not return an access token")
    return token


class UCloudInterception(Interception):
    config_cls = UCloudInterceptionConfig

    def __init__(
        self,
        config: UCloudInterceptionConfig,
        requires_tunnel: bool = False,
        state_service_secrets: Collection[str] = (),
    ) -> None:
        super().__init__()
        self.config = config
        self.requires_tunnel = requires_tunnel
        self.server = InterceptionServer(
            requires_tunnel=False,
            state_service_secrets=state_service_secrets,
        )
        self.relay: AsyncRelayWorkerClient | None = None
        self.worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

    async def start(self) -> None:
        await self.stack.enter_async_context(self.server)
        relay = AsyncRelayWorkerClient(
            _relay_url(self.config.relay_url),
            worker_token=os.environ.get("UCLOUD_RELAY_WORKER_TOKEN"),
            timeout_seconds=max(
                self.config.poll_timeout_seconds,
                self.config.forward_timeout_seconds,
            ),
        )
        self.relay = relay
        self.stack.push_async_callback(relay.close)

    @asynccontextmanager
    async def acquire(self, session: RolloutSession) -> AsyncIterator[Slot]:
        relay = self.relay
        if relay is None:
            raise RuntimeError("ucloud interception has not been started")

        tunnel_id = session.trace.id
        model_secret, state_secret = self.server.register(session)
        registered = False
        worker: asyncio.Task[None] | None = None
        try:
            response = await relay.register_tunnel(
                tunnel_id, metadata={"consumer": "verifiers"}
            )
            registered = True
            access_token = _registration_token(response)
            worker = asyncio.create_task(
                self._poll(relay, tunnel_id),
                name=f"ucloud-relay-{tunnel_id}",
            )
            base_url = (
                f"{relay.relay_url}/tunnels/{quote(tunnel_id, safe='')}"
                f"/_relay/{quote(access_token, safe='')}"
            )
            yield base_url, model_secret, state_secret
        finally:
            if worker is not None:
                worker.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker
            if registered:
                with contextlib.suppress(Exception):
                    await relay.unregister_tunnel(tunnel_id)
            self.server.unregister(model_secret, state_secret)

    async def _poll(self, relay: AsyncRelayWorkerClient, tunnel_id: str) -> None:
        forwards: set[asyncio.Task[None]] = set()
        try:
            while True:
                try:
                    result = await relay.poll(
                        tunnel_id,
                        worker_id=self.worker_id,
                        timeout_seconds=self.config.poll_timeout_seconds,
                        limit=8,
                        lease_seconds=self.config.lease_seconds,
                    )
                    for request in result.requests:
                        task = asyncio.create_task(self._forward(relay, request))
                        forwards.add(task)
                        task.add_done_callback(forwards.discard)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("ucloud relay polling failed for %s", tunnel_id)
                    await asyncio.sleep(1)
        finally:
            for task in forwards:
                task.cancel()
            await asyncio.gather(*forwards, return_exceptions=True)

    async def _forward(
        self, relay: AsyncRelayWorkerClient, request: RelayRequest
    ) -> None:
        try:
            await relay.forward_to(
                request,
                self.server.base_url,
                timeout_seconds=self.config.forward_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "ucloud relay forwarding failed for %s", request.request_id
            )
            with contextlib.suppress(Exception):
                await relay.error_request(request, str(exc))
