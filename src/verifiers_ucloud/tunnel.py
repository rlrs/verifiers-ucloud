"""Expose host tool servers to UCloud sandboxes through the relay."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from contextlib import AsyncExitStack, asynccontextmanager

from verifiers.v1.errors import TunnelError
from verifiers.v1.interception.tunnel.base import Tunnel

from .recovery import ResilientRelayWorkerClient

logger = logging.getLogger(__name__)

_RESTART_SECONDS = (1, 2, 4, 8, 16, 30)


class UCloudTunnel(Tunnel):
    """A relay session per exposed host port, so relay-only sandboxes reach host
    MCP and tool servers the same way they reach the model."""

    @asynccontextmanager
    async def expose(self, port: int):
        config = self.config
        service_id = f"tool-{uuid.uuid4().hex}"
        async with AsyncExitStack() as stack:
            try:
                env = dict(os.environ)
                if config.relay_url is not None:
                    env["UCLOUD_RELAY_URL"] = config.relay_url
                client = await stack.enter_async_context(
                    ResilientRelayWorkerClient.from_env(
                        env=env,
                        timeout_seconds=max(30.0, config.poll_timeout_seconds),
                        forward_timeout_seconds=config.forward_timeout_seconds,
                    )
                )
                tunnel = await stack.enter_async_context(
                    client.rollout_session(
                        service_id,
                        worker_id=f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}",
                        metadata={"consumer": "verifiers-shared-tools"},
                    )
                )
            except Exception as exc:
                raise TunnelError(
                    f"UCloud tool relay for port {port} failed to start: {exc}"
                ) from exc
            cancel = asyncio.Event()

            # The URL serves every rollout's tool calls for the whole eval, so a
            # failed worker is restarted on the same session rather than ending
            # the eval; calls that fail meanwhile fail only their own rollouts.
            async def serve() -> None:
                failures = 0
                while not cancel.is_set():
                    try:
                        await tunnel.run(
                            upstream_base_url=f"http://127.0.0.1:{port}",
                            cancel=cancel,
                            max_concurrency=64,
                            poll_timeout_seconds=config.poll_timeout_seconds,
                            lease_seconds=config.lease_seconds,
                        )
                        failures = 0
                        if cancel.is_set():
                            return
                        logger.warning("UCloud tool relay %s stopped", service_id)
                    except Exception:
                        logger.exception("UCloud tool relay %s failed", service_id)
                    delay = _RESTART_SECONDS[min(failures, len(_RESTART_SECONDS) - 1)]
                    failures += 1
                    try:
                        await asyncio.wait_for(cancel.wait(), delay)
                    except TimeoutError:
                        pass

            worker = asyncio.create_task(serve(), name=f"ucloud-{service_id}")
            try:
                await asyncio.sleep(0)
                # The caller's errors propagate unchanged (the Tunnel contract).
                yield tunnel.base_url.rstrip("/")
            finally:
                cancel.set()
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
