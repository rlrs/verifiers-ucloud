"""Host tool service exposure through our UCloud HTTP relay."""
from __future__ import annotations

import asyncio
import os
import socket
import uuid
from contextlib import asynccontextmanager

from verifiers.v1.errors import TunnelError
from verifiers.v1.interception.tunnel.base import Tunnel

from .recovery import ResilientRelayWorkerClient


class UCloudTunnel(Tunnel):
    @asynccontextmanager
    async def expose(self, port: int):
        config = self.config
        env = dict(os.environ)
        if config.relay_url is not None:
            env["UCLOUD_RELAY_URL"] = config.relay_url
        client = ResilientRelayWorkerClient.from_env(
            env=env,
            timeout_seconds=max(30.0, config.poll_timeout_seconds),
            forward_timeout_seconds=config.forward_timeout_seconds,
        )
        service_id = f"tool-{uuid.uuid4().hex}"
        worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        async with client:
            async with client.rollout_session(
                service_id, worker_id=worker_id,
                metadata={"consumer": "verifiers-shared-tools"},
            ) as tunnel:
                cancel = asyncio.Event()
                # A worker failure must tear down the owning serving scope, not leave
                # a live-looking URL whose requests can never complete.
                async def run():
                    try:
                        await tunnel.run(
                            upstream_base_url=f"http://127.0.0.1:{port}",
                            cancel=cancel, max_concurrency=64,
                            poll_timeout_seconds=config.poll_timeout_seconds,
                            lease_seconds=config.lease_seconds,
                        )
                        if not cancel.is_set():
                            raise TunnelError("UCloud tool relay worker stopped unexpectedly")
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        raise TunnelError("UCloud tool relay worker failed") from exc

                async with asyncio.TaskGroup() as workers:
                    worker = workers.create_task(run(), name=f"ucloud-{service_id}")
                    try:
                        await asyncio.sleep(0)
                        yield tunnel.base_url.rstrip("/")
                    finally:
                        cancel.set()
                        worker.cancel()
