"""Surface a rollout's relay worker failure in its current sandbox operation."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar

from verifiers.v1.errors import TunnelError

from .recovery import node_lost

relay_worker: ContextVar[asyncio.Task | None] = ContextVar(
    "ucloud_relay_worker", default=None
)
"""The relay worker task of the rollout running in this context, if any."""


async def with_relay(operation):
    """Await a sandbox operation, failing it if the rollout's relay worker dies.

    Without this, a sandbox waiting on a model call whose relay worker failed
    would wait until its own timeout. The failure stays with this rollout: no
    sibling is cancelled, and external cancellation propagates unchanged.
    """
    try:
        return await _with_relay(operation)
    except Exception as error:
        if (lost := node_lost(error)) is not None and lost is not error:
            raise lost from error
        raise


async def _with_relay(operation):
    worker = relay_worker.get()
    if worker is None:
        return await operation
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait(
            {task, worker}, return_when=asyncio.FIRST_COMPLETED
        )
        if worker in done and task not in done:
            try:
                await worker
            except Exception as error:
                if (lost := node_lost(error)) is not None:
                    raise lost from error
                raise TunnelError(
                    f"UCloud relay worker failed: {type(error).__name__}: {error}"
                ) from error
            raise TunnelError(
                "UCloud relay worker stopped before the sandbox operation"
            )
        return await task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
