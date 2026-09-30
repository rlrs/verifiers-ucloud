"""Keep relay worker failures inside the current sandbox operation."""
import asyncio
from contextvars import ContextVar

from verifiers.v1.errors import TunnelError
from ucloud_sandboxes_sdk import SandboxApiError
from .recovery import SandboxNodeLost, is_node_lost

relay_worker = ContextVar("ucloud_relay_worker", default=None)


async def with_relay(operation):
    try:
        return await _with_relay(operation)
    except SandboxApiError as error:
        if is_node_lost(error):
            raise SandboxNodeLost(f"node_lost: {error}") from error
        raise


async def _with_relay(operation):
    worker = relay_worker.get()
    if worker is None:
        return await operation
    task = asyncio.create_task(operation)
    try:
        done, _ = await asyncio.wait({task, worker}, return_when=asyncio.FIRST_COMPLETED)
        if worker in done:
            try:
                await worker
            except Exception as error:
                if isinstance(error, SandboxNodeLost) or is_node_lost(error):
                    raise SandboxNodeLost(f"node_lost: {error}") from error
                raise TunnelError(f"UCloud relay worker failed: {type(error).__name__}: {error}") from error
            raise TunnelError("UCloud relay worker stopped before sandbox operation completed")
        return await task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
