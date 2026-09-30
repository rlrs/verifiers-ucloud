"""Bounded recovery for idempotent relay commits and read-only job polling."""
import asyncio
import logging
import re
import time

from aiohttp import ClientConnectionError
from ucloud_sandboxes_sdk import AsyncRelayWorkerClient, SandboxApiError
from ucloud_sandboxes_sdk.relay import RelayApiError

from verifiers.v1.errors import SandboxError

logger = logging.getLogger(__name__)


class SandboxNodeLost(SandboxError):
    """The backend explicitly declared this sandbox unrecoverable."""


def is_node_lost(error) -> bool:
    def marked(value):
        if isinstance(value, dict):
            return any(marked(value.get(key)) for key in ("error_code", "code", "reason", "error", "status", "state"))
        return isinstance(value, str) and re.search(r"\bnode_lost\b", value) is not None
    return marked(getattr(error, "body", None)) or marked(str(error))



class ResilientRelayWorkerClient(AsyncRelayWorkerClient):
    async def renew_request(self, relay_request, *, worker_id=None, lease_seconds=None):
        try:
            return await super().renew_request(relay_request, worker_id=worker_id, lease_seconds=lease_seconds)
        except RelayApiError as error:
            # Completion can race with the background renewer while response
            # delivery waits for wake. Forward/commit still determines success.
            if error.status_code != 410 or "request is already completed" not in str(error):
                raise
            logger.info("Relay lease renewal raced with completed request")
            return relay_request

    async def _terminal_checked_request(self, method, path, *, payload=None, timeout_seconds=None):
        try:
            return await super()._request_json(method, path, payload=payload, timeout_seconds=timeout_seconds)
        except RelayApiError as error:
            if is_node_lost(error):
                raise SandboxNodeLost(f"node_lost: {error}") from error
            raise

    async def _request_json(self, method, path, *, payload=None, timeout_seconds=None):
        # A lost acknowledgement must not re-run generation. The SDK's response
        # commit is idempotent and uses the same request identity and bytes.
        if method != "POST" or path != "/worker/respond":
            return await self._terminal_checked_request(method, path, payload=payload, timeout_seconds=timeout_seconds)
        for attempt in range(3):
            try:
                return await self._terminal_checked_request(
                    method, path, payload=payload,
                    timeout_seconds=max(120.0, timeout_seconds or self.timeout_seconds),
                )
            except RelayApiError as error:
                if not isinstance(error.__cause__, (TimeoutError, ClientConnectionError)) or attempt == 2:
                    raise
                logger.warning("Retrying idempotent relay response commit after %s (attempt %d/3)",
                               type(error.__cause__).__name__, attempt + 1)
                await asyncio.sleep(attempt + 1)
        raise AssertionError("unreachable")


async def wait_for_managed_job(job, *, recovery_seconds=120.0, poll_seconds=5.0):
    """Read status without re-launching the process after a transient route loss."""
    failed_since = None
    while True:
        try:
            record = await job.refresh()
        except SandboxApiError as error:
            if is_node_lost(error):
                raise SandboxNodeLost(f"node_lost: {error}") from error
            route_missing = error.status_code == 404 and "sandbox route not found" in str(error)
            if not route_missing:
                raise
            now = time.monotonic()
            if failed_since is None:
                failed_since = now
                logger.warning("Managed job route missing; polling same sandbox/job for at most %.0fs: %s/%s",
                               recovery_seconds, job.sandbox_id, job.job_id)
            if now - failed_since >= recovery_seconds:
                raise
        else:
            failed_since = None
            if record.terminal:
                return record
        await asyncio.sleep(poll_seconds)


async def read_managed_logs(job, stream, *, offset=0):
    """Retry only explicit transient read failures at the same byte offset."""
    for attempt in range(5):
        try:
            return await job.logs(stream, offset=offset)
        except SandboxApiError as error:
            if is_node_lost(error):
                raise SandboxNodeLost(f"node_lost: {error}") from error
            body = error.body
            if not (
                error.status_code == 503
                and isinstance(body, dict)
                and body.get("error_code") == "managed_process_read_unavailable"
                and body.get("retryable") is True
                and "failed to find initial working directory" not in str(body.get("error", ""))
            ) or attempt == 4:
                raise
            logger.warning(
                "Retrying managed log read %s/%s %s offset=%d (attempt %d/5)",
                job.sandbox_id, job.job_id, stream, offset, attempt + 1,
            )
            await asyncio.sleep(2 ** attempt)
    raise AssertionError("unreachable")
