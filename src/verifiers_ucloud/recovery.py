"""Node loss, and bounded recovery for relay commits and managed-job reads."""

from __future__ import annotations

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
    """The gateway declared this sandbox's node lost: the sandbox is gone.

    Its own type lets a trainer retry the episode on a fresh sandbox
    (e.g. `env.retries.include = ["SandboxNodeLost"]`) instead of scoring it.
    """


# `exec_worker_lost`: the node running an exec was lost, so its sandbox is too.
_NODE_LOST = re.compile(r"\b(?:node_lost|exec_worker_lost)\b")


def is_node_lost(error: BaseException) -> bool:
    def marked(value: object) -> bool:
        if isinstance(value, dict):
            keys = ("error_code", "code", "reason", "error", "status", "state")
            return any(marked(value.get(key)) for key in keys)
        return isinstance(value, str) and _NODE_LOST.search(value) is not None

    return marked(getattr(error, "body", None)) or marked(str(error))


def node_lost(error: BaseException) -> SandboxNodeLost | None:
    """The SandboxNodeLost to raise for `error`, or None when it is not one."""
    if isinstance(error, SandboxNodeLost):
        return error
    if is_node_lost(error):
        lost = SandboxNodeLost(f"node_lost: {error}")
        lost.__cause__ = error
        return lost
    return None


class ResilientRelayWorkerClient(AsyncRelayWorkerClient):
    """Relay worker client that marks node loss and re-sends lost commits."""

    async def _terminal_checked_request(
        self, method, path, *, payload=None, timeout_seconds=None
    ):
        try:
            return await super()._request_json(
                method, path, payload=payload, timeout_seconds=timeout_seconds
            )
        except RelayApiError as error:
            if (lost := node_lost(error)) is not None:
                raise lost from error
            raise

    async def _request_json(self, method, path, *, payload=None, timeout_seconds=None):
        # A lost acknowledgement must not re-run generation. The response commit
        # is idempotent: the same request identity and bytes are sent again.
        if method != "POST" or path != "/worker/respond":
            return await self._terminal_checked_request(
                method, path, payload=payload, timeout_seconds=timeout_seconds
            )
        for attempt in range(3):
            try:
                return await self._terminal_checked_request(
                    method,
                    path,
                    payload=payload,
                    timeout_seconds=max(120.0, timeout_seconds or self.timeout_seconds),
                )
            except RelayApiError as error:
                transport = isinstance(
                    error.__cause__, (TimeoutError, ClientConnectionError)
                )
                if not transport or attempt == 2:
                    raise
                logger.warning(
                    "Retrying idempotent relay response commit after %s (%d/3)",
                    type(error.__cause__).__name__,
                    attempt + 1,
                )
                await asyncio.sleep(attempt + 1)
        raise AssertionError("unreachable")


def _retryable(error: SandboxApiError) -> bool:
    """The gateway marks answers worth repeating, such as a node whose heartbeat
    is briefly stale (503 `sandbox_worker_unreachable`)."""
    body = error.body
    return isinstance(body, dict) and body.get("retryable") is True


async def wait_for_managed_job(job, *, recovery_seconds=120.0, poll_seconds=2.0):
    """Poll a managed job until it ends, never re-launching it.

    Answers the gateway marks retryable (beyond the SDK's own few retries) are
    polled again for at most `recovery_seconds`; a lost node is terminal.
    """
    failed_since = None
    while True:
        try:
            record = await job.refresh()
        except SandboxApiError as error:
            if (lost := node_lost(error)) is not None:
                raise lost from error
            if not _retryable(error):
                raise
            now = time.monotonic()
            if failed_since is None:
                failed_since = now
                logger.warning(
                    "Managed job %s/%s unavailable (%s); polling for at most %.0fs",
                    job.sandbox_id,
                    job.job_id,
                    error,
                    recovery_seconds,
                )
            if now - failed_since >= recovery_seconds:
                raise
        else:
            failed_since = None
            if record.terminal:
                return record
        await asyncio.sleep(poll_seconds)


async def read_managed_logs(job, stream, *, offset=0):
    """Read managed-job output, retrying only explicit transient read refusals at the
    same byte offset."""
    for attempt in range(5):
        try:
            return await job.logs(stream, offset=offset)
        except SandboxApiError as error:
            if (lost := node_lost(error)) is not None:
                raise lost from error
            body = error.body
            transient = (
                error.status_code == 503
                and isinstance(body, dict)
                and body.get("error_code") == "managed_process_read_unavailable"
                and body.get("retryable") is True
                and "failed to find initial working directory"
                not in str(body.get("error", ""))
            )
            if not transient or attempt == 4:
                raise
            logger.warning(
                "Retrying managed log read %s/%s %s offset=%d (%d/5)",
                job.sandbox_id,
                job.job_id,
                stream,
                offset,
                attempt + 1,
            )
            await asyncio.sleep(2**attempt)
    raise AssertionError("unreachable")
