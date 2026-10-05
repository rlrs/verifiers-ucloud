"""Coalesce concurrent identical sandbox creates into gateway group creates.

verifiers starts each rollout's runtime on its own, with no group hint, but the
rollouts of one example start together and ask for the same sandbox. Creates of
one spec that arrive within a fixed window (or until a fixed size) become one
`/v1/sandboxes:batch` request: the gateway packs the members onto few workers,
which attach the image once each. Every runtime takes one member and deletes it
on teardown as before. A lone create, and every create once the gateway answers
that it cannot create groups, stays a single create.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import uuid
import weakref
from collections.abc import Awaitable, Coroutine
from dataclasses import dataclass, field

from ucloud_sandboxes_sdk import (
    AsyncSandboxClient,
    SandboxApiError,
    SandboxGroupStatus,
    SandboxGroupUnavailableError,
    SandboxSpec,
)
from verifiers.v1.errors import SandboxError
from verifiers.v1.runtimes.limiters import creation_limiter

logger = logging.getLogger(__name__)

Waiter = asyncio.Future[str | None]


@dataclass(frozen=True)
class GroupPolicy:
    window_seconds: float
    max_size: int
    placement: str
    request_timeout_seconds: float
    create_timeout_seconds: float
    # Paces create requests; one group create is one request.
    creates_per_sec: float | None


@dataclass(eq=False)
class _Batch:
    spec: SandboxSpec
    policy: GroupPolicy
    gateway: str
    waiters: list[Waiter] = field(default_factory=list)
    timer: asyncio.TimerHandle | None = None


class GroupCreates:
    """One event loop's open batches, keyed by everything the request carries."""

    def __init__(self) -> None:
        self._open: dict[tuple[object, ...], _Batch] = {}
        self._running: set[asyncio.Task[None]] = set()
        # Gateways that answered they cannot create groups (ranked placement).
        self._unavailable: set[str] = set()

    async def create(self, spec: SandboxSpec, policy: GroupPolicy) -> str | None:
        """The group member this create became, or None to create it singly."""
        gateway = os.environ.get("UCLOUD_SANDBOX_URL", "")
        if gateway in self._unavailable:
            return None
        template = {k: v for k, v in spec.to_dict().items() if k != "id"}
        key = (
            gateway,
            os.environ.get("UCLOUD_SANDBOX_API_TOKEN"),
            json.dumps(template, sort_keys=True),
            spec.image.name,
            spec.image.tag,
            policy,
        )
        loop = asyncio.get_running_loop()
        batch = self._open.get(key)
        if batch is None:
            batch = self._open[key] = _Batch(spec, policy, gateway)
            batch.timer = loop.call_later(policy.window_seconds, self._flush, key)
        waiter: Waiter = loop.create_future()
        batch.waiters.append(waiter)
        if len(batch.waiters) >= policy.max_size:
            self._flush(key)
        # A cancelled caller cancels its waiter: unsent, it is left out of the
        # request; sent, its member is deleted once the gateway reports it.
        try:
            return await waiter
        except asyncio.CancelledError:
            if waiter.done() and not waiter.cancelled() and waiter.exception() is None:
                # Cancelled in the tick after its member arrived.
                member = waiter.result()
                if member is not None:
                    self._spawn(_abandon(member, policy))
            raise

    def _spawn(self, work: Coroutine[object, object, None]) -> None:
        task = asyncio.get_running_loop().create_task(work)
        self._running.add(task)
        task.add_done_callback(self._running.discard)

    def _flush(self, key: tuple[object, ...]) -> None:
        batch = self._open.pop(key, None)
        if batch is None:
            return
        if batch.timer is not None:
            batch.timer.cancel()
        waiters = [waiter for waiter in batch.waiters if not waiter.done()]
        if len(waiters) < 2:
            for waiter in waiters:
                waiter.set_result(None)
            return
        self._spawn(self._run(batch, waiters))

    async def _run(self, batch: _Batch, waiters: list[Waiter]) -> None:
        policy = batch.policy
        group_id = f"vf-{uuid.uuid4().hex}"
        members = {f"{group_id}-{index:04d}": w for index, w in enumerate(waiters)}
        owned: set[str] = set()

        def deliver(status: SandboxGroupStatus) -> None:
            # Each rollout starts as soon as the gateway places its member.
            for member in status.members:
                waiter = members.get(member.id)
                if member.placed and waiter is not None and not waiter.done():
                    owned.add(member.id)
                    waiter.set_result(member.id)

        try:
            client = AsyncSandboxClient.from_env(
                timeout_seconds=policy.request_timeout_seconds
            )
        except Exception as exc:
            _fail(waiters, group_id, exc)
            return
        async with client:
            try:
                async with (
                    creation_limiter(policy.creates_per_sec, "ucloud-sandbox")
                    or contextlib.nullcontext()
                ):
                    await client.create_sandbox_group(
                        group_id,
                        batch.spec,
                        count=len(waiters),
                        placement=policy.placement,
                        request_timeout_seconds=policy.create_timeout_seconds,
                        on_progress=deliver,
                    )
            except SandboxGroupUnavailableError:
                logger.info(
                    "ucloud gateway %s does not create groups; creating singly",
                    batch.gateway,
                )
                self._unavailable.add(batch.gateway)
                for waiter in waiters:
                    if not waiter.done():
                        waiter.set_result(None)
                return
            except BaseException as exc:
                _fail(waiters, group_id, exc)
                await _release(client, group_id, set(members) - owned, owned)
                if not isinstance(exc, Exception):
                    raise
                return
            logger.info("ucloud group %s: %d sandboxes", group_id, len(members))
            await _release(client, group_id, set(members) - owned, owned)


def _fail(waiters: list[Waiter], group_id: str, exc: BaseException) -> None:
    for waiter in waiters:
        if not waiter.done():
            error = SandboxError(f"ucloud group create {group_id} failed: {exc}")
            error.__cause__ = exc
            waiter.set_exception(error)


async def _release(
    client: AsyncSandboxClient, group_id: str, unowned: set[str], owned: set[str]
) -> None:
    """Delete the members no rollout took: unplaced, or placed after their
    rollout went away. A group no rollout took is deleted whole, which also
    refuses it the members a queued replay would still place."""
    if not unowned:
        return
    if not owned:
        await _quietly(client.delete_sandbox_group(group_id), group_id)
        return
    await asyncio.gather(
        *(_quietly(client.delete_sandbox(sid), sid) for sid in sorted(unowned))
    )


async def _abandon(member: str, policy: GroupPolicy) -> None:
    async with AsyncSandboxClient.from_env(
        timeout_seconds=policy.request_timeout_seconds
    ) as client:
        await _quietly(client.delete_sandbox(member), member)


async def _quietly(call: Awaitable[object], name: str) -> None:
    try:
        await call
    except SandboxApiError as exc:
        if exc.status_code != 404:  # Never created, or already gone.
            logger.warning("ucloud failed to delete %s: %s", name, exc)
    except Exception as exc:  # Cleanup is best-effort, as teardown is.
        logger.warning("ucloud failed to delete %s: %s", name, exc)


_COALESCERS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, GroupCreates] = (
    weakref.WeakKeyDictionary()
)


def group_creates() -> GroupCreates:
    """This event loop's coalescer; asyncio state never crosses loops."""
    loop = asyncio.get_running_loop()
    creates = _COALESCERS.get(loop)
    if creates is None:
        creates = _COALESCERS[loop] = GroupCreates()
    return creates
