"""Build task images before the trainer needs them.

A task whose image is a recipe the gateway has not built yet waits for its build
(minutes) when its first sandbox is created. A trainer that knows its next
batches can ask for those builds now, so they overlap the current step:

    prefetch = ImagePrefetcher()
    await prefetch.ensure(next_batches_tasks)   # verifiers Tasks, or image names

`ensure` is idempotent and cheap: it submits missing builds (the gateway builds
at most a few dozen at once) and answers each image's state; calling it again
only polls. Images must be names in the gateway's image index
(`image_reference_type = "name"`); others answer `unknown`. Failures are logged,
never raised: prefetching is advice.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable

from ucloud_sandboxes_sdk import AsyncSandboxClient, SandboxClient

logger = logging.getLogger(__name__)


def image_names(tasks_or_names: Iterable[object]) -> list[str]:
    """The distinct image names of verifiers Tasks (``task.data.image``) or names."""
    names = set()
    for item in tasks_or_names:
        name = (
            item
            if isinstance(item, str)
            else getattr(getattr(item, "data", None), "image", None)
        )
        if name:
            names.add(name)
    return sorted(names)


class ImagePrefetcher:
    """Asks the gateway to build the images of upcoming tasks (async)."""

    def __init__(self, client: AsyncSandboxClient | None = None) -> None:
        self._client = client

    async def ensure(self, tasks_or_names: Iterable[object]) -> dict[str, dict]:
        names = image_names(tasks_or_names)
        if not names:
            return {}
        try:
            if self._client is None:
                self._client = AsyncSandboxClient.from_env(timeout_seconds=60)
            statuses = await self._client.ensure_images(names, timeout_seconds=120)
        except Exception as exc:
            logger.warning("image prefetch failed (%s): %s", type(exc).__name__, exc)
            return {}
        logger.info(
            "image prefetch: %s",
            dict(Counter(status.get("state") for status in statuses.values())),
        )
        return statuses

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None


def ensure_task_images(tasks_or_names: Iterable[object]) -> dict[str, dict]:
    """`ImagePrefetcher.ensure` for synchronous callers."""
    names = image_names(tasks_or_names)
    if not names:
        return {}
    try:
        return SandboxClient.from_env(timeout_seconds=60).ensure_images(
            names, timeout_seconds=120
        )
    except Exception as exc:
        logger.warning("image prefetch failed (%s): %s", type(exc).__name__, exc)
        return {}
