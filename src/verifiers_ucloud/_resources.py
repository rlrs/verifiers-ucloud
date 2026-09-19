"""Runner resources needed by concurrent sandbox and relay connections."""

from __future__ import annotations

import logging
from functools import cache

try:
    import resource
except ImportError:  # Windows has no Unix descriptor resource limit.
    resource = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)
_CONNECTION_FILE_BUDGET = 8192


@cache
def ensure_file_descriptor_capacity() -> None:
    """Raise this runner's soft limit within its existing hard limit once."""
    if resource is None:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft == resource.RLIM_INFINITY or soft >= _CONNECTION_FILE_BUDGET:
            return
        target = (
            _CONNECTION_FILE_BUDGET
            if hard == resource.RLIM_INFINITY
            else min(_CONNECTION_FILE_BUDGET, hard)
        )
        if target > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        if target < _CONNECTION_FILE_BUDGET:
            logger.warning(
                "UCloud runner file limit is %s; 512-way sandbox/relay workloads "
                "use a recommended budget of %s. Raise the runner's hard limit "
                "(for example, systemd LimitNOFILE) before starting that workload.",
                target,
                _CONNECTION_FILE_BUDGET,
            )
    except (OSError, ValueError) as exc:
        logger.warning(
            "Could not raise UCloud runner file limit to %s: %s. "
            "Configure the launch environment with ulimit -n %s for 512-way workloads.",
            _CONNECTION_FILE_BUDGET,
            exc,
            _CONNECTION_FILE_BUDGET,
        )
