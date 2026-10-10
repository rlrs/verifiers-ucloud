"""``verifiers-ucloud``: what the gateway can serve a trainer.

  summary                   the gateway's image index: names and tasks per
                            environment, by state
  task-ids DIR [-e ENV ...] [--ready-only]
                            each environment's task_ids_file
                            (DIR/<environment>.task-ids.json) and DIR/summary.json;
                            --ready-only keeps only tasks whose image is built
  ensure NAME ... [--from FILE]
                            build images ahead (each image's state; repeat to poll)

The gateway creates sandboxes only for image names in its index. Pass each
environment's file as its taskset's `task_ids_file` so the trainer samples only
those tasks, and export again when the index changes. The gateway URL and key
come from UCLOUD_SANDBOX_URL and UCLOUD_SANDBOX_API_TOKEN.
"""

from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ucloud_sandboxes_sdk import SandboxClient

STATES = ("ready", "building", "not_built", "retrying", "failed")


def _client() -> SandboxClient:
    return SandboxClient.from_env(timeout_seconds=120)


def _write(path: Path, value: object) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=1) + "\n")
    temporary.replace(path)


def _environments(summary: dict) -> list[str]:
    # "(none)" holds names registered without an environment: no tasks to sample.
    return [name for name in summary["environments"] if name != "(none)"]


def summary(args: argparse.Namespace) -> int:
    index = _client().image_index_summary()
    if args.json:
        print(json.dumps(index, indent=1))
        return 0
    columns = ("names", "tasks", *STATES)
    print(f"{'environment':<16}" + "".join(f"{column:>11}" for column in columns))
    for environment, row in index["environments"].items():
        states = row.get("states", {})
        values = [row.get("names", 0), row.get("tasks", 0)]
        values += [states.get(state, 0) for state in STATES]
        print(f"{environment:<16}" + "".join(f"{value:>11,}" for value in values))
    return 0


def _ready_task_ids(client: SandboxClient, environment: str, row: dict) -> dict:
    """The tasks whose image name is ready now, so a sandbox for them starts
    without waiting for a build; `excluded` counts the other names, by state."""
    states = row.get("states", {})
    if states.get("ready", 0) == row.get("names", 0):
        return client.image_index_task_ids(environment)
    ready = [
        entry["name"]
        for entry in client.image_index_names(
            environment=environment, state="ready", page_size=5000
        )
    ]
    with ThreadPoolExecutor(16) as pool:
        details = list(pool.map(client.image_index_name, ready))
    # A name's detail lists at most 100 of its tasks: never export a cut list.
    cut = [d["name"] for d in details if d and d.get("tasks", 0) > len(d["task_ids"])]
    if cut:
        raise SystemExit(
            f"{environment}: {len(cut)} ready names serve more than 100 tasks (e.g. "
            f"{cut[0]}); --ready-only needs every name ready here, or a gateway that "
            "filters task ids by state"
        )
    ids = sorted({task for detail in details if detail for task in detail["task_ids"]})
    excluded = {
        state: count for state, count in states.items() if state != "ready" and count
    }
    return {"environment": environment, "task_ids": ids, "excluded": excluded}


def task_ids(args: argparse.Namespace) -> int:
    client = _client()
    index = client.image_index_summary()
    known = _environments(index)
    unknown = sorted(set(args.environment) - set(known))
    if unknown:
        print(f"not in the image index: {', '.join(unknown)}", file=sys.stderr)
        return 2
    args.directory.mkdir(parents=True, exist_ok=True)
    # Without --ready-only, `excluded` counts tasks (failed names'); with it, names.
    report: dict = {
        "gateway": client.base_url,
        "ready_only": args.ready_only,
        "environments": {},
    }
    for environment in args.environment or known:
        if args.ready_only:
            answer = _ready_task_ids(
                client, environment, index["environments"][environment]
            )
        else:
            answer = client.image_index_task_ids(environment)
        if not answer["task_ids"]:
            continue
        _write(args.directory / f"{environment}.task-ids.json", answer["task_ids"])
        excluded = answer["excluded"]
        report["environments"][environment] = {
            "task_ids": len(answer["task_ids"]),
            "excluded": excluded,
        }
        print(
            f"{environment:<16} {len(answer['task_ids']):>8,} task ids"
            + (f"  excluded {excluded}" if excluded else "")
        )
    _write(args.directory / "summary.json", {**report, "index": index})
    return 0


def ensure(args: argparse.Namespace) -> int:
    from collections import Counter

    names = list(args.names)
    if args.from_file:
        names += [
            line.strip()
            for line in args.from_file.read_text().splitlines()
            if line.strip()
        ]
    statuses = _client().ensure_images(sorted(set(names)), timeout_seconds=300)
    if args.json:
        print(json.dumps(statuses, indent=1))
    else:
        print(json.dumps(dict(Counter(row.get("state") for row in statuses.values()))))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="verifiers-ucloud",
        description=__doc__.split("\n\n", 1)[1],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    show = commands.add_parser("summary", help="the gateway's image index")
    show.add_argument("--json", action="store_true", help="print JSON")
    show.set_defaults(func=summary)
    export = commands.add_parser("task-ids", help="each environment's task_ids_file")
    export.add_argument("directory", type=Path)
    export.add_argument(
        "-e",
        "--environment",
        action="append",
        default=[],
        help="only this environment (repeatable)",
    )
    export.add_argument(
        "--ready-only",
        action="store_true",
        help="only tasks whose image is built (no build wait at sandbox creation)",
    )
    export.set_defaults(func=task_ids)
    build = commands.add_parser("ensure", help="build images ahead of the trainer")
    build.add_argument("names", nargs="*", help="image names from the image index")
    build.add_argument("--from", dest="from_file", type=Path, help="a file of names")
    build.add_argument("--json", action="store_true", help="each image's state")
    build.set_defaults(func=ensure)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
