"""Group create and harness delivery against a fake gateway, through the SDK."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from ucloud_sandboxes_sdk._agent_contract import require_agent_sandbox_record
from verifiers.v1.errors import SandboxError

from verifiers_ucloud import UCloudRuntime, UCloudRuntimeConfig

GROUP = "/v1/sandboxes:batch"
RETRY = {"Retry-After": "0.01", "X-UCloud-Sandbox-Retryable": "true"}


def answer(payload: dict, statuses: list[str], **extra: Any) -> dict:
    group_id = payload["group_id"]
    members = []
    for index, status in enumerate(statuses):
        member: dict[str, Any] = {"id": f"{group_id}-{index:04d}", "status": status}
        if status == "running":
            member["sandbox"] = {"spec": {"id": member["id"]}}
            member["generation"] = index + 1
        members.append(member)
    return {
        "group": {"id": group_id, "count": payload["count"], "state": "active"},
        "sandboxes": members,
        **extra,
    }


@dataclass
class Step:
    status: int
    body: Callable[[dict], dict]
    headers: dict[str, str] = field(default_factory=dict)
    gate: threading.Event | None = None


@dataclass
class Gateway:
    url: str
    script: list[Step]
    requests: list[tuple[str, str, Any]] = field(default_factory=list)

    def calls(self, method: str, prefix: str) -> list[tuple[str, str, Any]]:
        return [r for r in self.requests if r[0] == method and r[1].startswith(prefix)]

    def batches(self) -> list[dict]:
        return [body for _, _, body in self.calls("POST", GROUP)]

    def singles(self) -> list[dict]:
        return [b for m, p, b in self.requests if (m, p) == ("POST", "/v1/sandboxes")]

    async def until(self, predicate: Callable[[], bool]) -> None:
        async with asyncio.timeout(5):
            while not predicate():  # noqa: ASYNC110 - the log fills on another thread
                await asyncio.sleep(0.01)


@pytest.fixture
def gateway(monkeypatch) -> Iterator[Gateway]:
    state = Gateway("", [])
    batch_answers = 0

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _reply(self, status: int, body: dict, headers: dict | None = None):
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(raw)

        def _handle(self):
            nonlocal batch_answers
            raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            path = urlsplit(self.path)
            try:
                body = json.loads(raw) if raw else None
            except ValueError:
                body = raw
            state.requests.append((self.command, path.path, body))
            if (self.command, path.path) == ("POST", GROUP):
                step = state.script[min(batch_answers, len(state.script) - 1)]
                batch_answers += 1
                if step.gate is not None:
                    step.gate.wait(5)
                return self._reply(step.status, step.body(body), step.headers)
            if (self.command, path.path) == ("POST", "/v1/sandboxes"):
                return self._reply(201, {"sandbox": {"spec": body}})
            if self.command == "PUT" and path.path.endswith("/files"):
                sandbox_id = path.path.split("/")[3]
                return self._reply(
                    200,
                    {
                        "ok": True,
                        "sandbox_id": sandbox_id,
                        "path": parse_qs(path.query)["path"][0],
                        "size": len(raw),
                    },
                )
            return self._reply(200, {})

        do_GET = do_POST = do_PUT = do_DELETE = _handle

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setenv("UCLOUD_SANDBOX_URL", state.url)
    monkeypatch.delenv("UCLOUD_SANDBOX_API_TOKEN", raising=False)
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)


def runtimes(count: int, **config: Any) -> list[UCloudRuntime]:
    settings = UCloudRuntimeConfig(request_timeout_seconds=5, **config)
    return [UCloudRuntime(settings, name=f"rollout-{i}") for i in range(count)]


async def test_concurrent_identical_creates_share_one_group(gateway) -> None:
    gateway.script = [Step(201, lambda p: answer(p, ["running"] * p["count"]))]
    boxes = runtimes(3)
    await asyncio.gather(*(box.start() for box in boxes))

    (batch,) = gateway.batches()
    assert (batch["count"], batch["placement"]) == (3, "pack")
    assert "id" not in batch["spec"] and not gateway.singles()
    group_id = batch["group_id"]
    assert [box.info.id for box in boxes] == [f"{group_id}-{i:04d}" for i in range(3)]
    # Each member's handle is a managed agent sandbox the relay can bind.
    for index, box in enumerate(boxes):
        assert require_agent_sandbox_record(box.sandbox.record, require_generation=True) == index + 1

    await asyncio.gather(*(box.stop() for box in boxes))
    deleted = sorted(path for _, path, _ in gateway.calls("DELETE", "/v1/sandboxes/"))
    assert deleted == [f"/v1/sandboxes/{group_id}-{i:04d}" for i in range(3)]
    assert not gateway.calls("DELETE", GROUP)


async def test_lone_or_distinct_creates_and_group_create_off_stay_single(
    gateway,
) -> None:
    lone, other = runtimes(1)[0], runtimes(1, cpu=2)[0]
    other.name = "rollout-other"
    await asyncio.gather(lone.start(), other.start())
    off = runtimes(2, group_create=False)
    await asyncio.gather(*(box.start() for box in off))

    assert not gateway.batches()
    assert sorted(spec["id"] for spec in gateway.singles()) == [
        "rollout-0",
        "rollout-0",
        "rollout-1",
        "rollout-other",
    ]
    assert lone.info.id == "rollout-0"


async def test_a_gateway_without_groups_falls_back_to_single_creates(
    gateway,
) -> None:
    unavailable = {"error_code": "sandbox_group_create_unavailable", "retryable": False}
    gateway.script = [Step(501, lambda p: unavailable)]
    first = runtimes(3)
    await asyncio.gather(*(box.start() for box in first))
    assert [box.info.id for box in first] == ["rollout-0", "rollout-1", "rollout-2"]
    # The answer holds for this gateway: later waves do not ask again.
    await asyncio.gather(*(box.start() for box in runtimes(2)))
    assert len(gateway.batches()) == 1
    assert len(gateway.singles()) == 5


async def test_a_retryable_answer_repeats_and_starts_placed_members_early(
    gateway,
) -> None:
    release = threading.Event()
    incomplete = {"error_code": "node_admission_closed", "retryable": True}
    gateway.script = [
        Step(503, lambda p: answer(p, ["running", "pending"], **incomplete), RETRY),
        Step(201, lambda p: answer(p, ["running", "running"]), gate=release),
    ]
    first, second = runtimes(2)
    starts = [asyncio.create_task(box.start()) for box in (first, second)]
    try:
        await asyncio.wait_for(starts[0], 5)
        assert first.info.id is not None and not starts[1].done()
    finally:
        release.set()
    await asyncio.wait_for(starts[1], 5)

    first_try, repeat = gateway.batches()
    assert first_try == repeat
    assert second.info.id == f"{first_try['group_id']}-0001"


async def test_a_failed_group_fails_every_rollout_and_is_deleted(gateway) -> None:
    failed = {"retryable": False}
    gateway.script = [Step(502, lambda p: answer(p, ["failed", "failed"], **failed))]
    boxes = runtimes(2)
    results = await asyncio.gather(
        *(box.start() for box in boxes), return_exceptions=True
    )
    assert all(isinstance(result, SandboxError) for result in results)
    group_id = gateway.batches()[0]["group_id"]
    await gateway.until(lambda: bool(gateway.calls("DELETE", GROUP)))
    assert gateway.calls("DELETE", GROUP)[0][1] == f"{GROUP}/{group_id}"


async def test_cancelled_rollouts_never_keep_a_member(gateway) -> None:
    release = threading.Event()
    gateway.script = [
        Step(201, lambda p: answer(p, ["running"] * p["count"]), gate=release)
    ]
    boxes = runtimes(4)
    starts = [asyncio.create_task(box.start()) for box in boxes]
    await asyncio.sleep(0)
    starts[3].cancel()  # Before the request: left out of it.
    await gateway.until(lambda: bool(gateway.batches()))
    starts[2].cancel()  # After the request: its member is deleted.
    release.set()
    await asyncio.gather(*starts, return_exceptions=True)

    (batch,) = gateway.batches()
    assert batch["count"] == 3
    orphan = f"/v1/sandboxes/{batch['group_id']}-0002"
    await gateway.until(lambda: bool(gateway.calls("DELETE", orphan)))
    assert [box.info.id for box in boxes[:2]] == [
        f"{batch['group_id']}-{i:04d}" for i in range(2)
    ]
    assert boxes[2].info.id is None and boxes[3].info.id is None


async def test_a_harness_file_is_one_upload(gateway) -> None:
    (box,) = runtimes(1)
    await box.start()
    await box.write("skills/tool/run.sh", b"#!/bin/sh\n")
    await box.write("/etc/agent/config.toml", b"x = 1\n")

    uploads = gateway.calls("PUT", f"/v1/sandboxes/{box.info.id}/files")
    assert [body for _, _, body in uploads] == [b"#!/bin/sh\n", b"x = 1\n"]
    assert not gateway.calls("POST", f"/v1/sandboxes/{box.info.id}/exec")
