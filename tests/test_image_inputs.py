import pytest

from verifiers_ucloud.image_builds import ImageBuildFailure, materialize, validate_copy_inputs


def test_declared_empty_workspace_and_missing_file(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    target = tmp_path / "context"
    target.mkdir()
    materialize({"context_dir": str(source), "directories": ["task_file"],
                 "dockerfile": "FROM python:3.13\nCOPY ./task_file /app/task_file\n"}, target)
    assert (target / "task_file").is_dir()
    assert list((target / "task_file").iterdir()) == []
    with pytest.raises(ImageBuildFailure, match="Missing Docker COPY input"):
        validate_copy_inputs('COPY ["input.csv", "/app/input.csv"]', target)
    validate_copy_inputs("COPY --from=builder /bin/app /bin/app", target)
    with pytest.raises(ValueError, match="Invalid build-context"):
        validate_copy_inputs("COPY ../outside /app", target)


def test_no_implicit_creation_of_missing_assets(tmp_path):
    with pytest.raises(ImageBuildFailure, match="Missing Docker COPY input"):
        materialize({"dockerfile": "COPY task_file /app/task_file"}, tmp_path)
    assert not (tmp_path / "task_file").exists()


def test_missing_build_receipt_rebuilds_same_recipe(tmp_path, monkeypatch):
    import hashlib
    import json
    import sqlite3
    from ucloud_sandboxes_sdk import SandboxApiError, SandboxClient
    from verifiers_ucloud import image_builds
    encoded = json.dumps({"dockerfile": "FROM python:3.11-slim\n"})
    key = hashlib.sha256(encoded.encode()).hexdigest()
    db = tmp_path / "images.sqlite"
    with sqlite3.connect(db) as con:
        con.execute("CREATE TABLE images (source TEXT, recipe TEXT, prepared_image TEXT)")
        con.execute("INSERT INTO images VALUES (?, ?, ?)", ("source", encoded, None))
    folder = tmp_path / "cache" / key
    folder.mkdir(parents=True)
    old = {"build_id": "gone", "status": "succeeded", "image": "old"}
    (folder / "receipt.json").write_text(json.dumps(old))
    submitted = []
    def get_build(self, identifier, **kwargs):
        if identifier == "replacement":
            return {"status": "succeeded"}
        assert identifier == "gone"
        raise SandboxApiError("image build not found", status_code=404)
    def submit(self, image, **kwargs):
        submitted.append(image)
        return {"build_id": "replacement", "status": "pending"}
    # Use the real SDK client: mock transport-facing methods, not its lifecycle.
    monkeypatch.setattr(SandboxClient, "get_image_build", get_build)
    monkeypatch.setattr(SandboxClient, "submit_image_build", submit)
    monkeypatch.setenv("UCLOUD_SANDBOX_URL", "http://localhost")
    image_builds.prepare_image("source", db, tmp_path / "cache")
    assert len(submitted) == 1
    assert (folder / "context/Dockerfile").read_text() == "FROM python:3.11-slim\n"
    assert json.loads((folder / "receipt.json").read_text())["build_id"] == "replacement"
    assert json.loads(next(folder.glob("receipt-missing-*.json")).read_text()) == old


@pytest.mark.parametrize("status,retry", [(None, True), (429, True), (502, True), (503, True), (504, True), (401, False), (404, False)])
def test_build_status_read_retries_only_safe_transient_errors(monkeypatch, status, retry):
    from ucloud_sandboxes_sdk import SandboxApiError, SandboxClient
    from verifiers_ucloud import image_builds
    calls = []
    sleeps = []
    def get(identifier, timeout_seconds):
        calls.append((identifier, timeout_seconds))
        if len(calls) < 3:
            error = SandboxApiError("read failed", status_code=status)
            if status is None:
                error.__cause__ = TimeoutError("read timed out")
            raise error
        return {"status": "succeeded"}
    monkeypatch.setattr(image_builds.time, "monotonic", lambda: 100)
    monkeypatch.setattr(image_builds.time, "sleep", sleeps.append)
    client = SandboxClient("http://localhost", timeout_seconds=120)
    monkeypatch.setattr(client, "get_image_build", get)
    if retry:
        assert image_builds._wait_for_image_build(client, "same-build", 130)["status"] == "succeeded"
        assert calls == [("same-build", 30)] * 3
        assert len(sleeps) == 2 and all(0 < delay <= 5 for delay in sleeps)
    else:
        with pytest.raises(SandboxApiError):
            image_builds._wait_for_image_build(client, "same-build", 130)
        assert len(calls) == 1 and sleeps == []


def test_build_read_retry_respects_deadline_and_exhaustion(monkeypatch):
    from ucloud_sandboxes_sdk import SandboxApiError, SandboxClient
    from verifiers_ucloud import image_builds
    clock = [0.0]
    calls = []
    def get(identifier, timeout_seconds):
        calls.append(timeout_seconds)
        clock[0] += timeout_seconds
        raise SandboxApiError("service unavailable", status_code=503)
    monkeypatch.setattr(image_builds.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(image_builds.time, "sleep", lambda t: clock.__setitem__(0, clock[0] + t))
    client = SandboxClient("http://localhost", timeout_seconds=120)
    monkeypatch.setattr(client, "get_image_build", get)
    with pytest.raises(TimeoutError):
        image_builds._wait_for_image_build(client, "same-build", 10)
    assert clock == [10] and calls == [10]
    clock[0] = 0
    calls.clear()
    with pytest.raises(TimeoutError):
        image_builds._wait_for_image_build(client, "same-build", 1000)
    assert len(calls) > 3
    assert all(0 < value <= 120 for value in calls)
    assert clock[0] == 1000


def test_failed_build_poll_is_shared_but_not_a_bad_image(tmp_path, monkeypatch):
    import asyncio
    from verifiers_ucloud import image_builds
    calls = []
    def fail(*args):
        calls.append(args)
        raise image_builds._polling_error("image", "build", TimeoutError("read timeout"))
    monkeypatch.setattr(image_builds, "prepare_image", fail)
    async def run():
        return await asyncio.gather(*(image_builds.prepare_image_async("image", tmp_path, tmp_path) for _ in range(8)), return_exceptions=True)
    errors = asyncio.run(run())
    assert len(calls) == 1
    assert all(e is errors[0] for e in errors)
    assert isinstance(errors[0], image_builds.ImageBuildPollingError)
    assert not isinstance(errors[0], ImageBuildFailure)
    assert "[incident=" in str(errors[0])
