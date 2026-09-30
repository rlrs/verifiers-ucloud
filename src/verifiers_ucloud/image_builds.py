"""Prepare recipe-backed task images on first use; cache builds across rollouts."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import weakref

import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import re
import shlex
import subprocess
import tempfile
import time
import uuid

from ucloud_sandboxes_sdk import Image, SandboxApiError, SandboxClient
from verifiers.v1.errors import SandboxError

class ImageBuildFailure(SandboxError):
    """A confirmed failed recipe build; exclude the affected task image."""


class ImageBuildPollingError(SandboxError):
    """A shared build-status observation failed; not a confirmed bad task image."""


def _wait_for_image_build(client, build_id: str, deadline: float):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(f"Image build status deadline exceeded for {build_id}")
    # SDK >=0.4.28 retries only status reads, inside this remaining deadline.
    return client.wait_for_image_build(build_id, timeout_seconds=remaining, poll_interval_seconds=5)


def _polling_error(source: str, build_id: str, error: Exception) -> ImageBuildPollingError:
    # One exception is shared by every rollout waiting on this preparation future.
    return ImageBuildPollingError(
        f"Image build status unavailable [incident={uuid.uuid4().hex}] build={build_id} source={source}: {error}"
    )


logger = logging.getLogger(__name__)
_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="image-build")
_PENDING = weakref.WeakKeyDictionary()


async def prepare_image_async(source: str, database: Path, cache: Path) -> Image:
    # Keep cold builds off the shared asyncio executor used by live rollouts.
    loop = asyncio.get_running_loop()
    pending = _PENDING.setdefault(loop, {})
    key = (source, str(database), str(cache))
    future = pending.get(key)
    if future is None:
        future = loop.run_in_executor(_EXECUTOR, prepare_image, source, database, cache)
        pending[key] = future
        def finished(done):
            pending.pop(key, None)
            if not done.cancelled():
                done.exception()  # Observe errors even if every waiting rollout was cancelled.
        future.add_done_callback(finished)
    return await asyncio.shield(future)



def _relative(path: str) -> Path:
    value = PurePosixPath(path)
    if value.is_absolute() or '..' in value.parts or not value.parts:
        raise ValueError(f'Invalid build-context destination: {path!r}')
    return Path(*value.parts)


def validate_copy_inputs(dockerfile: str, context: Path) -> None:
    """Reject missing literal COPY inputs before scheduling a remote build."""
    for line in dockerfile.replace("\\\n", " ").splitlines():
        match = re.match(r"^\s*COPY\s+(.+)$", line, re.I)
        if not match:
            continue
        statement = match.group(1)
        if re.search(r"(?:^|\s)--from(?:=|\s)", statement):
            continue
        statement = re.sub(r"^(?:--[\w-]+(?:=[^\s]+)?\s+)+", "", statement)
        words = json.loads(statement) if statement.startswith("[") else shlex.split(statement)
        if len(words) < 2:
            raise ValueError(f"Invalid COPY instruction: {line}")
        for name in words[:-1]:
            # Docker resolves build arguments; check the literal inputs here.
            if "$" in name:
                continue
            normalized = name.removeprefix("./").rstrip("/")
            if normalized in ("", "."):
                continue
            relative = _relative(normalized)
            if not any(context.glob(str(relative))):
                raise ImageBuildFailure(f"Missing Docker COPY input {name!r} in build context {context}")


def materialize(recipe: dict, destination: Path) -> None:
    source = recipe.get('context_dir')
    if source:
        source = Path(source).resolve(strict=True)
        for item in source.rglob('*'):
            target = destination / item.relative_to(source)
            if not item.resolve().is_relative_to(source):
                raise ValueError(f'Build input escapes its source directory: {item}')
            if item.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not item.is_file():
                raise ValueError(f'Unsupported build input: {item}')
            target.parent.mkdir(parents=True, exist_ok=True)
            with item.open('rb') as handle:
                prefix = handle.read(100)
            if prefix.startswith(b'version https://git-lfs.github.com/spec/v1'):
                from huggingface_hub import hf_hub_download
                relative = item.relative_to(Path(recipe['hf_root'])).as_posix()
                resolved = hf_hub_download(recipe['hf_repo'], relative, repo_type='dataset', revision=recipe['hf_revision'])
                shutil.copyfile(resolved, target)
            else:
                shutil.copyfile(item, target)
    for name in recipe.get('directories', []):
        (destination / _relative(name)).mkdir(parents=True, exist_ok=True)
    repository = recipe.get('source_repository')
    if repository:
        repo, commit = repository['repo'], repository['commit']
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo) or not re.fullmatch(r'[a-f0-9]{40}', commit):
            raise ValueError('Invalid pinned source repository')
        target = destination / 'repo'
        target.mkdir()
        for command in [
            ['git', 'init', str(target)],
            ['git', '-C', str(target), 'remote', 'add', 'origin', f'https://github.com/{repo}.git'],
            ['git', '-C', str(target), 'fetch', '--depth=1', 'origin', commit],
            ['git', '-C', str(target), 'checkout', '--detach', 'FETCH_HEAD'],
        ]:
            subprocess.run(command, check=True, capture_output=True, timeout=300)
    for name, content in recipe.get('files', {}).items():
        target = destination / _relative(name)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    (destination / 'Dockerfile').write_text(recipe['dockerfile'])
    validate_copy_inputs(recipe['dockerfile'], destination)


def prepare_image(source: str, database: Path, cache: Path, timeout: float = 7200) -> Image:
    with sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True) as connection:
        row = connection.execute('SELECT recipe, prepared_image FROM images WHERE source = ?', (source,)).fetchone()
    if row is None:
        raise ValueError(f'No preparation recipe for task image {source!r}')
    encoded, prepared = row
    if prepared:
        return Image.from_name(prepared)
    recipe = json.loads(encoded)
    key = hashlib.sha256(encoded.encode()).hexdigest()
    folder = cache / key
    folder.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    fd = os.open(folder / 'lock', os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, 'r+') as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f'Image preparation still in progress for {source}')
                time.sleep(1)
        receipt_path = folder / 'receipt.json'
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        # Receipts outlive backend build records. Re-submit the same immutable
        # recipe only when the backend explicitly confirms its build is gone.
        if receipt and receipt.get('build_id'):
            probe = SandboxClient.from_env(timeout_seconds=120)
            try:
                build = _wait_for_image_build(probe, receipt['build_id'], deadline)
                receipt.update(status=build.get('status'), build=build, error=build.get('error', ''))
                receipt_path.write_text(json.dumps(receipt, indent=2))
            except SandboxApiError as error:
                if error.status_code != 404:
                    raise _polling_error(source, receipt['build_id'], error) from error
                archive = folder / f"receipt-missing-{time.time_ns()}.json"
                archive.write_text(json.dumps(receipt, indent=2))
                logger.warning('Rebuilding missing backend build %s for %s', receipt['build_id'], source)
                receipt = None
            except OSError as error:
                raise _polling_error(source, receipt['build_id'], error) from error
        if receipt and receipt.get('status') == 'succeeded':
            return Image.from_name(receipt['image'])
        if receipt and receipt.get('status') == 'failed':
            raise ImageBuildFailure(f"Cached image build failure for {source}: {str(receipt.get('error', ''))[-1000:]}")
        context = folder / 'context'
        if not context.exists():
            with tempfile.TemporaryDirectory(prefix='context-', dir=folder) as temporary:
                staging = Path(temporary) / 'ready'
                staging.mkdir()
                materialize(recipe, staging)
                staging.rename(context)
        client = SandboxClient.from_env(timeout_seconds=120)
        name = 'agentic-pool-' + key[:24]
        if receipt is None:
            # SDK retries only explicit admission refusals and reuses the upload.
            build = client.submit_image_build(
                Image.from_dockerfile(name=name, context_path=context),
                timeout_seconds=min(600, max(1, deadline - time.monotonic())),
            )
            receipt = {'source': source, 'image': name, 'recipe_sha256': key, 'build_id': build['build_id'], 'status': build.get('status', 'pending')}
            receipt_path.write_text(json.dumps(receipt, indent=2))
            logger.info('Preparing task image %s: build %s', source, build['build_id'])
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f'Image preparation deadline exceeded for {source}')
        try:
            build = _wait_for_image_build(client, receipt['build_id'], deadline)
        except (SandboxApiError, OSError) as error:
            raise _polling_error(source, receipt['build_id'], error) from error
        receipt.update(status=build.get('status'), build=build, error=build.get('error', ''))
        receipt_path.write_text(json.dumps(receipt, indent=2))
        if build.get('status') == 'failed':
            raise ImageBuildFailure(f"Image preparation failed for {source}: {str(build.get('error', ''))[-1000:]}")
        if build.get('status') != 'succeeded':
            raise RuntimeError(f"Image preparation did not finish for {source} (status={build.get('status')}): {str(build.get('error', ''))[-1000:]}")
        return Image.from_name(name)
