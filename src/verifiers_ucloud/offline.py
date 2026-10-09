"""Stage pinned offline bundles into relay-only sandboxes.

A relay-only sandbox cannot download Python, uv or harness dependencies. A
portable Python bundle (its manifest lists the script digests it was built
for) runs harness uv scripts, and a harness bundle carries agent assets such
as Node, OpenCode and Pi into fixed paths. Both are checked against their
manifests' SHA-256 before use; a bundle's manifest is `<archive>.json`.
"""

from __future__ import annotations

import hashlib
import io
import json
import shlex
import tarfile
from functools import lru_cache
from pathlib import Path, PurePosixPath

from verifiers.v1.errors import SandboxError

PYTHON_ROOT = "/opt/verifiers-offline"
HARNESS_ROOTS = (
    "var/tmp/vf-node",
    "var/tmp/vf-pi",
    "var/tmp/vf-prime-agent",
    "var/tmp/vf-opencode",
    "root/.prime/agent/kernel-venv",
    "root/.local/bin",
    "root/.local/share/uv/python",
)


def _manifest(archive: Path) -> dict:
    return json.loads(archive.with_name(archive.name + ".json").read_text())


@lru_cache(maxsize=2)
def read_bundle(path: str) -> tuple[bytes, dict]:
    archive = Path(path)
    manifest = _manifest(archive)
    data = archive.read_bytes()
    if hashlib.sha256(data).hexdigest() != manifest["sha256"]:
        raise SandboxError("Offline Python bundle checksum mismatch")
    if manifest["python"] != "python/bin/python3.12":
        raise SandboxError("Unsupported offline Python layout")
    return data, manifest


async def prepare_script(runtime, script, env=None, *, activate=True) -> list[str]:
    """Run a uv script with the bundle's interpreter instead of preparing it."""
    bundle = runtime.config.offline_python_bundle
    if bundle is None:
        raise SandboxError("Relay-only script setup requires an offline_python_bundle")
    data = script.encode() if isinstance(script, str) else script
    digest = hashlib.sha256(data).hexdigest()
    archive, manifest = read_bundle(str(bundle))
    if digest not in manifest["scripts"]:
        raise SandboxError("Script is absent from the offline Python bundle manifest")
    prefix = f"{PYTHON_ROOT}/{manifest['sha256']}"
    interpreter = f"{prefix}/{manifest['python']}"
    async with runtime._offline_setup_lock:
        if runtime._offline_prefix is None:
            target = f"/tmp/verifiers-offline-{manifest['sha256']}.tar.gz"
            await runtime.write(target, archive)
            sha, target_q, prefix_q = map(
                shlex.quote, (manifest["sha256"], target, prefix)
            )
            command = (
                f"printf '%s  %s\\n' {sha} {target_q} | sha256sum -c - "
                f"&& mkdir -p {prefix_q} "
                f"&& tar --no-same-owner -xzf {target_q} -C {prefix_q} "
                f"&& {shlex.quote(interpreter)} -I -c "
                "'import openai, mcp, httpx, httpx2, tenacity'"
            )
            result = await runtime.run(["sh", "-c", command], env or {})
            if result.exit_code:
                raise SandboxError(
                    "Offline Python setup failed: " + result.stderr.strip()[-2000:]
                )
            runtime._offline_prefix = prefix
        path = f"{runtime.scripts_dir}/{digest}.py"
        if digest not in runtime._uv_interpreters:
            await runtime.write(path, data)
            runtime._uv_interpreters[digest] = interpreter
    if not activate:
        return [interpreter, "-I", path]
    return [
        "sh",
        "-c",
        'export VIRTUAL_ENV="$1" PATH="$1/bin:$PATH"; shift; exec "$@"',
        "offline-script",
        f"{prefix}/python",
        interpreter,
        "-I",
        path,
    ]


@lru_cache(maxsize=4)
def read_harness_bundle(path: str) -> tuple[bytes, dict]:
    archive = Path(path)
    manifest = _manifest(archive)
    blob = archive.read_bytes()
    if hashlib.sha256(blob).hexdigest() != manifest["sha256"]:
        raise SandboxError("Offline harness bundle checksum mismatch")
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        for member in tar:
            name = member.name.rstrip("/")
            parts = PurePosixPath(name)
            if parts.is_absolute() or ".." in parts.parts:
                raise SandboxError("Unsafe harness archive member")
            if not any(name == r or name.startswith(r + "/") for r in HARNESS_ROOTS):
                raise SandboxError("Unexpected harness archive member: " + name)
            kinds = (member.isfile(), member.isdir(), member.issym(), member.islnk())
            if not any(kinds):
                raise SandboxError("Unsupported harness archive member")
    return blob, manifest


async def prepare_harness_bundle(runtime) -> None:
    """Unpack the harness bundle into the sandbox's root after its checksum."""
    blob, manifest = read_harness_bundle(str(runtime.config.offline_harness_bundle))
    path = f"/var/tmp/vf-harness-{manifest['sha256']}.tar.gz"
    await runtime.write(path, blob)
    sha, path_q = shlex.quote(manifest["sha256"]), shlex.quote(path)
    result = await runtime.run(
        [
            "sh",
            "-eu",
            "-c",
            f"printf '%s  %s\\n' {sha} {path_q} | sha256sum -c - "
            f"&& tar --no-same-owner -xzf {path_q} -C /",
        ],
        {},
    )
    if result.exit_code:
        raise SandboxError("Offline harness setup failed: " + result.stderr[-2000:])
