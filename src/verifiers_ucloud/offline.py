"""Stage a pinned portable Python bundle through the sandbox control plane."""
import hashlib
import json
import shlex
from functools import lru_cache
from pathlib import Path

from verifiers.v1.errors import SandboxError


@lru_cache(maxsize=2)
def read_bundle(path: str):
    archive = Path(path)
    manifest = json.loads(archive.with_name(archive.name + ".json").read_text())
    data = archive.read_bytes()
    if hashlib.sha256(data).hexdigest() != manifest["sha256"]:
        raise SandboxError("Offline Python bundle checksum mismatch")
    if manifest["python"] != "python/bin/python3.12":
        raise SandboxError("Unsupported offline Python layout")
    return data, manifest


async def prepare_script(runtime, script, env=None, *, activate=True):
    bundle = runtime.config.offline_python_bundle
    if bundle is None:
        raise SandboxError("Relay-only script setup requires an offline_python_bundle")
    data = script.encode() if isinstance(script, str) else script
    digest = hashlib.sha256(data).hexdigest()
    archive, manifest = read_bundle(str(bundle))
    if digest not in manifest["scripts"]:
        raise SandboxError("Script is absent from the pinned offline Python bundle manifest")
    prefix = f"/opt/verifiers-offline/{manifest['sha256']}"
    interpreter = f"{prefix}/{manifest['python']}"
    async with runtime._offline_setup_lock:
        if runtime._offline_prefix is None:
            target = f"/tmp/verifiers-offline-{manifest['sha256']}.tar.gz"
            await runtime.write(target, archive)
            command = (
                f"printf '%s  %s\n' {shlex.quote(manifest['sha256'])} {shlex.quote(target)} | sha256sum -c - "
                f"&& mkdir -p {shlex.quote(prefix)} "
                f"&& tar --no-same-owner -xzf {shlex.quote(target)} -C {shlex.quote(prefix)} "
                f"&& {shlex.quote(interpreter)} -I -c 'import openai, mcp, httpx, httpx2, tenacity'"
            )
            result = await runtime.run(["sh", "-c", command], env or {})
            if result.exit_code:
                raise SandboxError("Offline Python setup failed: " + result.stderr.strip()[-2000:])
            runtime._offline_prefix = prefix
        path = f"{runtime.scripts_dir}/{digest}.py"
        if digest not in runtime._uv_interpreters:
            await runtime.write(path, data)
            runtime._uv_interpreters[digest] = interpreter
    if not activate:
        return [interpreter, "-I", path]
    return ["sh", "-c", 'export VIRTUAL_ENV="$1" PATH="$1/bin:$PATH"; shift; exec "$@"',
            "offline-script", f"{prefix}/python", interpreter, "-I", path]


@lru_cache(maxsize=4)
def read_harness_bundle(path: str):
    import io
    import tarfile
    from pathlib import PurePosixPath
    archive = Path(path)
    manifest = json.loads(archive.with_name(archive.name + ".json").read_text())
    blob = archive.read_bytes()
    if hashlib.sha256(blob).hexdigest() != manifest["sha256"]:
        raise SandboxError("Offline harness bundle checksum mismatch")
    roots = ("var/tmp/vf-node", "var/tmp/vf-pi", "var/tmp/vf-prime-agent",
             "var/tmp/vf-opencode", "root/.prime/agent/kernel-venv",
             "root/.local/bin", "root/.local/share/uv/python")
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        for member in tar:
            name = member.name.rstrip("/")
            if PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts:
                raise SandboxError("Unsafe harness archive member")
            if not any(name == root or name.startswith(root + "/") for root in roots):
                raise SandboxError("Unexpected harness archive member: " + name)
            if not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                raise SandboxError("Unsupported harness archive member")
    return blob, manifest


async def prepare_harness_bundle(runtime):
    blob, manifest = read_harness_bundle(str(runtime.config.offline_harness_bundle))
    path = f"/var/tmp/vf-harness-{manifest['sha256']}.tar.gz"
    await runtime.write(path, blob)
    result = await runtime.run(["sh", "-eu", "-c",
        f"printf '%s  %s\\n' {shlex.quote(manifest['sha256'])} {shlex.quote(path)} | sha256sum -c - "
        f"&& tar --no-same-owner -xzf {shlex.quote(path)} -C /"], {})
    if result.exit_code:
        raise SandboxError("Offline harness setup failed: " + result.stderr[-2000:])
