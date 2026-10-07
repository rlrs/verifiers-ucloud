# verifiers-ucloud

UCloud sandbox runtime and relay-backed interception for verifiers v1.

The package exports both integrations from `verifiers_ucloud`. Once installed,
verifiers discovers them from the normal config types:

```toml
[env.agent.runtime]
type = "ucloud"
image = "python:3.11-slim"
cpu = 2
memory = 4

[env.interception]
type = "ucloud"
```

`image` is only the runtime default; each environment can select its own image.

Set `UCLOUD_SANDBOX_URL` and, when required,
`UCLOUD_SANDBOX_API_TOKEN` for the sandbox gateway. Set `UCLOUD_RELAY_URL` and,
when required, `UCLOUD_RELAY_WORKER_TOKEN` for relay interception.

For local development beside the `verifiers` repository:

```console
uv sync
uv run pytest
```

The interception slot exposes its base URL without a trailing slash. The SDK's
HTTP tunnel URL includes one, while verifiers appends `/v1` and state/tool paths.
Normalize this join boundary so requests do not contain `//v1`, which the relay
rejects as a non-absolute endpoint. The interception lifecycle test uses the SDK's
trailing-slash convention.

The pinned SDK 0.4.36 includes archive upload, group create, shared relay admission,
startup/restore backpressure handling, and separate relay connection pools sized for 512
upstream calls, 128 rotating polls, and reply/lease control. Forwarding and polling
limits can be configured for the upstream service. After updating this checkout, run
`uv sync` to install the tested SDK.

Rollouts that start together and ask for the same sandbox share one gateway group
create (`POST /v1/sandboxes:batch`): the gateway packs the members onto few workers, so
each worker attaches the image once. verifiers starts every rollout's runtime on its own,
so the runtime coalesces creates of one spec that arrive within
`group_window_seconds` (0.05 by default) on one event loop, up to `group_max_size` (32)
per request; `group_placement = "spread"` spreads a group instead of packing it. Each
rollout takes one member, starts as soon as the gateway places that member, and deletes
it on teardown as before. A parkable managed sandbox created in a group passes the same
checks as a single create: the member's record must show a parkable managed process and
a positive generation. A lone rollout, `group_create = false`, and a gateway that
cannot create groups (ranked placement answers 501) use single creates.
`creates_per_sec` paces create requests, of which a group create is one. Coalescing is
per process: an env-server pool dispatches each request to its least-busy worker, which
can split one example's rollouts across workers.

A single `write` is one upload request; `write_many` (skill folders, MCP and judge
files) sends all its files in one archive request (`PUT /v1/sandboxes/{id}/archive`).
Either way the gateway creates missing parent directories and writes each file as 0600.
A gateway without the archive endpoint gets one upload per file from the SDK.

This isolated checkout also restores standard localhost entries when the sandbox
cannot resolve localhost. The current gateway supplied empty /etc/hosts files,
breaking PostgreSQL, local HTTP tests, and pytest-rerunfailures' loopback socket.
Runtime setup preserves existing hosts entries. A setup failure deletes the newly
created sandbox before closing the client.

Long reasoning evaluations require SDK 0.4.17 and use a 7200-second forwarding
budget, separate from the 30-second control-call timeout. The relay buffers
responses, so the forwarding budget must cover queueing and complete generation.
Token and rollout budgets remain independent.

For the bash harness, sandbox exec and relay worker completion are supervised
together. Worker errors become TunnelError on the affected rollout and cancel
and drain only that sandbox operation. They do not cancel sibling rollouts.
External cancellation propagates normally. Slot teardown cancels and drains its
worker. This supervision covers runtime.run (the bash harness path); live-process
harnesses using open_process require equivalent supervision before use.

Bounded reads delegate to the native runtime base implementation, which enforces
the byte cap inside the sandbox before transferring data. Unbounded reads use the
SDK download endpoint.

SDK 0.4.18 with gateway 0.5.33+ supports host-enforced relay-only policy.
Framework-only tasksets (`allow=[]`) select `SandboxNetworkPolicy.relay_only("default")`
at sandbox creation. Guest DNS, direct IP egress, IPv6 and inbound SSH remain blocked.
Custom destination allow/block lists are unsupported. The policy is immutable;
setup dependencies must be baked into the task image or staged through the SDK.

Set `guest_relay_url` in both the runtime and interception config to the deployment's
private relay origin. For this deployment it is
`http://gateway-live-ucloud-20260824a:8092`. Keep `UCLOUD_RELAY_URL` (or interception
`relay_url`) at `https://app-sandboxes-relay-v2.cloud.sdu.dk` for external workers.
These are distinct endpoints; the SDK does not rewrite them automatically.
The adapter preserves the SDK's registration-token tunnel path, including `/v1`
and task-state/tool routes, while replacing only its guest-facing origin.
Standalone OpenAI-only clients may use `model_relay_env(guest_relay_url, rollout_id)`;
verifiers needs the general HTTP tunnel for task-state requests too.

Relay-only bash harness setup uses `offline_python_bundle`: a portable Python
archive with a sidecar `<archive>.json` recording its SHA256, interpreter path,
allowed script SHA256 values and pinned package versions. The adapter checks the
archive and requested script, stages it through the SDK, and executes locally.
Missing bundles or unknown scripts fail explicitly; setup never opens Internet access.

For gateway-built images, set runtime `image_reference_type = "name"` and use
its managed image ID in the task image field. The default `"registry"` treats
that field as an OCI registry reference. The SDK distinguishes these through a
request header; a managed image ID must not be sent as a public registry name.

Sandbox bootstrap preserves existing `/etc/hosts` entries and ensures both
localhost and the sandbox's own kernel hostname resolve locally. Java uses the
latter in test-report metadata; missing hostname resolution can produce verifier
failures even when all test dependencies are present. This adds no network egress.

Offline harness Python runs with `-I` for both the dependency probe and scripts.
Task checkout modules (for example `/testbed/httpx`) must not shadow the
pinned harness dependencies. This flag applies to harness Python only.

Control-plane file downloads and identical-byte uploads retry explicit HTTP 503
CPU-admission rejection up to five attempts (15 seconds total backoff). Other
errors propagate. Shell commands are not replayed: retrying an ambiguous exec
can apply an operation twice. This does not increase sandbox node CPU capacity.

`image_manifest` maps native task image references to prepared UCloud names.
The manifest must have version1 and each admitted entry must contain `image`
and `validated: true`. Missing or unvalidated images fail closed. Validate with
fresh baseline/oracle sandboxes before publishing this manifest; build success
alone does not qualify a task. Omitting the manifest retains normal resolution.

Recipe-backed pools may set `image_recipe_db` and `image_build_cache` instead of
`image_manifest`. The read-only SQLite `images` table contains `source`, `recipe`
(JSON), and optional `prepared_image` columns. Missing images are built on first
use and deduplicated by a fixed per-recipe lock. Successful build receipts persist;
confirmed failed builds are task-local `ImageBuildFailure` exclusions. This mode does not require an
oracle-validation allowlist. Reserve managed builders and allow sufficient setup
time for cold builds. Do not mutate an index while jobs use it.

Image recipes may explicitly declare empty relative `directories`. Materialization
creates those directories, validates literal Docker COPY sources locally and fails
before requesting a builder if an input is absent. No directory or data file is
implicitly invented to satisfy COPY. Stage-to-stage COPY is validated by Docker.
Change the recipe/index to retry repaired builds; preserve old cached failure receipts.

For inference-wait parking, set `env.agent.runtime.parkable = true`. The adapter
creates a `container` profile sandbox with `parkable=True, managed_process=True`,
root execution, writable rootfs and the benchmark profile's capability/filesystem
settings. SDK 0.4.18 does not support managed agents with `profile=linux_host`.
Linux-host entrypoint service startup is therefore not provided; task images must
supply their dependencies and start required services in setup.

The agent's `run_program` uses `start_agent()` exactly once and reads its persistent
job ledger. Setup/file commands continue using short exec operations. Attached
`open_process` is rejected for parkable runtimes: its transport cannot survive
checkpoint/restore. A gateway-confirmed sandbox handle (excluded from trace
serialization) binds `rollout_session(..., sandbox=handle)` to the correct ID and
generation. Missing managed/parkable flags or generation fail provisioning.

Use `scripts/lumi_probe_parkable_agent.py` in the owning PRIME-RL checkout to
verify two actual park/wake cycles with memory and file preservation, and
`scripts/lumi_probe_parkable_harness.py` to validate the real bash harness,
interception and tool-state exchange. Both delete only their own randomly named
probe. Successful creation alone is not proof that model waits actually park.

Offline Python extraction uses `tar --no-same-owner`: the pinned archive contains
owner-only executable/library files with build-host numeric owners. Preserving
those owners makes the managed workload fail with `exec workload: permission denied`,
even when a privileged setup exec successfully imports the same interpreter.
Preserve file modes; assign ownership to the sandbox extraction user.

The offline bundle must also target the oldest supported task-image glibc. The
r10 bundle retains cryptography 50.0.1 but uses its `manylinux_2_28` wheel instead
of the host-selected `manylinux_2_34` build, which fails in Debian 11 SWE images.
`lumi_repack_offline_bundle.py` writes a new archive and provenance/checksum
manifest; never replace a bundle used by an existing run in place.


## Relay and image recovery

Relay response commits can wait for a parked sandbox to wake. The UCloud adapter
uses a 120-second response-commit timeout and retries transport failures up to
three times with the same request identity and response bytes. It never retries
generation or arbitrary control mutations. SDK retryable-503 commit handling
remains active. Unrelated control requests retain their shorter timeout.
Read-only managed-job polling tolerates a missing route for at most 120 seconds
and polls every five seconds; it never restarts the managed process. Permanent
route loss still fails the rollout. These are bounded client recoveries, not a
claim that backend route loss is repaired.
Image-cache receipts are checked against the backend build record. An explicit
404 archives the stale receipt and re-submits the identical pinned recipe under
the existing cross-process lock. Other errors do not invalidate the receipt.
Before relaunch, run adapter tests and concurrent real-harness park/resume probes.

The synchronous SDK `SandboxClient` is a request-scoped urllib client, not a
context manager. Use it directly; tests must retain its real lifecycle contract.
A lease-renewal 410 with exactly "request is already completed" is an expected
completion race: let the response forward/commit operation determine success.
Do not suppress other 410s or treat a response-commit 410 as success.

Startup admission requires SDK 0.4.20 paired with server 0.5.35 or newer. Pin
both the adapter dependency and `scripts/lumi_prepare_runtime.sh` so the generated
runtime uses the same SDK. The SDK retries explicit pre-dispatch busy responses
within the caller deadline, including uploads; ambiguous timeout responses are
not permission to replay work. Validate concurrent cold setup and park/resume
before allocating a broad GPU pilot. Pass absolute script/test paths to
`lumi_run_in_container.sh`, whose container working directory is `/workdir`.

## Runner concurrency

Based on verifiers-ucloud main `964708a`, with the local parkable runtime,
guest relay origin, image preparation, and bounded recovery integrations retained.
SDK 0.4.22 provides separate forward, poll, and control connection pools.
Runtime and interception startup raise the soft descriptor limit to 8192 within
the existing hard limit; see `_resources.py`.

SDK 0.4.23 / upstream main `61a313a` handles cold builder admission with a
600-second submission budget and one context upload. The local image adapter
uses this native retry policy; ambiguous build failures are not resubmitted.
Requires server 0.5.46 or newer for structured builder admission.

Explicit backend `node_lost` is terminal for managed job polling and relay
response delivery, even if the HTTP status would otherwise be retryable.
The failed rollout is returned to the normal scheduler; its sandbox is never
recreated within that rollout. Unmarked transient route404s retain bounded
recovery. The pilot observer counts node losses separately but includes them
in sustained-loss protection.

Parkable ACP harnesses use managed jobs with a sandbox-local input mailbox.
Output polling skips parked/transitioning sandboxes and treats log EOF as an
available-data boundary until the job is terminal. Configure `offline_harness_bundle`
with a checksum-sidecar asset archive for pi, Prime Agent or OpenCode, alongside
`offline_python_bundle` containing the pinned ACP client and runner script digest.
Large harness archives are staged on /var/tmp to avoid the small /tmp tmpfs.

The private `guest_relay_url` applies only to a resolved network-restricted runtime.
An unrestricted sandbox uses the SDK public relay URL, since it lacks the named
relay-only policy hostname mapping. Task network policy is resolved before this
selection. Both paths retain the same registered rollout capability and parking.

### Host MCP tools

UCloud interception supplies the environment-scoped host tunnel used by shared
and per-task MCP servers. It registers a managed HTTP relay session, forwards
to the local MCP port, and tears down the registration and worker with the
serving scope. Worker failures terminate that scope instead of leaving a dead
URL. No Prime infrastructure or credentials are used.

Interception slots retain the public relay origin so host-side shared tools can
reach the signed state channel. The UCloud runtime translates framework URLs
for restricted guests to `guest_relay_url`; unrestricted guests and host tools
keep the public URL. Paths, queries, and capabilities remain unchanged.

Managed job log reads retry a structured HTTP 503
`managed_process_read_unavailable` response only when `retryable=true`, with
five total attempts and 1/2/4/8-second backoff. Every attempt uses the same job,
stream and byte offset; only a successful chunk advances the caller's cursor.
This applies to interactive harness output and completed-agent output. It never
restarts the agent or replays shell commands. Cancellation, explicit `node_lost`,
nonretryable errors and exhausted retries propagate to the rollout.
A missing initial working directory is also terminal even if the backend marks
that read retryable: an agent can delete its repository, and polling cannot
restore it. Managed control commands should use a stable backend working
directory independently of the agent's task directory.

Confirmed failed image-build receipts and terminal failed build responses raise
`ImageBuildFailure`, preserved through runtime setup and verifiers error
serialization. The training scheduler can therefore exclude that task image
without classifying it as a backend outage. Build API failures, timeouts and
nonterminal/unexpected statuses keep their infrastructure-error classification.


Image-build status reads require SDK **0.4.28 or newer**. Both new builds and
cached receipts use the SDK's deadline-bounded retrying status waiter. Retries
poll the same build ID; ambiguous submission failures never trigger a duplicate
submission. Only an explicit 404 for an old receipt allows resubmitting its
immutable recipe. Exhausted polling carries `ImageBuildPollingError` with a
per-preparation incident ID shared by its waiting rollouts. It must remain an
infrastructure failure, not a task exclusion. The original overall preparation
deadline also includes lock wait and submission time.

Missing literal COPY inputs raise `ImageBuildFailure` before contacting a builder,
so the training task quarantine handles them. Unsafe paths and unclassified
materialization failures continue to fail explicitly. Never fabricate missing
inputs. Fix the recipe and publish a separate index version to retry the task.

The LUMI runtime pins SDK 0.4.34. Its request clients retry identified transient
connection-establishment failures within the original deadline, for at most five
attempts with bounded backoff. Do not add a blanket adapter retry around exec,
sandbox creation, or other mutating operations: certificate errors, cancellation,
and ambiguous post-dispatch failures must not be replayed. Keep the runtime wheel
pin and this package's dependency lock aligned, and verify the loaded version in
the actual job container before submission.

SDK 0.4.34 is pinned in `pyproject.toml` and `uv.lock`. Its synchronous client
reuses bounded HTTP connections and its TLS context. Use the same SDK pin in
the LUMI runtime metadata layer. Hetzner deployments use the same gateway and
relay environment variables; keep image-build receipt caches separate between
gateways and verify the guest relay route with a managed-agent parking probe.

For the Hetzner gateway at `https://77.42.92.27`, external workers use
`UCLOUD_RELAY_URL=https://77.42.92.27/relay`; restricted guests use
`guest_relay_url=http://10.42.0.2:8092`. Guest URL translation replaces the
public relay base, including its `/relay` mount prefix, while retaining the
registration capability path and query. URLs outside that prefix are unchanged.

On the LUMI bench, validate deployment changes with
`scripts/lumi_probe_parkable_harness.py --guest-relay-url http://10.42.0.2:8092
--image <registered-python-image> --image-type name --output <probe.json>`
inside the operational runtime. This checks two model replies and preserved tool state. Warm retention during
model waits is normal and does not fail this probe. Use `--require-park` only
when explicitly testing cold parking; a passing warm-retention probe does not
establish that a cold park/wake cycle occurred.

SDK 0.4.32 retries explicit pre-dispatch wake/migration capacity waits until
the operation deadline, and idempotent deletion during memory publication
draining. Those server codes require gateway 0.5.114rc58 or newer. This is
separate from server-side image-context persistence and routing correctness.

Managed-process stdout/stderr polling uses SDK 0.4.33's
`get_sandbox_status(id)` to request a compact, exact-ID status record instead
of scanning full sandbox specifications. The gateway must support `view=status`;
unsupported responses fail explicitly. Multi-sandbox status monitors can use
`list_sandbox_statuses(sandbox_ids=...)` in chunks of at most 256 IDs. Keep full
records for ownership-checked deletion; compact status is not ownership proof.

SDK 0.4.34 prepares asynchronous image-build contexts in a bounded background
pool, avoiding event-loop stalls during archive creation. Existing asynchronous
image submission calls use this automatically; no adapter concurrency override
is needed. Preparation time counts against the submission deadline.
