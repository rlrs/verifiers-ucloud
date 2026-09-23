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

The pinned SDK 0.4.26 includes shared relay admission, startup/restore backpressure handling, and separate
relay connection pools sized for 512 upstream calls, 128 rotating polls, and reply/lease
control. Forwarding and polling limits can be configured for the upstream service.
After updating this checkout, run `uv sync` to install the tested SDK.

For 512-way runs, the runner needs headroom for sandbox streams, tool requests,
relay polls, and upstream connections. On Unix, startup raises the process's
soft file-descriptor limit to 8192 when the existing hard limit permits it.
It never lowers a limit or changes the hard limit. If the host restricts this,
startup logs a warning; configure `LimitNOFILE=8192` for a systemd runner, or
set the shell's `ulimit -n 8192` before launch. Smaller runs can still operate
under a lower limit.

The SDK installs from its versioned GitHub release wheel; no local SDK checkout
is required. The development environment still uses the sibling Verifiers
checkout configured in `tool.uv.sources`.

`[env.interception].max_inflight_requests` defaults to 512 and is shared by all
rollout sessions of this interception instance. Workers queue for capacity
before leasing relay requests; inference, forwarding, and durable response
submission hold that capacity. Reply and lease-renewal HTTP pools remain
separate. The existing per-rollout concurrency of eight is a local share, not
an experiment-wide limit. Separate runner processes require their supervisor
to divide a total budget; this client budget does not coordinate processes.

Optional `[env.interception].resource_phase_hints = true` requires the coordinated
SDK/server resource-phase API. It reports expected tool activity when a rollout
session is acquired and completion only when that session exits normally. It is
off by default during the server/SDK migration. The integration's
`report_resource_phase(rollout_id, phase, ...)` lets real model/training hooks
supply `model_wait`, `tool`, `training_pause`, or `training_resume`, with optional
expected remaining wait seconds for model waits. Ordinary model waits are already
observed by the relay; this API does not duplicate an event for every forwarded
request. Cancellation is not guessed to be a training pause or successful rollout.
Hints expire, are fenced by the active registration and cannot grant execution,
parking, deletion, or trainer restart continuity. A failed hint does not fail the
rollout. Missing hints preserve existing scheduling behavior.
