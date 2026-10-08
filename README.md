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

The pinned SDK 0.4.37 includes toolkits, image recipes, archive upload, group create,
shared relay admission, startup/restore backpressure handling, and separate relay
connection pools sized for 512 upstream calls, 128 rotating polls, and reply/lease
control. Forwarding and polling limits can be configured for the upstream service.
After updating this checkout, run `uv sync` to install the tested SDK.

Rollouts that start together and ask for the same sandbox share one gateway group
create (`POST /v1/sandboxes:batch`): the gateway packs the members onto few workers, so
each worker attaches the image once. verifiers starts every rollout's runtime on its own,
so the runtime coalesces creates of one spec that arrive within
`group_window_seconds` (0.05 by default) on one event loop, up to `group_max_size` (32)
per request; `group_placement = "spread"` spreads a group instead of packing it. Each
rollout takes one member, starts as soon as the gateway places that member, and deletes
it on teardown as before. A lone rollout, `group_create = false`, and a gateway that
cannot create groups (ranked placement answers 501) use single creates.
`creates_per_sec` paces create requests, of which a group create is one. Coalescing is
per process: an env-server pool dispatches each request to its least-busy worker, which
can split one example's rollouts across workers.

By default (`managed_agent = true`) each rollout gets a parkable managed-process sandbox
and its main program (`run_program`: the harness) runs as the sandbox's managed primary
process; setup, tools and `open_process` stay execs. The interception binds the
rollout's relay session to that sandbox, so the gateway knows each model call as the
sandbox's wait: it pauses or parks the sandbox through the wait and charges disk by what
the sandbox writes. Needs a verifiers that passes the runtime to
`Interception.acquire`; with an older one the session stays unbound. Set
`managed_agent = false` for a `linux_host` sandbox that runs the program as one exec.

`toolkits = ["vf-harness:latest"]` asks the gateway to stack read-only toolkits (at most
4) on each rollout's image under `/opt/ucloud/toolkits/<name>`. `uv_toolkit` names one
of them whose uv, managed Python and prebuilt script environments verifiers uses to
prepare harness and task uv scripts (`Runtime.uv_env`), instead of installing them per
rollout; task commands keep the image's own tools. The toolkit is built by
`runtime/toolkits/vf-harness/build.sh` in `ucloud-sandboxes`. `uv_toolkit` needs a
verifiers with `Runtime.uv_env`.

A single `write` is one upload request; `write_many` (skill folders, MCP and judge
files) sends all its files in one archive request (`PUT /v1/sandboxes/{id}/archive`).
Either way the gateway creates missing parent directories and writes each file as 0600.
A gateway without the archive endpoint gets one upload per file from the SDK.

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
