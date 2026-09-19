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

The pinned SDK 0.4.22 includes startup/restore backpressure handling and separate
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
