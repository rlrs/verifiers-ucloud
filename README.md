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

The pinned SDK 0.4.21 includes startup/restore backpressure handling and separate
relay connection pools for polling, upstream calls, and reply/lease control.
After updating this checkout, run `uv sync` to install the tested SDK.
