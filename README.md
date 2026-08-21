# onelastleaf Rust plugin SDK

Build trusted [onelastleaf](https://github.com/onelastleaf/onelastleaf) plugins
with async Rust. You register ordinary async functions as actions; the SDK takes
care of the gRPC session, handshake, message ordering, cancellation, host calls,
artifacts, heartbeat, and graceful shutdown.

## Build and test the SDK

You need Rust 1.88 or newer (the current Tonic dependency requires it). The build
uses a vendored `protoc`, so you do not need to install Protocol Buffers
separately.

From this repository:

```bash
cargo build --all-targets
cargo test --all-targets
```

That compiles the library and the conformance fixture, then runs the SDK's unit
tests.

## Create a plugin

The simplest route is to let oll generate the project and its `oll.toml`
manifest together:

```bash
oll plugin new echo-plugin \
  --language rust \
  --id org.example.echo \
  --name echo-plugin
cd echo-plugin
cargo test
```

The generated project already has a working `echo` action, a test, the SDK
dependency from crates.io, and the source-build recipe used during installation.

Commit the `Cargo.lock` that Cargo creates after it successfully resolves the
dependencies. The installation recipe deliberately uses Cargo's `--locked`
flag so a later install cannot quietly choose a different dependency set.

A minimal plugin entry point looks like this:

```rust
use onelastleaf_plugin_sdk::{
    ActionResult, Plugin, SdkError,
    protocol::{ConfigValue, config_value},
};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    Plugin::builder("org.example.echo", env!("CARGO_PKG_VERSION"))
        .action("echo", "Return the supplied arguments", |_context, arguments| async move {
            ActionResult::value(ConfigValue {
                kind: Some(config_value::Kind::StringValue(arguments.join(" "))),
            })
        })?
        .build()?
        .run()
        .await
}
```

The plugin ID passed to `Plugin::builder` must be the same immutable ID declared
in `oll.toml`. Action names must be non-empty and unique.

The runtime owns at most 256 concurrent action futures by default. Use
`PluginBuilder::maximum_concurrent_jobs` to choose a tighter or larger bound;
admission beyond that bound receives a retryable `UNAVAILABLE` response instead
of creating an unbounded task.

### Add an existing Rust project by hand

If you do not use `oll plugin new`, add the published SDK and Tokio to
`Cargo.toml`:

```toml
[dependencies]
onelastleaf-plugin-sdk = "=0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Then place an `oll.toml` like this at the repository root. Change the ID, name,
and binary name to match your project:

```toml
format_version = 1

[plugin]
id = "org.example.echo"
name = "echo-plugin"

[source]
checkout = "source"
steps = [
  ["cargo", "install", "--locked", "--path", "{source}", "--root", "{install}"],
]

[source.dependencies]
cargo = "Install the Rust toolchain and ensure cargo is in PATH."

[runtime]
argv = ["{install}/bin/echo-plugin"]
```

This SDK follows the canonical protobuf wire contract. It never computes,
embeds, publishes, or compares a schema hash or fingerprint. Descriptor-wide
hashes change for compatible additions and unrelated services, so they reject
valid peers. Protocol changes instead preserve field numbers and wire types,
give additions safe absent semantics, and tolerate unknown fields. Unknown enum
values are retained when the current operation has safe semantics for them,
such as a future nonzero cancellation reason or structured error code. Exact
SDK pins provide reproducible builds; they are not protobuf API versioning.

## Install and run the plugin with oll

oll installs plugins from Git remotes. Once the generated project is committed
and pushed somewhere the oll daemon can reach, run:

```bash
oll plugin install https://github.com/your-name/echo-plugin.git
oll plugin start org.example.echo
oll plugin call org.example.echo echo -- hello from oll
```

`plugin call` prints a job ID after the plugin accepts the work. Use that ID to
inspect the eventual result:

```bash
oll job info <job-id>
oll plugin log org.example.echo
```

After pushing a new commit, update and restart explicitly:

```bash
oll plugin update org.example.echo
oll plugin restart org.example.echo
```

A successful update does not restart a running process for you, which lets you
choose when the new build takes over.

## How the runtime fits together

The plugin is a gRPC **client**, not a server. oll starts the executable, opens
its own loopback gRPC server on an ephemeral port, and passes that address in
`OLL_PLUGIN_ENDPOINT`. The SDK reads the variable and connects when `run()` is
called. You normally should not set the variable or launch the binary by hand.

The SDK does not impose a fixed encoded-size limit on gRPC envelopes. Effective
limits come from the gRPC implementation and available memory; artifact bytes
still use the negotiated bounded-chunk transfer protocol.

stdin is also part of the runtime contract: oll keeps it open as a liveness
pipe, and the plugin exits when it reaches EOF. stdout and stderr are captured
in the per-plugin log. Application input arrives as action arguments or through
host calls, never through stdin.

Inside an action, `ActionContext` gives you cooperative cancellation, the job
deadline and trace context, current plugin configuration, configuration-function
calls, structured logging, document host calls, and verified artifact transfer.
The project produced by `oll plugin new` is the supported starting point for a
complete plugin executable.
