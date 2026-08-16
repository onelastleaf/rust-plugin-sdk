# onelastleaf Rust plugin SDK

The official Rust runtime for trusted onelastleaf process plugins. It owns the
gRPC stream, handshake, message ordering, job cancellation, host calls,
artifacts, heartbeat, shutdown, and stdin parent-liveness contract.

```rust
use onelastleaf_plugin_sdk::{ActionResult, Plugin};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Plugin::builder("org.example.echo", env!("CARGO_PKG_VERSION"))
        .action("echo", "Return the supplied arguments", |_context, arguments| async move {
            println!("{arguments:?}");
            Ok(ActionResult::default())
        })?
        .build()?
        .run()
        .await?;
    Ok(())
}
```

The package requires the exact oll protocol fingerprint recorded by this SDK
release. A mismatch is rejected during the application handshake.
