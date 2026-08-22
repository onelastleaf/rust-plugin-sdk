//! Runtime support for trusted onelastleaf process plugins.
//!
//! Register asynchronous actions with [`Plugin::builder`]. The SDK owns the
//! oll connection, protocol handshake, concurrent job lifecycle, cooperative
//! cancellation, host calls, structured logs, and artifact transfer.
//!
//! # Example
//!
//! ```no_run
//! use onelastleaf_plugin_sdk::{
//!     ActionResult, Plugin, SdkError,
//!     protocol::{ConfigValue, config_value},
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SdkError> {
//!     Plugin::builder("org.example.echo", env!("CARGO_PKG_VERSION"))
//!         .action("echo", "Echo arguments", |_context, arguments| async move {
//!             ActionResult::value(ConfigValue {
//!                 kind: Some(config_value::Kind::StringValue(arguments.join(" "))),
//!             })
//!         })?
//!         .build()?
//!         .run()
//!         .await
//! }
//! ```
#![warn(missing_docs)]

mod runtime;

/// Generated protobuf messages used by action and host-call APIs.
#[allow(missing_docs)]
pub mod protocol {
    tonic::include_proto!("oll.protocol");
}

pub use runtime::{
    ActionContext, ActionResult, Cancellation, Plugin, PluginBuilder, SdkError, StoredArtifact,
};
