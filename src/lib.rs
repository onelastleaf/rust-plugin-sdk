//! Runtime support for trusted onelastleaf process plugins.
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
