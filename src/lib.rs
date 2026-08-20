//! Runtime support for trusted onelastleaf process plugins.

mod runtime;

pub mod protocol {
    tonic::include_proto!("oll.protocol");
}

pub use runtime::{ActionContext, ActionResult, Cancellation, Plugin, PluginBuilder, SdkError};
