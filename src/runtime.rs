mod error;
mod host;
mod plugin;
mod sender;
mod session;
mod validation;

pub use error::SdkError;
pub use host::{ActionContext, ActionResult, Cancellation};
pub use plugin::{Plugin, PluginBuilder};

const MAXIMUM_ENVELOPE_BYTES: usize = 64 * 1024 * 1024;
const OUTGOING_CAPACITY: usize = 256;
