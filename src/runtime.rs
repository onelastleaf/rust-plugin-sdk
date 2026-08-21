mod cancellation;
mod error;
mod host;
mod plugin;
mod sender;
mod session;
mod validation;
mod value;

pub use cancellation::Cancellation;
pub use error::SdkError;
pub use host::{ActionContext, ActionResult, StoredArtifact};
pub use plugin::{Plugin, PluginBuilder};

// Bound burst memory while allowing independent action tasks to enqueue behind
// the one sender-ordering lock.
const OUTGOING_CAPACITY: usize = 256;
