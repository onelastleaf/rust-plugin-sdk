use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use super::{ActionContext, ActionResult, SdkError, session, validation};

// The host may multiplex jobs without a negotiated count. Keep the process
// bounded by default while allowing workloads to choose a tighter limit.
const DEFAULT_MAXIMUM_CONCURRENT_JOBS: usize = 256;

pub(super) type ActionFuture = Pin<Box<dyn Future<Output = Result<ActionResult, SdkError>> + Send>>;
pub(super) type ActionHandler =
    Arc<dyn Fn(ActionContext, Vec<String>) -> ActionFuture + Send + Sync>;

#[derive(Clone)]
pub(super) struct RegisteredAction {
    pub(super) description: String,
    pub(super) handler: ActionHandler,
}

/// A configured plugin ready to open its oll-owned runtime stream.
pub struct Plugin {
    pub(super) plugin_id: String,
    pub(super) version: String,
    pub(super) actions: HashMap<String, RegisteredAction>,
    pub(super) maximum_concurrent_jobs: usize,
}

/// Validates and assembles a [`Plugin`] one action at a time.
pub struct PluginBuilder(Plugin);

impl Plugin {
    /// Starts a builder for the immutable publisher ID and informational version.
    pub fn builder(plugin_id: impl Into<String>, version: impl Into<String>) -> PluginBuilder {
        PluginBuilder(Plugin {
            plugin_id: plugin_id.into(),
            version: version.into(),
            actions: HashMap::new(),
            maximum_concurrent_jobs: DEFAULT_MAXIMUM_CONCURRENT_JOBS,
        })
    }

    /// Connects to the oll-owned endpoint and runs until shutdown, failure, or stdin EOF.
    pub async fn run(self) -> Result<(), SdkError> {
        let endpoint = std::env::var("OLL_PLUGIN_ENDPOINT")
            .map_err(|error| SdkError::runtime("read OLL_PLUGIN_ENDPOINT", error))?;
        session::run(self, endpoint).await
    }
}

impl PluginBuilder {
    /// Sets the maximum number of action futures owned by this process.
    pub fn maximum_concurrent_jobs(mut self, maximum: usize) -> Result<Self, SdkError> {
        if maximum == 0 {
            return Err(SdkError::InvalidArgument(
                "maximum concurrent jobs must be nonzero".to_owned(),
            ));
        }
        self.0.maximum_concurrent_jobs = maximum;
        Ok(self)
    }

    /// Registers one uniquely named asynchronous action.
    pub fn action<F, Fut>(
        mut self,
        name: impl Into<String>,
        description: impl Into<String>,
        handler: F,
    ) -> Result<Self, SdkError>
    where
        F: Fn(ActionContext, Vec<String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ActionResult, SdkError>> + Send + 'static,
    {
        let name = name.into();
        if name.is_empty() || self.0.actions.contains_key(&name) {
            return Err(SdkError::InvalidArgument(
                "action names must be nonempty and unique".to_owned(),
            ));
        }
        self.0.actions.insert(
            name,
            RegisteredAction {
                description: description.into(),
                handler: Arc::new(move |context, arguments| Box::pin(handler(context, arguments))),
            },
        );
        Ok(self)
    }

    /// Validates the plugin declaration and returns a runnable plugin.
    pub fn build(self) -> Result<Plugin, SdkError> {
        validation::plugin_id(&self.0.plugin_id)?;
        if self.0.version.is_empty() {
            return Err(SdkError::InvalidArgument(
                "plugin version must not be empty".to_owned(),
            ));
        }
        Ok(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_rejects_invalid_identity_and_duplicate_actions() {
        assert!(Plugin::builder("invalid", "0.1.0").build().is_err());
        assert!(
            Plugin::builder("org.example.echo", "0.1.0")
                .maximum_concurrent_jobs(0)
                .is_err()
        );
        let builder = Plugin::builder("org.example.echo", "0.1.0")
            .action("echo", "echo", |_, _| async { Ok(ActionResult::default()) })
            .unwrap();
        assert!(
            builder
                .action("echo", "duplicate", |_, _| async {
                    Ok(ActionResult::default())
                })
                .is_err()
        );
    }
}
