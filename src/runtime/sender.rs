use std::sync::Arc;

use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

use crate::protocol::{self as oll, plugin_envelope};

use super::SdkError;

#[derive(Clone)]
pub(super) struct SessionSender {
    wire: mpsc::Sender<OutboundEnvelope>,
    identity: Arc<RwLock<SessionIdentity>>,
    state: Arc<Mutex<SenderState>>,
}

impl SessionSender {
    pub(super) fn new(
        wire: mpsc::Sender<OutboundEnvelope>,
        identity: Arc<RwLock<SessionIdentity>>,
    ) -> Self {
        Self {
            wire,
            identity,
            state: Arc::new(Mutex::new(SenderState { next_message_id: 1 })),
        }
    }

    pub(super) async fn send(
        &self,
        reply_to: Option<u64>,
        trace: oll::TraceContext,
        payload: plugin_envelope::Payload,
    ) -> Result<u64, SdkError> {
        let (message_id, ()) = self
            .send_registered(reply_to, trace, payload, |_| Ok(()))
            .await?;
        Ok(message_id)
    }

    pub(super) async fn send_registered<T>(
        &self,
        reply_to: Option<u64>,
        trace: oll::TraceContext,
        payload: plugin_envelope::Payload,
        register: impl FnOnce(u64) -> Result<T, SdkError>,
    ) -> Result<(u64, T), SdkError> {
        let mut state = self.state.lock().await;
        let message_id = state.next_message_id;
        state.next_message_id = state
            .next_message_id
            .checked_add(1)
            .ok_or_else(|| SdkError::Protocol("plugin exhausted message IDs".to_owned()))?;
        let registered = register(message_id)?;
        let identity = self.identity.read().await.clone();
        let (consumed, acknowledged) = oneshot::channel();
        self.wire
            .send(OutboundEnvelope {
                envelope: oll::PluginEnvelope {
                    message_id,
                    reply_to,
                    session_id: identity.session_id,
                    plugin_instance_id: identity.instance_id,
                    trace: Some(trace),
                    payload: Some(payload),
                },
                consumed,
            })
            .await
            .map_err(|_| SdkError::Transport("plugin output stream closed".to_owned()))?;
        drop(state);
        acknowledged.await.map_err(|_| {
            SdkError::Transport("plugin output stream closed before consuming a message".to_owned())
        })?;
        Ok((message_id, registered))
    }
}

struct SenderState {
    next_message_id: u64,
}

pub(super) struct OutboundEnvelope {
    pub(super) envelope: oll::PluginEnvelope,
    consumed: oneshot::Sender<()>,
}

impl OutboundEnvelope {
    pub(super) fn consume(self) -> oll::PluginEnvelope {
        let _ = self.consumed.send(());
        self.envelope
    }
}

#[derive(Clone, Default)]
pub(super) struct SessionIdentity {
    pub(super) session_id: String,
    pub(super) instance_id: String,
}
