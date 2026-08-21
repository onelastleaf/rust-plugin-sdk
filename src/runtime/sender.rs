use std::sync::Arc;

use tokio::sync::{Mutex, mpsc, oneshot};

use crate::protocol::{self as oll, plugin_envelope};

use super::SdkError;

#[derive(Clone)]
pub(super) struct SessionSender {
    wire: mpsc::Sender<OutboundEnvelope>,
    identity: Arc<SessionIdentity>,
    state: Arc<Mutex<SenderState>>,
}

impl SessionSender {
    pub(super) fn new(wire: mpsc::Sender<OutboundEnvelope>, identity: SessionIdentity) -> Self {
        Self {
            wire,
            identity: Arc::new(identity),
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
            .enqueue(reply_to, trace, payload, None, |_| Ok(()))
            .await?;
        Ok(message_id)
    }

    pub(super) async fn send_and_wait_for_consumption(
        &self,
        reply_to: Option<u64>,
        trace: oll::TraceContext,
        payload: plugin_envelope::Payload,
    ) -> Result<u64, SdkError> {
        let (consumed, acknowledgment) = oneshot::channel();
        let (message_id, ()) = self
            .enqueue(reply_to, trace, payload, Some(consumed), |_| Ok(()))
            .await?;
        acknowledgment.await.map_err(|error| {
            SdkError::runtime("wait for plugin output stream consumption", error)
        })?;
        Ok(message_id)
    }

    pub(super) async fn send_registered<T>(
        &self,
        reply_to: Option<u64>,
        trace: oll::TraceContext,
        payload: plugin_envelope::Payload,
        register: impl FnOnce(u64) -> Result<T, SdkError>,
    ) -> Result<(u64, T), SdkError> {
        self.enqueue(reply_to, trace, payload, None, register).await
    }

    async fn enqueue<T>(
        &self,
        reply_to: Option<u64>,
        trace: oll::TraceContext,
        payload: plugin_envelope::Payload,
        consumed: Option<oneshot::Sender<()>>,
        register: impl FnOnce(u64) -> Result<T, SdkError>,
    ) -> Result<(u64, T), SdkError> {
        // Keep ID allocation, request registration, and bounded enqueue under
        // one lock. Tokio's mpsc send is cancellation-safe, so a dropped send
        // cannot leave registered routing state for an envelope that was not
        // enqueued.
        let mut state = self.state.lock().await;
        let message_id = state.next_message_id;
        state.next_message_id = state
            .next_message_id
            .checked_add(1)
            .ok_or_else(|| SdkError::Protocol("plugin exhausted message IDs".to_owned()))?;
        let registered = register(message_id)?;
        self.wire
            .send(OutboundEnvelope {
                envelope: oll::PluginEnvelope {
                    message_id,
                    reply_to,
                    session_id: self.identity.session_id.clone(),
                    plugin_instance_id: self.identity.instance_id.clone(),
                    trace: Some(trace),
                    payload: Some(payload),
                },
                consumed,
            })
            .await
            .map_err(|_| SdkError::Transport("plugin output stream closed".to_owned()))?;
        Ok((message_id, registered))
    }
}

pub(super) struct OutboundEnvelope {
    envelope: oll::PluginEnvelope,
    consumed: Option<oneshot::Sender<()>>,
}

impl OutboundEnvelope {
    pub(super) fn consume(mut self) -> oll::PluginEnvelope {
        if let Some(consumed) = self.consumed.take() {
            let _ = consumed.send(());
        }
        self.envelope
    }
}

struct SenderState {
    next_message_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SessionIdentity {
    pub(super) session_id: String,
    pub(super) instance_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace() -> oll::TraceContext {
        oll::TraceContext {
            correlation_id: "correlation".to_owned(),
            parent_call_id: None,
            call_depth: 0,
            causal_depth: 0,
            task_id: None,
            task_group_id: None,
        }
    }

    fn payload(nonce: u64) -> plugin_envelope::Payload {
        plugin_envelope::Payload::Heartbeat(oll::Heartbeat { nonce })
    }

    #[tokio::test]
    async fn bounded_output_preserves_order_and_applies_backpressure() {
        let (wire, mut receiver) = mpsc::channel(1);
        let sender = SessionSender::new(
            wire,
            SessionIdentity {
                session_id: "session".to_owned(),
                instance_id: "instance".to_owned(),
            },
        );
        assert_eq!(sender.send(None, trace(), payload(1)).await.unwrap(), 1);

        let second_sender = sender.clone();
        let second =
            tokio::spawn(async move { second_sender.send(None, trace(), payload(2)).await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());

        let first = receiver.recv().await.unwrap().consume();
        assert_eq!(first.message_id, 1);
        assert_eq!(second.await.unwrap().unwrap(), 2);
        let second = receiver.recv().await.unwrap().consume();
        assert_eq!(second.message_id, 2);
        assert_eq!(second.session_id, "session");
        assert_eq!(second.plugin_instance_id, "instance");
    }

    #[tokio::test]
    async fn terminal_delivery_can_wait_until_the_request_stream_consumes_it() {
        let (wire, mut receiver) = mpsc::channel(1);
        let sender = SessionSender::new(
            wire,
            SessionIdentity {
                session_id: "session".to_owned(),
                instance_id: "instance".to_owned(),
            },
        );
        let delivery = tokio::spawn(async move {
            sender
                .send_and_wait_for_consumption(None, trace(), payload(1))
                .await
        });

        tokio::task::yield_now().await;
        assert!(!delivery.is_finished());
        let queued = receiver.recv().await.unwrap();
        assert!(!delivery.is_finished());
        assert_eq!(queued.consume().message_id, 1);
        assert_eq!(delivery.await.unwrap().unwrap(), 1);
    }
}
