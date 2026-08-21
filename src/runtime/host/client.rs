use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

use crate::protocol::{self as oll, plugin_envelope};

use super::super::{SdkError, sender::SessionSender, validation};

#[derive(Clone)]
pub(in crate::runtime) struct HostClient {
    sender: SessionSender,
    pending: Arc<Mutex<HashMap<u64, PendingResponse>>>,
    session_id: Arc<str>,
    maximum_artifact_chunk_bytes: u64,
    maximum_call_depth: u32,
}

impl HostClient {
    pub(in crate::runtime) fn new(
        sender: SessionSender,
        session_id: String,
        maximum_artifact_chunk_bytes: u64,
        maximum_call_depth: u32,
    ) -> Self {
        Self {
            sender,
            pending: Arc::new(Mutex::new(HashMap::new())),
            session_id: session_id.into(),
            maximum_artifact_chunk_bytes,
            maximum_call_depth,
        }
    }

    pub(in crate::runtime) fn sender(&self) -> &SessionSender {
        &self.sender
    }

    pub(in crate::runtime) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(in crate::runtime) fn maximum_artifact_chunk_bytes(&self) -> u64 {
        self.maximum_artifact_chunk_bytes
    }

    pub(in crate::runtime) fn maximum_call_depth(&self) -> u32 {
        self.maximum_call_depth
    }

    pub(in crate::runtime) async fn call(
        &self,
        trace: oll::TraceContext,
        call: oll::host_call_request::Call,
    ) -> Result<oll::HostCallResponse, SdkError> {
        match self
            .request(
                trace,
                plugin_envelope::Payload::HostCall(oll::HostCallRequest { call: Some(call) }),
            )
            .await?
        {
            plugin_envelope::Payload::HostResult(response) => match response.result {
                Some(oll::host_call_response::Result::Error(error)) => {
                    validation::protocol_error(&error)?;
                    Err(SdkError::Host(error))
                }
                Some(_) => Ok(response),
                None => Err(SdkError::Protocol(
                    "HostCallResponse result is required".to_owned(),
                )),
            },
            _ => Err(SdkError::Protocol(
                "host call received another response kind".to_owned(),
            )),
        }
    }

    pub(in crate::runtime) async fn request(
        &self,
        trace: oll::TraceContext,
        payload: plugin_envelope::Payload,
    ) -> Result<plugin_envelope::Payload, SdkError> {
        let (response, receiver) = oneshot::channel();
        let pending = self.pending.clone();
        let expected_trace = trace.clone();
        let (_, (receiver, mut guard)) = self
            .sender
            .send_registered(None, trace, payload, move |message_id| {
                {
                    let mut requests = pending.lock().map_err(|_| {
                        SdkError::Protocol("pending host-call state is poisoned".to_owned())
                    })?;
                    let std::collections::hash_map::Entry::Vacant(entry) =
                        requests.entry(message_id)
                    else {
                        return Err(SdkError::Protocol(
                            "plugin reused a pending request message ID".to_owned(),
                        ));
                    };
                    entry.insert(PendingResponse {
                        expected_trace,
                        response,
                    });
                }
                Ok((
                    receiver,
                    PendingGuard {
                        message_id,
                        pending,
                        sent: false,
                    },
                ))
            })
            .await?;
        guard.sent = true;
        // From this point the request is on the ordered wire. If its action is
        // cancelled, retain the routing entry so the host's eventual direct
        // response is recognized and discarded instead of failing the session.
        let payload = receiver
            .await
            .map_err(|error| SdkError::runtime("wait for host response", error))?;
        match payload {
            plugin_envelope::Payload::ProtocolError(error) => Err(SdkError::Host(error)),
            payload => Ok(payload),
        }
    }

    pub(in crate::runtime) fn route(
        &self,
        reply_to: u64,
        trace: &oll::TraceContext,
        payload: plugin_envelope::Payload,
    ) -> Result<(), SdkError> {
        let waiter = self
            .pending
            .lock()
            .map_err(|_| SdkError::Protocol("pending host-call state is poisoned".to_owned()))?
            .remove(&reply_to)
            .ok_or_else(|| {
                SdkError::Protocol("response names no pending plugin request".to_owned())
            })?;
        if waiter.expected_trace != *trace {
            return Err(SdkError::Protocol(
                "response trace context differs from its plugin request".to_owned(),
            ));
        }
        // A cancelled action drops its receiver but deliberately leaves this
        // routing entry until the already-issued host operation settles.
        let _ = waiter.response.send(payload);
        Ok(())
    }
}

struct PendingResponse {
    expected_trace: oll::TraceContext,
    response: oneshot::Sender<plugin_envelope::Payload>,
}

struct PendingGuard {
    message_id: u64,
    pending: Arc<Mutex<HashMap<u64, PendingResponse>>>,
    sent: bool,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if self.sent {
            return;
        }
        match self.pending.lock() {
            Ok(mut pending) => {
                pending.remove(&self.message_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&self.message_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::runtime::sender::{OutboundEnvelope, SessionIdentity};

    fn trace() -> oll::TraceContext {
        oll::TraceContext {
            correlation_id: "correlation".to_owned(),
            parent_call_id: Some(7),
            call_depth: 1,
            causal_depth: 0,
            task_id: Some("task".to_owned()),
            task_group_id: Some("group".to_owned()),
        }
    }

    fn client() -> (HostClient, mpsc::Receiver<OutboundEnvelope>) {
        let (wire, receiver) = mpsc::channel(4);
        let sender = SessionSender::new(
            wire,
            SessionIdentity {
                session_id: "session".to_owned(),
                instance_id: "instance".to_owned(),
            },
        );
        (
            HostClient::new(sender, "session".to_owned(), 64, 10),
            receiver,
        )
    }

    async fn receive(wire: &mut mpsc::Receiver<OutboundEnvelope>) -> oll::PluginEnvelope {
        wire.recv().await.unwrap().consume()
    }

    #[tokio::test]
    async fn late_response_to_cancelled_waiter_is_discarded_without_failing_the_session() {
        let (client, mut wire) = client();
        let request_client = client.clone();
        let expected_trace = trace();
        let request_trace = expected_trace.clone();
        let task = tokio::spawn(async move {
            request_client
                .request(
                    request_trace,
                    plugin_envelope::Payload::HostCall(oll::HostCallRequest { call: None }),
                )
                .await
        });
        let request = receive(&mut wire).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        client
            .route(
                request.message_id,
                &expected_trace,
                plugin_envelope::Payload::HostResult(oll::HostCallResponse { result: None }),
            )
            .unwrap();
        assert!(client.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn response_routing_compares_the_complete_trace_context() {
        let (client, mut wire) = client();
        let request_client = client.clone();
        let task = tokio::spawn(async move {
            request_client
                .request(
                    trace(),
                    plugin_envelope::Payload::HostCall(oll::HostCallRequest { call: None }),
                )
                .await
        });
        let request = receive(&mut wire).await;
        let mut changed = trace();
        changed.task_id = Some("another-task".to_owned());
        assert!(
            client
                .route(
                    request.message_id,
                    &changed,
                    plugin_envelope::Payload::HostResult(oll::HostCallResponse { result: None }),
                )
                .is_err()
        );
        assert!(task.await.unwrap().is_err());
    }
}
