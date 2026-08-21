mod jobs;
mod liveness;

use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt as _, wrappers::ReceiverStream};
use tonic::{Request, transport::Endpoint};

use crate::protocol::{self as oll, plugin_envelope, plugin_runtime_client::PluginRuntimeClient};

use super::{
    OUTGOING_CAPACITY, SdkError,
    host::HostClient,
    plugin::Plugin,
    sender::{OutboundEnvelope, SessionIdentity, SessionSender},
    validation,
};
use jobs::JobManager;
use liveness::ParentLiveness;

pub(super) async fn run(plugin: Plugin, endpoint: String) -> Result<(), SdkError> {
    let endpoint = validation::endpoint(&endpoint)?;
    let mut parent = ParentLiveness::start()?;
    tokio::select! {
        result = run_session(plugin, endpoint) => result,
        result = parent.wait() => result,
    }
}

async fn run_session(plugin: Plugin, endpoint: http::Uri) -> Result<(), SdkError> {
    let channel = Endpoint::from(endpoint)
        .connect()
        .await
        .map_err(|error| SdkError::runtime("connect to oll plugin endpoint", error))?;
    let (wire_tx, wire_rx) = mpsc::channel::<OutboundEnvelope>(OUTGOING_CAPACITY);
    let mut client = PluginRuntimeClient::new(channel)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);
    let outgoing = ReceiverStream::new(wire_rx).map(OutboundEnvelope::consume);
    let mut incoming = client
        .connect(Request::new(outgoing))
        .await
        .map_err(|error| SdkError::runtime("open plugin runtime stream", error))?
        .into_inner();

    let first = receive_initial(&mut incoming).await?;
    if first.session_id.is_empty() || first.plugin_instance_id.is_empty() {
        return Err(SdkError::Protocol(
            "HostHello envelope omitted its session or instance identity".to_owned(),
        ));
    }
    if first.reply_to.is_some() {
        return Err(SdkError::Protocol(
            "HostHello must not reply to another message".to_owned(),
        ));
    }
    let Some(plugin_envelope::Payload::HostHello(ref hello)) = first.payload else {
        return Err(SdkError::Protocol(
            "HostHello must be the first host message".to_owned(),
        ));
    };
    validation::host_hello(&plugin.plugin_id, hello)?;
    let handshake_trace =
        validation::trace(&first, hello.maximum_call_depth, hello.maximum_causal_depth)?.clone();
    let mut session = SessionState {
        identity: SessionIdentity {
            session_id: first.session_id.clone(),
            instance_id: first.plugin_instance_id.clone(),
        },
        last_host_message_id: first.message_id,
        maximum_call_depth: hello.maximum_call_depth,
        maximum_causal_depth: hello.maximum_causal_depth,
    };
    let sender = SessionSender::new(wire_tx, session.identity.clone());
    let effective_name = hello
        .plugin_name
        .as_ref()
        .expect("validated plugin name")
        .value
        .clone();
    sender
        .send(
            None,
            handshake_trace.clone(),
            plugin_envelope::Payload::PluginHello(oll::PluginHello {
                plugin_id: Some(oll::PluginId {
                    value: plugin.plugin_id,
                }),
                plugin_name: Some(oll::PluginName {
                    value: effective_name,
                }),
                actions: plugin
                    .actions
                    .iter()
                    .map(|(name, action)| oll::ActionDescriptor {
                        name: name.clone(),
                        description: action.description.clone(),
                    })
                    .collect(),
                plugin_version: plugin.version,
            }),
        )
        .await?;

    let ready = receive(&mut incoming, &mut session).await?;
    if ready.reply_to.is_some()
        || ready.trace.as_ref() != Some(&handshake_trace)
        || !matches!(ready.payload, Some(plugin_envelope::Payload::Ready(_)))
    {
        return Err(SdkError::Protocol(
            "host SessionReady must follow PluginHello with the HostHello trace".to_owned(),
        ));
    }
    sender
        .send(
            None,
            handshake_trace,
            plugin_envelope::Payload::Ready(oll::SessionReady {}),
        )
        .await?;

    let host = HostClient::new(
        sender,
        session.identity.session_id.clone(),
        hello.maximum_artifact_chunk_bytes,
        hello.maximum_call_depth,
    );
    let shutdown_deadline = serve(
        plugin.actions,
        plugin.maximum_concurrent_jobs,
        &mut incoming,
        &mut session,
        host,
    )
    .await?;
    wait_for_host_close(&mut incoming, shutdown_deadline).await
}

async fn serve(
    actions: std::collections::HashMap<String, super::plugin::RegisteredAction>,
    maximum_concurrent_jobs: usize,
    incoming: &mut tonic::Streaming<oll::PluginEnvelope>,
    session: &mut SessionState,
    host: HostClient,
) -> Result<prost_types::Timestamp, SdkError> {
    let mut jobs = JobManager::new(actions, maximum_concurrent_jobs, host.clone());
    loop {
        let envelope = tokio::select! {
            finished = jobs.join_next(), if jobs.has_active_jobs() => {
                jobs.settle(finished?).await?;
                continue;
            }
            value = incoming.message() => match value {
                Ok(Some(envelope)) => envelope,
                Ok(None) => {
                    return Err(SdkError::Transport("host closed the plugin stream".to_owned()));
                }
                Err(error) => {
                    return Err(SdkError::runtime("receive plugin runtime envelope", error));
                }
            },
        };
        let trace = validate_envelope(&envelope, session)?.clone();
        let message_id = envelope.message_id;
        if let Some(reply_to) = envelope.reply_to {
            let payload = envelope
                .payload
                .ok_or_else(|| SdkError::Protocol("response payload is required".to_owned()))?;
            if let plugin_envelope::Payload::ProtocolError(ref error) = payload {
                validation::protocol_error(error)?;
            }
            host.route(reply_to, &trace, payload)?;
            continue;
        }

        match envelope.payload {
            Some(plugin_envelope::Payload::StartJob(request)) => {
                jobs.start(message_id, trace, request).await?;
            }
            Some(plugin_envelope::Payload::CancelJob(request)) => {
                jobs.cancel(message_id, trace, request).await?;
            }
            Some(plugin_envelope::Payload::Heartbeat(heartbeat)) => {
                host.sender()
                    .send(
                        Some(message_id),
                        trace,
                        plugin_envelope::Payload::Heartbeat(heartbeat),
                    )
                    .await?;
            }
            Some(plugin_envelope::Payload::Shutdown(request)) => {
                let deadline = validation::shutdown(&request)?;
                jobs.shutdown().await?;
                host.sender()
                    .send_and_wait_for_consumption(
                        Some(message_id),
                        trace,
                        plugin_envelope::Payload::ShutdownAcknowledged(
                            oll::ShutdownAcknowledged {},
                        ),
                    )
                    .await?;
                return Ok(deadline);
            }
            Some(plugin_envelope::Payload::ProtocolError(error)) => {
                validation::protocol_error(&error)?;
                return Err(SdkError::Host(error));
            }
            Some(_) => {
                return Err(SdkError::Protocol(
                    "unexpected host-initiated message".to_owned(),
                ));
            }
            None => return Err(SdkError::Protocol("payload is required".to_owned())),
        }
    }
}

async fn wait_for_host_close<S, E>(
    incoming: &mut S,
    deadline: prost_types::Timestamp,
) -> Result<(), SdkError>
where
    S: Stream<Item = Result<oll::PluginEnvelope, E>> + Unpin,
{
    let deadline = std::time::SystemTime::try_from(deadline)
        .map_err(|error| SdkError::Protocol(format!("invalid shutdown deadline: {error}")))?;
    let remaining = deadline
        .duration_since(std::time::SystemTime::now())
        .unwrap_or_default();
    match tokio::time::timeout(remaining, incoming.next()).await {
        Err(_) | Ok(None) | Ok(Some(Err(_))) => Ok(()),
        Ok(Some(Ok(_))) => Err(SdkError::Protocol(
            "host sent a message after ShutdownRequest".to_owned(),
        )),
    }
}

async fn receive_initial(
    incoming: &mut tonic::Streaming<oll::PluginEnvelope>,
) -> Result<oll::PluginEnvelope, SdkError> {
    let envelope = incoming
        .message()
        .await
        .map_err(|error| SdkError::runtime("receive initial HostHello", error))?
        .ok_or_else(|| SdkError::Transport("host closed the plugin stream".to_owned()))?;
    if envelope.message_id == 0 {
        return Err(SdkError::Protocol(
            "host message IDs must be nonzero and strictly increasing".to_owned(),
        ));
    }
    Ok(envelope)
}

struct SessionState {
    identity: SessionIdentity,
    last_host_message_id: u64,
    maximum_call_depth: u32,
    maximum_causal_depth: u32,
}

fn validate_envelope<'a>(
    envelope: &'a oll::PluginEnvelope,
    session: &mut SessionState,
) -> Result<&'a oll::TraceContext, SdkError> {
    if envelope.message_id == 0 || envelope.message_id <= session.last_host_message_id {
        return Err(SdkError::Protocol(
            "host message IDs must be nonzero and strictly increasing".to_owned(),
        ));
    }
    if envelope.session_id != session.identity.session_id
        || envelope.plugin_instance_id != session.identity.instance_id
    {
        return Err(SdkError::Protocol(
            "host envelope belongs to another plugin instance".to_owned(),
        ));
    }
    let trace = validation::trace(
        envelope,
        session.maximum_call_depth,
        session.maximum_causal_depth,
    )?;
    session.last_host_message_id = envelope.message_id;
    Ok(trace)
}

async fn receive(
    incoming: &mut tonic::Streaming<oll::PluginEnvelope>,
    session: &mut SessionState,
) -> Result<oll::PluginEnvelope, SdkError> {
    let envelope = incoming
        .message()
        .await
        .map_err(|error| SdkError::runtime("receive handshake envelope", error))?
        .ok_or_else(|| SdkError::Transport("host closed the plugin stream".to_owned()))?;
    validate_envelope(&envelope, session)?;
    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(message_id: u64) -> oll::PluginEnvelope {
        oll::PluginEnvelope {
            message_id,
            reply_to: None,
            session_id: "session".to_owned(),
            plugin_instance_id: "instance".to_owned(),
            trace: Some(oll::TraceContext {
                correlation_id: "correlation".to_owned(),
                parent_call_id: None,
                call_depth: 0,
                causal_depth: 0,
                task_id: None,
                task_group_id: None,
            }),
            payload: Some(plugin_envelope::Payload::Heartbeat(oll::Heartbeat {
                nonce: 1,
            })),
        }
    }

    #[test]
    fn established_envelopes_require_exact_identity_and_monotonic_ids() {
        let mut session = SessionState {
            identity: SessionIdentity {
                session_id: "session".to_owned(),
                instance_id: "instance".to_owned(),
            },
            last_host_message_id: 4,
            maximum_call_depth: 10,
            maximum_causal_depth: 10,
        };
        validate_envelope(&envelope(5), &mut session).unwrap();
        assert_eq!(session.last_host_message_id, 5);
        assert!(validate_envelope(&envelope(5), &mut session).is_err());

        let mut stale = envelope(6);
        stale.session_id = "stale".to_owned();
        assert!(validate_envelope(&stale, &mut session).is_err());
        assert_eq!(session.last_host_message_id, 5);
    }

    fn shutdown_deadline() -> prost_types::Timestamp {
        prost_types::Timestamp::from(
            std::time::SystemTime::now() + std::time::Duration::from_secs(1),
        )
    }

    #[tokio::test]
    async fn shutdown_wait_accepts_host_stream_close() {
        let mut incoming = tokio_stream::empty::<Result<oll::PluginEnvelope, tonic::Status>>();
        wait_for_host_close(&mut incoming, shutdown_deadline())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_wait_rejects_late_host_messages() {
        let mut incoming = tokio_stream::iter([Ok::<_, tonic::Status>(envelope(6))]);
        assert!(
            wait_for_host_close(&mut incoming, shutdown_deadline())
                .await
                .is_err()
        );
    }
}
