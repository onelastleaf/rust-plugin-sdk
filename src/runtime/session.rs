use std::{collections::HashMap, io::Read as _, sync::Arc};

use tokio::sync::{RwLock, mpsc, watch};
use tokio_stream::{StreamExt as _, wrappers::ReceiverStream};
use tonic::{Request, transport::Endpoint};

use crate::protocol::{self as oll, plugin_envelope, plugin_runtime_client::PluginRuntimeClient};

use super::{
    ActionContext, Cancellation, OUTGOING_CAPACITY, SdkError,
    host::HostClient,
    plugin::Plugin,
    sender::{OutboundEnvelope, SessionIdentity, SessionSender},
    validation,
};

pub(super) async fn run(plugin: Plugin, endpoint: String) -> Result<(), SdkError> {
    let endpoint = validation::endpoint(&endpoint)?;
    let mut stdin_eof = parent_liveness();
    tokio::select! {
        result = run_session(plugin, endpoint) => result,
        _ = &mut stdin_eof => Ok(()),
    }
}

async fn run_session(plugin: Plugin, endpoint: String) -> Result<(), SdkError> {
    let channel = Endpoint::from_shared(endpoint)
        .map_err(|error| SdkError::Environment(error.to_string()))?
        .connect()
        .await
        .map_err(|error| SdkError::Transport(error.to_string()))?;
    let (wire_tx, wire_rx) = mpsc::channel::<OutboundEnvelope>(OUTGOING_CAPACITY);
    let mut client = PluginRuntimeClient::new(channel)
        .max_decoding_message_size(super::MAXIMUM_ENVELOPE_BYTES)
        .max_encoding_message_size(super::MAXIMUM_ENVELOPE_BYTES);
    let outgoing = ReceiverStream::new(wire_rx).map(|message| message.consume());
    let mut incoming = client
        .connect(Request::new(outgoing))
        .await
        .map_err(|error| SdkError::Transport(error.to_string()))?
        .into_inner();
    let identity = Arc::new(RwLock::new(SessionIdentity::default()));
    let sender = SessionSender::new(wire_tx, identity.clone());

    let first = receive(&mut incoming, &identity, 0, u32::MAX, u32::MAX).await?;
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
    validation::host_hello(&plugin.plugin_id, &hello)?;
    let trace =
        validation::trace(&first, hello.maximum_call_depth, hello.maximum_causal_depth)?.clone();
    *identity.write().await = SessionIdentity {
        session_id: first.session_id.clone(),
        instance_id: first.plugin_instance_id.clone(),
    };
    let effective_name = hello
        .plugin_name
        .as_ref()
        .expect("validated plugin name")
        .value
        .clone();
    sender
        .send(
            None,
            trace.clone(),
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
    let ready = receive(
        &mut incoming,
        &identity,
        first.message_id,
        hello.maximum_call_depth,
        hello.maximum_causal_depth,
    )
    .await?;
    if ready.reply_to.is_some()
        || validation::trace(&ready, hello.maximum_call_depth, hello.maximum_causal_depth)?
            != &trace
        || !matches!(ready.payload, Some(plugin_envelope::Payload::Ready(_)))
    {
        return Err(SdkError::Protocol(
            "host SessionReady must follow PluginHello".to_owned(),
        ));
    }
    sender
        .send(
            None,
            trace,
            plugin_envelope::Payload::Ready(oll::SessionReady {}),
        )
        .await?;

    let host = HostClient::new(
        sender.clone(),
        hello.maximum_artifact_chunk_bytes,
        hello.maximum_call_depth,
    );
    let shutdown_deadline = serve(
        plugin.actions,
        &mut incoming,
        identity,
        sender,
        host,
        ready.message_id,
        hello.maximum_call_depth,
        hello.maximum_causal_depth,
    )
    .await?;
    if let Some(deadline) = shutdown_deadline {
        wait_for_host_close(&mut incoming, deadline).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn serve(
    actions: HashMap<String, super::plugin::RegisteredAction>,
    incoming: &mut tonic::Streaming<oll::PluginEnvelope>,
    identity: Arc<RwLock<SessionIdentity>>,
    sender: SessionSender,
    host: HostClient,
    mut last_host_message_id: u64,
    maximum_call_depth: u32,
    maximum_causal_depth: u32,
) -> Result<Option<prost_types::Timestamp>, SdkError> {
    let mut jobs: HashMap<String, ActiveJob> = HashMap::new();
    let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
    let result = loop {
        let envelope = tokio::select! {
            completed = completed_rx.recv() => {
                if let Some(job_id) = completed {
                    jobs.remove(&job_id);
                }
                continue;
            }
            value = incoming.message() => match value {
                Ok(Some(envelope)) => envelope,
                Ok(None) => break Err(SdkError::Transport(
                    "host closed the plugin stream".to_owned(),
                )),
                Err(error) => break Err(SdkError::Transport(error.to_string())),
            },
        };
        if let Err(error) = validate_envelope(
            &envelope,
            &identity,
            &mut last_host_message_id,
            maximum_call_depth,
            maximum_causal_depth,
        )
        .await
        {
            break Err(error);
        }
        let message_id = envelope.message_id;
        let trace = match validation::trace(&envelope, maximum_call_depth, maximum_causal_depth) {
            Ok(trace) => trace.clone(),
            Err(error) => break Err(error),
        };
        if let Some(reply_to) = envelope.reply_to {
            let Some(payload) = envelope.payload else {
                break Err(SdkError::Protocol(
                    "response payload is required".to_owned(),
                ));
            };
            if let Err(error) = host.route(reply_to, &trace, payload) {
                break Err(error);
            }
            continue;
        }
        match envelope.payload {
            Some(plugin_envelope::Payload::StartJob(request)) => {
                let job_id = match validation::job_id(request.job_id.as_ref()) {
                    Ok(value) => value.to_owned(),
                    Err(error) => break Err(error),
                };
                if jobs.contains_key(&job_id) {
                    break Err(SdkError::Protocol("duplicate active job ID".to_owned()));
                }
                let Some(oll::start_job_request::Invocation::Action(invocation)) =
                    request.invocation
                else {
                    break Err(SdkError::Protocol("unsupported job invocation".to_owned()));
                };
                let Some(action) = actions.get(&invocation.action).cloned() else {
                    break Err(SdkError::Protocol(format!(
                        "unknown action `{}`",
                        invocation.action
                    )));
                };
                if let Err(error) = sender
                    .send(
                        Some(message_id),
                        trace.clone(),
                        plugin_envelope::Payload::JobAccepted(oll::JobAccepted {
                            job_id: Some(oll::PluginJobId {
                                value: job_id.clone(),
                            }),
                        }),
                    )
                    .await
                {
                    break Err(error);
                }
                let (cancel_tx, cancel_rx) = watch::channel(false);
                let cancellation = Cancellation(cancel_rx);
                let cancellation_observer = cancellation.clone();
                let task_sender = sender.clone();
                let task_job_id = job_id.clone();
                let task_trace = trace.clone();
                let completion = completed_tx.clone();
                let context = ActionContext {
                    job_id: job_id.clone(),
                    deadline: request.deadline,
                    trace,
                    cancellation,
                    parent_call_id: message_id,
                    host: host.clone(),
                };
                let task = tokio::spawn(async move {
                    let output = (action.handler)(context, invocation.arguments).await;
                    if !cancellation_observer.is_cancelled() {
                        let (state, result, error, artifacts) = match output {
                            Ok(result) => (
                                oll::JobState::Succeeded,
                                result.result,
                                None,
                                result.artifacts,
                            ),
                            Err(error) => (
                                oll::JobState::Failed,
                                None,
                                Some(error.protocol_error()),
                                Vec::new(),
                            ),
                        };
                        let _ = task_sender
                            .send(
                                None,
                                task_trace,
                                plugin_envelope::Payload::JobUpdate(oll::JobUpdate {
                                    job_id: Some(oll::PluginJobId {
                                        value: task_job_id.clone(),
                                    }),
                                    state: state as i32,
                                    progress: Some(1.0),
                                    status_message: None,
                                    result,
                                    error,
                                    artifacts,
                                }),
                            )
                            .await;
                    }
                    let _ = completion.send(task_job_id);
                });
                jobs.insert(
                    job_id,
                    ActiveJob {
                        cancellation: cancel_tx,
                        task,
                    },
                );
            }
            Some(plugin_envelope::Payload::CancelJob(request)) => {
                let job_id = match validation::job_id(request.job_id.as_ref()) {
                    Ok(value) => value,
                    Err(error) => break Err(error),
                };
                let Some(job) = jobs.remove(job_id) else {
                    break Err(SdkError::Protocol(
                        "cancellation names no active job".to_owned(),
                    ));
                };
                stop_job(job).await;
                if let Err(error) = sender
                    .send(
                        Some(message_id),
                        trace,
                        plugin_envelope::Payload::CancelJobAcknowledged(
                            oll::CancelJobAcknowledged {
                                job_id: request.job_id,
                            },
                        ),
                    )
                    .await
                {
                    break Err(error);
                }
            }
            Some(plugin_envelope::Payload::Heartbeat(heartbeat)) => {
                if let Err(error) = sender
                    .send(
                        Some(message_id),
                        trace,
                        plugin_envelope::Payload::Heartbeat(heartbeat),
                    )
                    .await
                {
                    break Err(error);
                }
            }
            Some(plugin_envelope::Payload::Shutdown(request)) => {
                cancel_all(&mut jobs).await;
                if let Err(error) = sender
                    .send(
                        Some(message_id),
                        trace,
                        plugin_envelope::Payload::ShutdownAcknowledged(
                            oll::ShutdownAcknowledged {},
                        ),
                    )
                    .await
                {
                    break Err(error);
                }
                break Ok(request.grace_period_deadline);
            }
            Some(plugin_envelope::Payload::ProtocolError(error)) => {
                break Err(SdkError::Host(error));
            }
            Some(_) => {
                break Err(SdkError::Protocol(
                    "unexpected host-initiated message".to_owned(),
                ));
            }
            None => break Err(SdkError::Protocol("payload is required".to_owned())),
        }
    };
    cancel_all(&mut jobs).await;
    result
}

async fn wait_for_host_close(
    incoming: &mut tonic::Streaming<oll::PluginEnvelope>,
    deadline: prost_types::Timestamp,
) -> Result<(), SdkError> {
    let deadline = std::time::SystemTime::try_from(deadline)
        .map_err(|error| SdkError::Protocol(format!("invalid shutdown deadline: {error}")))?;
    let remaining = deadline
        .duration_since(std::time::SystemTime::now())
        .unwrap_or_default();
    match tokio::time::timeout(remaining, incoming.message()).await {
        Err(_) | Ok(Ok(None)) | Ok(Err(_)) => Ok(()),
        Ok(Ok(Some(_))) => Err(SdkError::Protocol(
            "host sent a message after ShutdownRequest".to_owned(),
        )),
    }
}

struct ActiveJob {
    cancellation: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

async fn stop_job(job: ActiveJob) {
    let _ = job.cancellation.send(true);
    job.task.abort();
    let _ = job.task.await;
}

async fn cancel_all(jobs: &mut HashMap<String, ActiveJob>) {
    let active = jobs.drain().map(|(_, job)| job).collect::<Vec<_>>();
    for job in &active {
        let _ = job.cancellation.send(true);
        job.task.abort();
    }
    for job in active {
        let _ = job.task.await;
    }
}

async fn receive(
    incoming: &mut tonic::Streaming<oll::PluginEnvelope>,
    identity: &RwLock<SessionIdentity>,
    last_message_id: u64,
    maximum_call_depth: u32,
    maximum_causal_depth: u32,
) -> Result<oll::PluginEnvelope, SdkError> {
    let envelope = incoming
        .message()
        .await
        .map_err(|error| SdkError::Transport(error.to_string()))?
        .ok_or_else(|| SdkError::Transport("host closed the plugin stream".to_owned()))?;
    let mut last = last_message_id;
    validate_envelope(
        &envelope,
        identity,
        &mut last,
        maximum_call_depth,
        maximum_causal_depth,
    )
    .await?;
    Ok(envelope)
}

async fn validate_envelope(
    envelope: &oll::PluginEnvelope,
    identity: &RwLock<SessionIdentity>,
    last_message_id: &mut u64,
    maximum_call_depth: u32,
    maximum_causal_depth: u32,
) -> Result<(), SdkError> {
    if envelope.message_id == 0 || envelope.message_id <= *last_message_id {
        return Err(SdkError::Protocol(
            "host message IDs must be nonzero and strictly increasing".to_owned(),
        ));
    }
    let identity = identity.read().await;
    if !identity.session_id.is_empty()
        && (envelope.session_id != identity.session_id
            || envelope.plugin_instance_id != identity.instance_id)
    {
        return Err(SdkError::Protocol(
            "host envelope belongs to another plugin instance".to_owned(),
        ));
    }
    validation::trace(envelope, maximum_call_depth, maximum_causal_depth)?;
    *last_message_id = envelope.message_id;
    Ok(())
}

fn parent_liveness() -> tokio::sync::oneshot::Receiver<()> {
    let (closed, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut input = std::io::stdin().lock();
        let mut buffer = [0_u8; 1];
        loop {
            match input.read(&mut buffer) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => break,
            }
        }
        let _ = closed.send(());
    });
    receiver
}
