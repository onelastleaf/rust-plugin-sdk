use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, watch};

use crate::protocol::{self as oll, plugin_envelope};

use super::{SdkError, sender::SessionSender, validation};

/// Successful terminal output from an action.
#[derive(Clone, Debug, Default)]
pub struct ActionResult {
    pub result: Option<oll::ConfigValue>,
    pub artifacts: Vec<oll::ArtifactDescriptor>,
}

/// Cooperative cancellation state visible to an action.
#[derive(Clone)]
pub struct Cancellation(pub(super) watch::Receiver<bool>);

impl Cancellation {
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    pub async fn cancelled(&mut self) {
        if !self.is_cancelled() {
            let _ = self.0.changed().await;
        }
    }
}

/// Per-job access to host calls, cancellation, trace context, and artifacts.
#[derive(Clone)]
pub struct ActionContext {
    pub job_id: String,
    pub deadline: Option<prost_types::Timestamp>,
    pub trace: oll::TraceContext,
    pub cancellation: Cancellation,
    pub(super) parent_call_id: u64,
    pub(super) host: HostClient,
}

impl ActionContext {
    pub async fn host_call(
        &self,
        call: oll::host_call_request::Call,
    ) -> Result<oll::HostCallResponse, SdkError> {
        let mut trace = self.trace.clone();
        trace.parent_call_id = Some(self.parent_call_id);
        trace.call_depth = trace
            .call_depth
            .checked_add(1)
            .ok_or_else(|| SdkError::Protocol("host-call depth overflowed".to_owned()))?;
        if trace.call_depth > self.host.maximum_call_depth {
            return Err(SdkError::Protocol(
                "host call exceeds the negotiated call-depth limit".to_owned(),
            ));
        }
        self.host.call(trace, call).await
    }

    pub async fn get_config(
        &self,
        path: Option<oll::ConfigPath>,
    ) -> Result<oll::GetConfigResponse, SdkError> {
        let response = self
            .host_call(oll::host_call_request::Call::GetConfig(
                oll::GetConfigRequest { path },
            ))
            .await?;
        match response.result {
            Some(oll::host_call_response::Result::GetConfig(value)) => Ok(value),
            _ => Err(SdkError::Protocol(
                "host returned another response kind for GetConfig".to_owned(),
            )),
        }
    }

    pub async fn invoke_config_function(
        &self,
        function: oll::ConfigFunctionRef,
        arguments: Vec<oll::ConfigValue>,
    ) -> Result<oll::InvokeConfigFunctionResponse, SdkError> {
        let response = self
            .host_call(oll::host_call_request::Call::InvokeConfigFunction(
                oll::InvokeConfigFunctionRequest {
                    function: Some(function),
                    arguments,
                },
            ))
            .await?;
        match response.result {
            Some(oll::host_call_response::Result::InvokeConfigFunction(value)) => Ok(value),
            _ => Err(SdkError::Protocol(
                "host returned another response kind for InvokeConfigFunction".to_owned(),
            )),
        }
    }

    pub async fn log(
        &self,
        level: oll::LogLevel,
        target: impl Into<String>,
        message: impl Into<String>,
        fields: HashMap<String, oll::ConfigValue>,
    ) -> Result<(), SdkError> {
        self.host
            .sender
            .send(
                None,
                self.trace.clone(),
                plugin_envelope::Payload::Log(oll::LogRecord {
                    timestamp: Some(system_timestamp()),
                    level: level as i32,
                    target: target.into(),
                    message: message.into(),
                    fields,
                }),
            )
            .await
            .map(|_| ())
    }

    pub async fn store_artifact(
        &self,
        descriptor: oll::ArtifactDescriptor,
        chunks: Vec<Vec<u8>>,
    ) -> Result<oll::ArtifactStored, SdkError> {
        let artifact_id =
            validate_artifact(&descriptor, &chunks, self.host.maximum_artifact_chunk_bytes)?;
        let start = self
            .host
            .request(
                self.trace.clone(),
                plugin_envelope::Payload::ArtifactStart(oll::ArtifactTransferStart {
                    job_id: Some(oll::PluginJobId {
                        value: self.job_id.clone(),
                    }),
                    artifact: Some(descriptor),
                    chunk_count: u32::try_from(chunks.len()).map_err(|_| {
                        SdkError::InvalidArgument("artifact has too many chunks".to_owned())
                    })?,
                }),
            )
            .await?;
        if !matches!(
            start,
            plugin_envelope::Payload::ArtifactAccepted(oll::ArtifactTransferAccepted {
                artifact_id: Some(ref accepted),
            }) if accepted.value == artifact_id.value
        ) {
            return Err(SdkError::Protocol(
                "host did not accept the artifact transfer".to_owned(),
            ));
        }
        for (index, data) in chunks.into_iter().enumerate() {
            self.host
                .sender
                .send(
                    None,
                    self.trace.clone(),
                    plugin_envelope::Payload::ArtifactChunk(oll::ArtifactTransferChunk {
                        artifact_id: Some(artifact_id.clone()),
                        chunk_index: u32::try_from(index).expect("chunk count already fits u32"),
                        data,
                    }),
                )
                .await?;
        }
        match self
            .host
            .request(
                self.trace.clone(),
                plugin_envelope::Payload::ArtifactComplete(oll::ArtifactTransferComplete {
                    artifact_id: Some(artifact_id.clone()),
                }),
            )
            .await?
        {
            plugin_envelope::Payload::ArtifactStored(stored)
                if stored.artifact_id.as_ref() == Some(&artifact_id) =>
            {
                Ok(stored)
            }
            _ => Err(SdkError::Protocol(
                "host did not confirm the same artifact ID".to_owned(),
            )),
        }
    }
}

#[derive(Clone)]
pub(super) struct HostClient {
    pub(super) sender: SessionSender,
    pending: Arc<Mutex<HashMap<u64, PendingResponse>>>,
    pub(super) maximum_artifact_chunk_bytes: u64,
    pub(super) maximum_call_depth: u32,
}

impl HostClient {
    pub(super) fn new(
        sender: SessionSender,
        maximum_artifact_chunk_bytes: u64,
        maximum_call_depth: u32,
    ) -> Self {
        Self {
            sender,
            pending: Arc::new(Mutex::new(HashMap::new())),
            maximum_artifact_chunk_bytes,
            maximum_call_depth,
        }
    }

    async fn call(
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
                Some(oll::host_call_response::Result::Error(error)) => Err(SdkError::Host(error)),
                _ => Ok(response),
            },
            plugin_envelope::Payload::ProtocolError(error) => Err(SdkError::Host(error)),
            _ => Err(SdkError::Protocol(
                "host call received another response kind".to_owned(),
            )),
        }
    }

    async fn request(
        &self,
        trace: oll::TraceContext,
        payload: plugin_envelope::Payload,
    ) -> Result<plugin_envelope::Payload, SdkError> {
        let (response, receiver) = oneshot::channel();
        let pending = self.pending.clone();
        let correlation_id = trace.correlation_id.clone();
        let (_, (receiver, _guard)) = self
            .sender
            .send_registered(None, trace, payload, move |message_id| {
                pending
                    .lock()
                    .map_err(|_| {
                        SdkError::Protocol("pending host-call state is poisoned".to_owned())
                    })?
                    .insert(
                        message_id,
                        PendingResponse {
                            correlation_id,
                            response,
                        },
                    );
                Ok((
                    receiver,
                    PendingGuard {
                        message_id,
                        pending,
                    },
                ))
            })
            .await?;
        receiver.await.map_err(|_| {
            SdkError::Transport("plugin session ended before host response".to_owned())
        })
    }

    pub(super) fn route(
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
        if waiter.correlation_id != trace.correlation_id {
            return Err(SdkError::Protocol(
                "response correlation context differs".to_owned(),
            ));
        }
        let _ = waiter.response.send(payload);
        Ok(())
    }
}

struct PendingResponse {
    correlation_id: String,
    response: oneshot::Sender<plugin_envelope::Payload>,
}

struct PendingGuard {
    message_id: u64,
    pending: Arc<Mutex<HashMap<u64, PendingResponse>>>,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
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

fn validate_artifact(
    descriptor: &oll::ArtifactDescriptor,
    chunks: &[Vec<u8>],
    maximum_chunk_bytes: u64,
) -> Result<oll::PluginArtifactId, SdkError> {
    if chunks.is_empty() || chunks.iter().any(|chunk| chunk.is_empty()) {
        return Err(SdkError::InvalidArgument(
            "artifact chunks must be nonempty".to_owned(),
        ));
    }
    if chunks
        .iter()
        .any(|chunk| chunk.len() as u64 > maximum_chunk_bytes)
    {
        return Err(SdkError::InvalidArgument(
            "artifact chunk exceeds the negotiated limit".to_owned(),
        ));
    }
    let artifact_id = descriptor
        .artifact_id
        .as_ref()
        .filter(|id| validation::canonical_uuid_v4(&id.value))
        .ok_or_else(|| {
            SdkError::InvalidArgument("artifact ID must be a canonical UUID v4".to_owned())
        })?;
    if descriptor.file_name.is_empty()
        || descriptor.media_type.is_empty()
        || descriptor.sha256.len() != 32
    {
        return Err(SdkError::InvalidArgument(
            "artifact descriptor is incomplete".to_owned(),
        ));
    }
    let mut size = 0_u64;
    let mut sha256 = Sha256::new();
    for chunk in chunks {
        size = size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| SdkError::InvalidArgument("artifact size overflowed".to_owned()))?;
        sha256.update(chunk);
    }
    if size != descriptor.size_bytes || sha256.finalize().as_slice() != descriptor.sha256 {
        return Err(SdkError::InvalidArgument(
            "artifact size or SHA-256 does not match its bytes".to_owned(),
        ));
    }
    Ok(artifact_id.clone())
}

fn system_timestamp() -> prost_types::Timestamp {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(value) => prost_types::Timestamp {
            seconds: i64::try_from(value.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(value.subsec_nanos()).expect("nanoseconds fit i32"),
        },
        Err(_) => prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_metadata_must_match_bytes() {
        let descriptor = oll::ArtifactDescriptor {
            artifact_id: Some(oll::PluginArtifactId {
                value: "0f337c0c-51d6-44a9-a691-a31fce775ab1".to_owned(),
            }),
            file_name: "result.txt".to_owned(),
            media_type: "text/plain".to_owned(),
            size_bytes: 3,
            sha256: Sha256::digest(b"abc").to_vec(),
        };
        assert!(validate_artifact(&descriptor, &[b"abc".to_vec()], 3).is_ok());
        assert!(validate_artifact(&descriptor, &[b"abcd".to_vec()], 4).is_err());
        assert!(validate_artifact(&descriptor, &[b"abc".to_vec()], 2).is_err());
    }
}
