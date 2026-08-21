mod artifact;
mod call;
mod client;

use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek};

use crate::protocol::{self as oll, plugin_envelope};

use super::{Cancellation, SdkError, value};
use artifact::{artifact_plan, validate_descriptor, validate_source};
use call::HostCallKind;
pub(super) use client::HostClient;

/// An artifact that oll has acknowledged as durably stored for this job.
#[derive(Clone, Debug)]
pub struct StoredArtifact {
    job_id: String,
    descriptor: oll::ArtifactDescriptor,
}

impl StoredArtifact {
    /// Returns the exact descriptor verified during the transfer.
    pub fn descriptor(&self) -> &oll::ArtifactDescriptor {
        &self.descriptor
    }
}

/// Successful terminal output from an action.
#[derive(Clone, Debug, Default)]
pub struct ActionResult {
    result: Option<oll::ConfigValue>,
    artifacts: Vec<StoredArtifact>,
}

impl ActionResult {
    /// Creates an empty successful result.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Creates a successful structured result after validating its durable value.
    pub fn value(value: oll::ConfigValue) -> Result<Self, SdkError> {
        value::validate_serializable(&value)?;
        Ok(Self {
            result: Some(value),
            artifacts: Vec::new(),
        })
    }

    /// Attaches an artifact previously returned by [`ActionContext::store_artifact`].
    pub fn with_artifact(mut self, artifact: StoredArtifact) -> Self {
        self.artifacts.push(artifact);
        self
    }

    /// Returns the optional structured result.
    pub fn result(&self) -> Option<&oll::ConfigValue> {
        self.result.as_ref()
    }

    /// Returns the artifacts acknowledged as stored for this result.
    pub fn artifacts(&self) -> &[StoredArtifact] {
        &self.artifacts
    }

    pub(super) fn into_wire(
        self,
        job_id: &str,
    ) -> Result<(Option<oll::ConfigValue>, Vec<oll::ArtifactDescriptor>), SdkError> {
        if let Some(result) = self.result.as_ref() {
            value::validate_serializable(result)?;
        }
        let mut artifact_ids = std::collections::HashSet::new();
        for artifact in &self.artifacts {
            if artifact.job_id != job_id {
                return Err(SdkError::InvalidArgument(
                    "job results may reference only artifacts stored by that job".to_owned(),
                ));
            }
            let artifact_id = artifact
                .descriptor
                .artifact_id
                .as_ref()
                .expect("stored artifacts have validated IDs");
            if !artifact_ids.insert(artifact_id.value.as_str()) {
                return Err(SdkError::InvalidArgument(
                    "job results must not reference the same artifact more than once".to_owned(),
                ));
            }
        }
        Ok((
            self.result,
            self.artifacts
                .into_iter()
                .map(|artifact| artifact.descriptor)
                .collect(),
        ))
    }
}

/// Per-job access to host calls, cancellation, trace context, and artifacts.
#[derive(Clone)]
pub struct ActionContext {
    job_id: String,
    deadline: Option<prost_types::Timestamp>,
    trace: oll::TraceContext,
    cancellation: Cancellation,
    parent_call_id: u64,
    host: HostClient,
}

impl ActionContext {
    pub(super) fn new(
        job_id: String,
        deadline: Option<prost_types::Timestamp>,
        trace: oll::TraceContext,
        cancellation: Cancellation,
        parent_call_id: u64,
        host: HostClient,
    ) -> Self {
        Self {
            job_id,
            deadline,
            trace,
            cancellation,
            parent_call_id,
            host,
        }
    }

    /// Returns the immutable host-assigned job ID.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Returns the optional absolute job deadline supplied by oll.
    pub fn deadline(&self) -> Option<&prost_types::Timestamp> {
        self.deadline.as_ref()
    }

    /// Returns the immutable root trace context of this job.
    pub fn trace(&self) -> &oll::TraceContext {
        &self.trace
    }

    /// Returns the cooperative cancellation token for this job.
    pub fn cancellation(&self) -> &Cancellation {
        &self.cancellation
    }

    /// Executes one raw document or configuration host call.
    pub async fn host_call(
        &self,
        call: oll::host_call_request::Call,
    ) -> Result<oll::HostCallResponse, SdkError> {
        self.cancellation.ensure_active()?;
        let kind = HostCallKind::validate(&call, self.host.session_id())?;
        let mut trace = self.trace.clone();
        trace.parent_call_id = Some(self.parent_call_id);
        trace.call_depth = trace
            .call_depth
            .checked_add(1)
            .ok_or_else(|| SdkError::Protocol("host-call depth overflowed".to_owned()))?;
        if trace.call_depth > self.host.maximum_call_depth() {
            return Err(SdkError::Protocol(
                "host call exceeds the negotiated call-depth limit".to_owned(),
            ));
        }
        let response = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(SdkError::Cancelled),
            response = self.host.call(trace, call) => response,
        }?;
        kind.validate_response(&response, self.host.session_id())?;
        Ok(response)
    }

    /// Reads the plugin's current live configuration at `path`.
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

    /// Invokes a configuration function owned by the current plugin session.
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

    /// Emits one validated structured plugin log record.
    pub async fn log(
        &self,
        level: oll::LogLevel,
        target: impl Into<String>,
        message: impl Into<String>,
        fields: HashMap<String, oll::ConfigValue>,
    ) -> Result<(), SdkError> {
        self.cancellation.ensure_active()?;
        if level == oll::LogLevel::Unspecified {
            return Err(SdkError::InvalidArgument(
                "log level must not be unspecified".to_owned(),
            ));
        }
        let target = target.into();
        if target.is_empty() {
            return Err(SdkError::InvalidArgument(
                "log target must not be empty".to_owned(),
            ));
        }
        for value in fields.values() {
            value::validate_serializable(value)?;
        }
        let send = self.host.sender().send(
            None,
            self.trace.clone(),
            plugin_envelope::Payload::Log(oll::LogRecord {
                timestamp: Some(system_timestamp()?),
                level: level as i32,
                target,
                message: message.into(),
                fields,
            }),
        );
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(SdkError::Cancelled),
            result = send => result.map(|_| ()),
        }
    }

    /// Validates and transfers an artifact from a seekable asynchronous source.
    ///
    /// The source is read once for size and SHA-256 validation, rewound to its
    /// original position, and then streamed in bounded chunks without buffering
    /// the complete artifact in memory.
    pub async fn store_artifact<R>(
        &self,
        descriptor: oll::ArtifactDescriptor,
        mut source: R,
    ) -> Result<StoredArtifact, SdkError>
    where
        R: AsyncRead + AsyncSeek + Unpin + Send,
    {
        self.cancellation.ensure_active()?;
        let artifact_id = validate_descriptor(&descriptor)?;
        validate_source(&descriptor, &mut source, &self.cancellation).await?;
        self.cancellation.ensure_active()?;

        let plan = artifact_plan(
            descriptor.size_bytes,
            self.host.maximum_artifact_chunk_bytes(),
        )?;
        let start = self
            .request(plugin_envelope::Payload::ArtifactStart(
                oll::ArtifactTransferStart {
                    job_id: Some(oll::PluginJobId {
                        value: self.job_id.clone(),
                    }),
                    artifact: Some(descriptor.clone()),
                    chunk_count: plan.chunk_count,
                },
            ))
            .await?;
        if !matches!(
            start,
            plugin_envelope::Payload::ArtifactAccepted(oll::ArtifactTransferAccepted {
                artifact_id: Some(ref accepted),
            }) if accepted.value == artifact_id.value
        ) {
            return Err(SdkError::Protocol(
                "host did not accept the same artifact transfer".to_owned(),
            ));
        }

        let mut remaining = descriptor.size_bytes;
        for index in 0..plan.chunk_count {
            self.cancellation.ensure_active()?;
            let length = usize::try_from(remaining.min(plan.chunk_bytes as u64))
                .expect("artifact chunk length is bounded by usize");
            let mut data = vec![0; length];
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(SdkError::Cancelled),
                read = source.read_exact(&mut data) => read,
            }
            .map_err(|source| SdkError::runtime("read validated artifact source", source))?;
            let send = self.host.sender().send(
                None,
                self.trace.clone(),
                plugin_envelope::Payload::ArtifactChunk(oll::ArtifactTransferChunk {
                    artifact_id: Some(artifact_id.clone()),
                    chunk_index: index,
                    data,
                }),
            );
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(SdkError::Cancelled),
                result = send => { result?; }
            }
            remaining -= length as u64;
        }

        match self
            .request(plugin_envelope::Payload::ArtifactComplete(
                oll::ArtifactTransferComplete {
                    artifact_id: Some(artifact_id.clone()),
                },
            ))
            .await?
        {
            plugin_envelope::Payload::ArtifactStored(stored)
                if stored.artifact_id.as_ref() == Some(&artifact_id) =>
            {
                Ok(StoredArtifact {
                    job_id: self.job_id.clone(),
                    descriptor,
                })
            }
            _ => Err(SdkError::Protocol(
                "host did not confirm the same artifact ID".to_owned(),
            )),
        }
    }

    async fn request(
        &self,
        payload: plugin_envelope::Payload,
    ) -> Result<plugin_envelope::Payload, SdkError> {
        self.cancellation.ensure_active()?;
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(SdkError::Cancelled),
            response = self.host.request(self.trace.clone(), payload) => response,
        }
    }
}

fn system_timestamp() -> Result<prost_types::Timestamp, SdkError> {
    let timestamp = prost_types::Timestamp::from(std::time::SystemTime::now());
    if value::valid_timestamp(&timestamp) {
        Ok(timestamp)
    } else {
        Err(SdkError::Environment(
            "system clock is outside the protobuf Timestamp domain".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    fn artifact(job_id: &str) -> StoredArtifact {
        StoredArtifact {
            job_id: job_id.to_owned(),
            descriptor: oll::ArtifactDescriptor {
                artifact_id: Some(oll::PluginArtifactId {
                    value: "00000000-0000-4000-8000-000000000001".to_owned(),
                }),
                file_name: "result.txt".to_owned(),
                media_type: "text/plain".to_owned(),
                size_bytes: 0,
                sha256: Sha256::digest([]).to_vec(),
            },
        }
    }

    #[test]
    fn result_artifacts_remain_bound_to_their_owning_job() {
        assert!(
            ActionResult::empty()
                .with_artifact(artifact("first-job"))
                .into_wire("second-job")
                .is_err()
        );
    }

    #[test]
    fn result_artifact_references_are_unique() {
        let artifact = artifact("job");
        assert!(
            ActionResult::empty()
                .with_artifact(artifact.clone())
                .with_artifact(artifact)
                .into_wire("job")
                .is_err()
        );
    }

    #[test]
    fn raw_configuration_calls_cannot_bypass_session_validation() {
        let call =
            oll::host_call_request::Call::InvokeConfigFunction(oll::InvokeConfigFunctionRequest {
                function: Some(oll::ConfigFunctionRef {
                    session_id: "another-session".to_owned(),
                    function_id: "function".to_owned(),
                }),
                arguments: Vec::new(),
            });
        assert!(HostCallKind::validate(&call, "session").is_err());
    }

    #[test]
    fn host_call_responses_must_match_the_request_kind_and_session() {
        let wrong_kind = oll::HostCallResponse {
            result: Some(oll::host_call_response::Result::ListDirectory(
                oll::ListDirectoryResponse::default(),
            )),
        };
        assert!(
            HostCallKind::ReadDocument
                .validate_response(&wrong_kind, "session")
                .is_err()
        );

        let foreign_function = oll::HostCallResponse {
            result: Some(oll::host_call_response::Result::GetConfig(
                oll::GetConfigResponse {
                    value: Some(oll::ConfigValue {
                        kind: Some(oll::config_value::Kind::FunctionValue(
                            oll::ConfigFunctionRef {
                                session_id: "another-session".to_owned(),
                                function_id: "function".to_owned(),
                            },
                        )),
                    }),
                },
            )),
        };
        assert!(
            HostCallKind::GetConfig
                .validate_response(&foreign_function, "session")
                .is_err()
        );
    }
}
