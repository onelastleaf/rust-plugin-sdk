use crate::protocol as oll;

use super::super::{SdkError, validation, value};

#[derive(Clone, Copy)]
pub(super) enum HostCallKind {
    ReadDocument,
    ListDirectory,
    GetDirectoryTree,
    ReadCrdt,
    CommitDocuments,
    GetConfig,
    InvokeConfigFunction,
}

impl HostCallKind {
    pub(super) fn validate(
        call: &oll::host_call_request::Call,
        session_id: &str,
    ) -> Result<Self, SdkError> {
        use oll::host_call_request::Call;

        match call {
            Call::ReadDocument(_) => Ok(Self::ReadDocument),
            Call::ListDirectory(_) => Ok(Self::ListDirectory),
            Call::GetDirectoryTree(_) => Ok(Self::GetDirectoryTree),
            Call::ReadCrdt(_) => Ok(Self::ReadCrdt),
            Call::CommitDocuments(_) => Ok(Self::CommitDocuments),
            Call::GetConfig(request) => {
                if request
                    .path
                    .as_ref()
                    .is_some_and(|path| path.segments.iter().any(|segment| segment.kind.is_none()))
                {
                    return Err(SdkError::InvalidArgument(
                        "configuration path segments must specify a key or index".to_owned(),
                    ));
                }
                Ok(Self::GetConfig)
            }
            Call::InvokeConfigFunction(request) => {
                let function = request.function.as_ref().ok_or_else(|| {
                    SdkError::InvalidArgument(
                        "configuration function reference is required".to_owned(),
                    )
                })?;
                if function.session_id != session_id || function.function_id.is_empty() {
                    return Err(SdkError::InvalidArgument(
                        "configuration function must belong to the active session".to_owned(),
                    ));
                }
                value::validate_session_values(&request.arguments, session_id)?;
                Ok(Self::InvokeConfigFunction)
            }
        }
    }

    pub(super) fn validate_response(
        self,
        response: &oll::HostCallResponse,
        session_id: &str,
    ) -> Result<(), SdkError> {
        use oll::host_call_response::Result;

        match (self, response.result.as_ref()) {
            (Self::ReadDocument, Some(Result::ReadDocument(_)))
            | (Self::ListDirectory, Some(Result::ListDirectory(_)))
            | (Self::GetDirectoryTree, Some(Result::GetDirectoryTree(_)))
            | (Self::ReadCrdt, Some(Result::ReadCrdt(_)))
            | (Self::CommitDocuments, Some(Result::CommitDocuments(_))) => Ok(()),
            (Self::GetConfig, Some(Result::GetConfig(response))) => {
                if let Some(configured) = response.value.as_ref() {
                    value::validate_session_values(std::slice::from_ref(configured), session_id)?;
                }
                Ok(())
            }
            (Self::InvokeConfigFunction, Some(Result::InvokeConfigFunction(response))) => {
                value::validate_session_values(&response.results, session_id)
            }
            (_, Some(Result::Error(error))) => {
                // HostClient converts this branch into SdkError::Host first;
                // retaining it here keeps this validator total.
                validation::protocol_error(error)
            }
            (_, Some(_)) => Err(SdkError::Protocol(
                "host returned another response kind for HostCallRequest".to_owned(),
            )),
            (_, None) => Err(SdkError::Protocol(
                "HostCallResponse result is required".to_owned(),
            )),
        }
    }
}
