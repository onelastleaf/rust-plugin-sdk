use std::collections::HashMap;

use crate::protocol as oll;

/// Errors reported while configuring or running a plugin.
#[derive(Debug)]
#[non_exhaustive]
pub enum SdkError {
    /// The process environment cannot establish the oll runtime contract.
    Environment(String),
    /// A plugin-supplied argument violates an SDK or protocol invariant.
    InvalidArgument(String),
    /// The gRPC stream or its ordered output channel failed.
    Transport(String),
    /// The peer or action attempted an invalid protocol transition.
    Protocol(String),
    /// oll returned a structured protocol error for a host capability call.
    Host(oll::ProtocolError),
    /// An action failed for a plugin-defined reason.
    Action(String),
    /// oll requested cancellation of the current action.
    Cancelled,
    /// A runtime boundary failed and retained its original error source.
    Runtime {
        /// The operation being performed when the error occurred.
        operation: &'static str,
        /// The original library or operating-system error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl SdkError {
    pub(super) fn runtime(
        operation: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Runtime {
            operation,
            source: Box::new(source),
        }
    }

    pub(super) fn protocol_error(&self) -> oll::ProtocolError {
        if let Self::Host(error) = self {
            return error.clone();
        }
        oll::ProtocolError {
            code: match self {
                Self::InvalidArgument(_) => oll::ErrorCode::InvalidArgument as i32,
                Self::Environment(_) | Self::Transport(_) | Self::Runtime { .. } => {
                    oll::ErrorCode::Unavailable as i32
                }
                Self::Protocol(_) => oll::ErrorCode::FailedPrecondition as i32,
                Self::Action(_) => oll::ErrorCode::Internal as i32,
                Self::Cancelled => oll::ErrorCode::Cancelled as i32,
                Self::Host(_) => unreachable!("host errors are returned unchanged"),
            },
            message: self.to_string(),
            retryable: matches!(
                self,
                Self::Environment(_) | Self::Transport(_) | Self::Runtime { .. }
            ),
            metadata: HashMap::new(),
            details: Vec::new(),
        }
    }
}

impl std::fmt::Display for SdkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Environment(message)
            | Self::InvalidArgument(message)
            | Self::Transport(message)
            | Self::Protocol(message)
            | Self::Action(message) => formatter.write_str(message),
            Self::Host(error) => formatter.write_str(&error.message),
            Self::Cancelled => formatter.write_str("action was cancelled"),
            Self::Runtime { operation, source } => write!(formatter, "{operation}: {source}"),
        }
    }
}

impl std::error::Error for SdkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_protocol_errors_keep_all_structured_fields() {
        let expected = oll::ProtocolError {
            code: oll::ErrorCode::Unavailable as i32,
            message: "retry later".to_owned(),
            retryable: true,
            metadata: HashMap::from([("retry-after".to_owned(), "1".to_owned())]),
            details: vec![prost_types::Any {
                type_url: "example.test/Detail".to_owned(),
                value: vec![1, 2, 3],
            }],
        };
        assert_eq!(SdkError::Host(expected.clone()).protocol_error(), expected);
    }

    #[test]
    fn runtime_failures_retain_their_source_and_retryability() {
        let error = SdkError::runtime(
            "read transport",
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset"),
        );
        assert!(std::error::Error::source(&error).is_some());
        let protocol = error.protocol_error();
        assert_eq!(protocol.code, oll::ErrorCode::Unavailable as i32);
        assert!(protocol.retryable);
    }
}
