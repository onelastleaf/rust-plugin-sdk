use std::collections::HashMap;

use crate::protocol as oll;

#[derive(Debug)]
pub enum SdkError {
    Environment(String),
    InvalidArgument(String),
    Transport(String),
    Protocol(String),
    Host(oll::ProtocolError),
    Action(String),
}

impl SdkError {
    pub(super) fn protocol_error(&self) -> oll::ProtocolError {
        oll::ProtocolError {
            code: match self {
                Self::InvalidArgument(_) => oll::ErrorCode::InvalidArgument as i32,
                Self::Host(error) => error.code,
                Self::Environment(_) | Self::Transport(_) => oll::ErrorCode::Unavailable as i32,
                Self::Protocol(_) => oll::ErrorCode::FailedPrecondition as i32,
                Self::Action(_) => oll::ErrorCode::Internal as i32,
            },
            message: self.to_string(),
            retryable: false,
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
        }
    }
}

impl std::error::Error for SdkError {}
