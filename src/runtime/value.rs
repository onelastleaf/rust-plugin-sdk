use crate::protocol as oll;

use super::SdkError;

// ConfigValue depth is zero-based. Map values consume the tightest share of
// prost's default decode-recursion budget because each level includes a map
// entry message, so 33 is the common safe wire limit for every value shape.
const MAXIMUM_VALUE_DEPTH: usize = 33;
const PROTOBUF_TIMESTAMP_MIN_SECONDS: i64 = -62_135_596_800;
const PROTOBUF_TIMESTAMP_MAX_SECONDS: i64 = 253_402_300_799;
const PROTOBUF_DURATION_MAX_SECONDS: i64 = 315_576_000_000;
const PROTOBUF_MAX_NANOSECONDS: i32 = 999_999_999;

pub(super) fn valid_timestamp(value: &prost_types::Timestamp) -> bool {
    (PROTOBUF_TIMESTAMP_MIN_SECONDS..=PROTOBUF_TIMESTAMP_MAX_SECONDS).contains(&value.seconds)
        && (0..=PROTOBUF_MAX_NANOSECONDS).contains(&value.nanos)
}

fn valid_duration(value: &prost_types::Duration) -> bool {
    (-PROTOBUF_DURATION_MAX_SECONDS..=PROTOBUF_DURATION_MAX_SECONDS).contains(&value.seconds)
        && (-PROTOBUF_MAX_NANOSECONDS..=PROTOBUF_MAX_NANOSECONDS).contains(&value.nanos)
        && !(value.seconds > 0 && value.nanos < 0)
        && !(value.seconds < 0 && value.nanos > 0)
}

pub(super) fn validate_serializable(value: &oll::ConfigValue) -> Result<(), SdkError> {
    validate(value, 0, None)
}

pub(super) fn validate_session_values(
    values: &[oll::ConfigValue],
    session_id: &str,
) -> Result<(), SdkError> {
    for value in values {
        validate(value, 0, Some(session_id))?;
    }
    Ok(())
}

fn validate(
    value: &oll::ConfigValue,
    depth: usize,
    function_session: Option<&str>,
) -> Result<(), SdkError> {
    use oll::config_value::Kind;

    if depth > MAXIMUM_VALUE_DEPTH {
        return Err(invalid("ConfigValue nesting exceeds the supported limit"));
    }
    match value.kind.as_ref() {
        Some(Kind::NullValue(value)) if *value == prost_types::NullValue::NullValue as i32 => {
            Ok(())
        }
        Some(Kind::NullValue(_)) => Err(invalid("ConfigValue contains an unknown null value")),
        Some(Kind::BoolValue(_))
        | Some(Kind::IntegerValue(_))
        | Some(Kind::StringValue(_))
        | Some(Kind::BytesValue(_)) => Ok(()),
        Some(Kind::NumberValue(value)) if value.is_finite() => Ok(()),
        Some(Kind::NumberValue(_)) => Err(invalid("ConfigValue numbers must be finite")),
        Some(Kind::ListValue(list)) => {
            for value in &list.values {
                validate(value, depth + 1, function_session)?;
            }
            Ok(())
        }
        Some(Kind::MapValue(map)) => {
            for value in map.entries.values() {
                validate(value, depth + 1, function_session)?;
            }
            Ok(())
        }
        Some(Kind::FunctionValue(function)) => match function_session {
            Some(session_id)
                if function.session_id == session_id && !function.function_id.is_empty() =>
            {
                Ok(())
            }
            Some(session_id) if function.session_id != session_id => Err(SdkError::Protocol(
                "configuration function belongs to another plugin session".to_owned(),
            )),
            Some(_) => Err(invalid("configuration function ID must not be empty")),
            None => Err(invalid(
                "session-scoped configuration functions cannot be stored or logged",
            )),
        },
        Some(Kind::TimestampValue(value)) if valid_timestamp(value) => Ok(()),
        Some(Kind::TimestampValue(_)) => Err(invalid(
            "ConfigValue timestamp is outside the protobuf Timestamp domain",
        )),
        Some(Kind::DurationValue(value)) if valid_duration(value) => Ok(()),
        Some(Kind::DurationValue(_)) => Err(invalid(
            "ConfigValue duration is outside the protobuf Duration domain",
        )),
        None => Err(invalid("ConfigValue kind is required")),
    }
}

fn invalid(message: &str) -> SdkError {
    SdkError::InvalidArgument(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_values_reject_non_finite_numbers_and_functions() {
        let number = oll::ConfigValue {
            kind: Some(oll::config_value::Kind::NumberValue(f64::NAN)),
        };
        let function = oll::ConfigValue {
            kind: Some(oll::config_value::Kind::FunctionValue(
                oll::ConfigFunctionRef {
                    session_id: "session".to_owned(),
                    function_id: "function".to_owned(),
                },
            )),
        };
        assert!(validate_serializable(&number).is_err());
        assert!(validate_serializable(&function).is_err());
        assert!(validate_session_values(&[function], "session").is_ok());
    }

    #[test]
    fn value_depth_matches_the_canonical_limit() {
        let mut accepted = oll::ConfigValue {
            kind: Some(oll::config_value::Kind::NullValue(
                prost_types::NullValue::NullValue as i32,
            )),
        };
        for _ in 0..MAXIMUM_VALUE_DEPTH {
            accepted = oll::ConfigValue {
                kind: Some(oll::config_value::Kind::ListValue(oll::ConfigList {
                    values: vec![accepted],
                })),
            };
        }
        let rejected = oll::ConfigValue {
            kind: Some(oll::config_value::Kind::ListValue(oll::ConfigList {
                values: vec![accepted.clone()],
            })),
        };
        assert!(validate_serializable(&accepted).is_ok());
        assert!(validate_serializable(&rejected).is_err());
    }

    #[test]
    fn values_reject_invalid_protobuf_well_known_types() {
        let timestamp = oll::ConfigValue {
            kind: Some(oll::config_value::Kind::TimestampValue(
                prost_types::Timestamp {
                    seconds: PROTOBUF_TIMESTAMP_MAX_SECONDS + 1,
                    nanos: 0,
                },
            )),
        };
        let duration = oll::ConfigValue {
            kind: Some(oll::config_value::Kind::DurationValue(
                prost_types::Duration {
                    seconds: 1,
                    nanos: -1,
                },
            )),
        };
        assert!(validate_serializable(&timestamp).is_err());
        assert!(validate_serializable(&duration).is_err());
    }

    #[test]
    fn session_values_reject_foreign_function_handles() {
        let function = oll::ConfigValue {
            kind: Some(oll::config_value::Kind::FunctionValue(
                oll::ConfigFunctionRef {
                    session_id: "another-session".to_owned(),
                    function_id: "function".to_owned(),
                },
            )),
        };
        assert!(validate_session_values(&[function], "session").is_err());
    }
}
