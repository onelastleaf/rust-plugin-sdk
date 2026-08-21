use std::net::IpAddr;

use crate::protocol::{self as oll, PluginEnvelope};

use super::SdkError;

const MAXIMUM_PLUGIN_ID_BYTES: usize = 191;
const MAXIMUM_DNS_LABEL_BYTES: usize = 63;

pub(super) fn endpoint(value: &str) -> Result<http::Uri, SdkError> {
    let parsed = value
        .parse::<http::Uri>()
        .map_err(|error| SdkError::runtime("parse OLL_PLUGIN_ENDPOINT", error))?;
    let authority = parsed.authority();
    let loopback = authority.is_some_and(|authority| {
        let host = authority.host();
        let host = host
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .unwrap_or(host);
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    let explicit_port = authority
        .and_then(http::uri::Authority::port_u16)
        .is_some_and(|port| port != 0);
    let no_user_information = authority.is_some_and(|value| !value.as_str().contains('@'));
    let root_path_without_query = parsed.path() == "/" && parsed.query().is_none();
    if parsed.scheme_str() != Some("http")
        || !loopback
        || !explicit_port
        || !no_user_information
        || !root_path_without_query
    {
        return Err(SdkError::Environment(
            "OLL_PLUGIN_ENDPOINT must be an http loopback URL with an explicit nonzero port"
                .to_owned(),
        ));
    };
    Ok(parsed)
}

pub(super) fn host_hello(plugin_id: &str, hello: &oll::HostHello) -> Result<(), SdkError> {
    let valid_node = hello.node.as_ref().is_some_and(|node| {
        node.node_id
            .as_ref()
            .is_some_and(|id| canonical_uuid_v4(&id.value))
            && node
                .node_name
                .as_ref()
                .is_some_and(|name| valid_dns_label(&name.value))
    });
    let valid_plugin = hello
        .plugin_id
        .as_ref()
        .is_some_and(|value| value.value == plugin_id)
        && hello
            .plugin_name
            .as_ref()
            .is_some_and(|value| valid_dns_label(&value.value));
    if !valid_node
        || !valid_plugin
        || hello.maximum_call_depth == 0
        || hello.maximum_causal_depth == 0
        || hello.maximum_artifact_chunk_bytes == 0
    {
        return Err(SdkError::Protocol(
            "HostHello does not describe the expected plugin instance".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn trace(
    envelope: &PluginEnvelope,
    maximum_call_depth: u32,
    maximum_causal_depth: u32,
) -> Result<&oll::TraceContext, SdkError> {
    let trace = envelope
        .trace
        .as_ref()
        .filter(|trace| !trace.correlation_id.is_empty())
        .ok_or_else(|| SdkError::Protocol("host omitted correlation context".to_owned()))?;
    if trace.call_depth > maximum_call_depth || trace.causal_depth > maximum_causal_depth {
        return Err(SdkError::Protocol(
            "host envelope exceeds a negotiated trace depth limit".to_owned(),
        ));
    }
    Ok(trace)
}

pub(super) fn job_id(job_id: Option<&oll::PluginJobId>) -> Result<&str, SdkError> {
    let value = job_id
        .map(|value| value.value.as_str())
        .ok_or_else(|| SdkError::Protocol("job ID is required".to_owned()))?;
    canonical_uuid_v4(value)
        .then_some(value)
        .ok_or_else(|| SdkError::Protocol("job ID must be a canonical UUID v4".to_owned()))
}

pub(super) fn timestamp<'a>(
    value: Option<&'a prost_types::Timestamp>,
    field: &str,
) -> Result<&'a prost_types::Timestamp, SdkError> {
    value
        .filter(|value| super::value::valid_timestamp(value))
        .ok_or_else(|| SdkError::Protocol(format!("{field} must be a valid protobuf Timestamp")))
}

pub(super) fn optional_timestamp(
    value: Option<&prost_types::Timestamp>,
    field: &str,
) -> Result<(), SdkError> {
    match value {
        Some(value) => timestamp(Some(value), field).map(|_| ()),
        None => Ok(()),
    }
}

pub(super) fn cancellation_reason(value: i32) -> Result<(), SdkError> {
    // The reason does not alter plugin-side cancellation behavior, so future
    // nonzero enum values retain safe semantics for this SDK.
    if value == oll::JobCancellationReason::Unspecified as i32 {
        Err(SdkError::Protocol(
            "CancelJobRequest reason must not be unspecified".to_owned(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn shutdown(request: &oll::ShutdownRequest) -> Result<prost_types::Timestamp, SdkError> {
    if request.reason.is_empty() {
        return Err(SdkError::Protocol(
            "ShutdownRequest reason must not be empty".to_owned(),
        ));
    }
    let deadline = timestamp(
        request.grace_period_deadline.as_ref(),
        "ShutdownRequest grace-period deadline",
    )?;
    Ok(*deadline)
}

pub(super) fn protocol_error(error: &oll::ProtocolError) -> Result<(), SdkError> {
    // Preserve future nonzero enum values as structured host errors. Their
    // operation-level meaning is still unambiguously an error.
    if error.code != oll::ErrorCode::Unspecified as i32 && !error.message.is_empty() {
        Ok(())
    } else {
        Err(SdkError::Protocol(
            "ProtocolError code and message must be specified".to_owned(),
        ))
    }
}

pub(super) fn canonical_uuid_v4(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| {
        id.get_version() == Some(uuid::Version::Random) && id.hyphenated().to_string() == value
    })
}

pub(super) fn plugin_id(value: &str) -> Result<(), SdkError> {
    let labels = value.split('.').collect::<Vec<_>>();
    if value.len() > MAXIMUM_PLUGIN_ID_BYTES
        || labels.len() < 2
        || !labels.into_iter().all(valid_dns_label)
    {
        return Err(SdkError::InvalidArgument(
            "plugin ID must be a lower-case dotted DNS name".to_owned(),
        ));
    }
    Ok(())
}

fn valid_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAXIMUM_DNS_LABEL_BYTES
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && (bytes[bytes.len() - 1].is_ascii_lowercase() || bytes[bytes.len() - 1].is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_is_private_and_explicit() {
        assert!(endpoint("http://127.0.0.1:1234").is_ok());
        assert!(endpoint("http://[::1]:1234").is_ok());
        assert!(endpoint("http://localhost:1234").is_ok());
        assert!(endpoint("http://example.com:1234").is_err());
        assert!(endpoint("https://127.0.0.1:1234").is_err());
        assert!(endpoint("http://127.0.0.1").is_err());
        assert!(endpoint("http://127.0.0.1:0").is_err());
        assert!(endpoint("http://user@127.0.0.1:1234").is_err());
    }

    #[test]
    fn uuid_requires_canonical_version_four() {
        assert!(canonical_uuid_v4("0f337c0c-51d6-44a9-a691-a31fce775ab1"));
        assert!(!canonical_uuid_v4("0f337c0c-51d6-14a9-a691-a31fce775ab1"));
        assert!(!canonical_uuid_v4("0F337C0C-51D6-44A9-A691-A31FCE775AB1"));
    }

    #[test]
    fn host_hello_requires_complete_typed_identity() {
        let valid = oll::HostHello {
            node: Some(oll::NodeIdentity {
                node_id: Some(oll::NodeId {
                    value: "0f337c0c-51d6-44a9-a691-a31fce775ab1".to_owned(),
                }),
                node_name: Some(oll::NodeName {
                    value: "primary-node".to_owned(),
                }),
            }),
            maximum_call_depth: 10,
            maximum_causal_depth: 10,
            maximum_artifact_chunk_bytes: 64 * 1024,
            plugin_id: Some(oll::PluginId {
                value: "org.example.echo".to_owned(),
            }),
            plugin_name: Some(oll::PluginName {
                value: "echo".to_owned(),
            }),
        };
        assert!(host_hello("org.example.echo", &valid).is_ok());

        let mut invalid = valid.clone();
        invalid
            .node
            .as_mut()
            .unwrap()
            .node_name
            .as_mut()
            .unwrap()
            .value = "Not DNS".to_owned();
        assert!(host_hello("org.example.echo", &invalid).is_err());

        let mut invalid = valid;
        invalid
            .node
            .as_mut()
            .unwrap()
            .node_id
            .as_mut()
            .unwrap()
            .value = "0f337c0c-51d6-14a9-a691-a31fce775ab1".to_owned();
        assert!(host_hello("org.example.echo", &invalid).is_err());
    }

    #[test]
    fn cancellation_and_shutdown_reject_unspecified_values() {
        assert!(cancellation_reason(oll::JobCancellationReason::UserRequest as i32).is_ok());
        assert!(cancellation_reason(oll::JobCancellationReason::Unspecified as i32).is_err());
        assert!(cancellation_reason(999).is_ok());
        assert!(
            shutdown(&oll::ShutdownRequest {
                reason: String::new(),
                grace_period_deadline: None,
            })
            .is_err()
        );
        assert!(
            protocol_error(&oll::ProtocolError {
                code: 999,
                message: "future error".to_owned(),
                retryable: false,
                metadata: Default::default(),
                details: Vec::new(),
            })
            .is_ok()
        );
    }

    #[test]
    fn timestamps_and_protocol_errors_must_be_in_their_domains() {
        assert!(
            timestamp(
                Some(&prost_types::Timestamp {
                    seconds: 0,
                    nanos: 1_000_000_000,
                }),
                "timestamp",
            )
            .is_err()
        );
        assert!(
            protocol_error(&oll::ProtocolError {
                code: oll::ErrorCode::Unspecified as i32,
                message: "missing code".to_owned(),
                retryable: false,
                metadata: Default::default(),
                details: Vec::new(),
            })
            .is_err()
        );
    }
}
