use std::net::IpAddr;

use crate::{
    PROTOCOL_SCHEMA_SHA256,
    protocol::{self as oll, PluginEnvelope},
};

use super::SdkError;

pub(super) fn endpoint(value: &str) -> Result<String, SdkError> {
    let parsed = url::Url::parse(value)
        .map_err(|error| SdkError::Environment(format!("invalid OLL_PLUGIN_ENDPOINT: {error}")))?;
    let loopback = match parsed.host() {
        Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => IpAddr::V4(address).is_loopback(),
        Some(url::Host::Ipv6(address)) => IpAddr::V6(address).is_loopback(),
        None => false,
    };
    if parsed.scheme() != "http"
        || !loopback
        || parsed.port().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(SdkError::Environment(
            "OLL_PLUGIN_ENDPOINT must be an http loopback URL with an explicit port".to_owned(),
        ));
    }
    Ok(value.to_owned())
}

pub(super) fn host_hello(plugin_id: &str, hello: &oll::HostHello) -> Result<(), SdkError> {
    if hello.node.is_none()
        || hello.protocol_schema_sha256.as_slice() != PROTOCOL_SCHEMA_SHA256
        || hello.plugin_id.as_ref().map(|value| value.value.as_str()) != Some(plugin_id)
        || hello
            .plugin_name
            .as_ref()
            .is_none_or(|value| value.value.is_empty())
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

pub(super) fn canonical_uuid_v4(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| {
        id.get_version() == Some(uuid::Version::Random) && id.hyphenated().to_string() == value
    })
}

pub(super) fn plugin_id(value: &str) -> Result<(), SdkError> {
    let labels = value.split('.').collect::<Vec<_>>();
    if value.len() > 191 || labels.len() < 2 || !labels.into_iter().all(valid_dns_label) {
        return Err(SdkError::InvalidArgument(
            "plugin ID must be a lower-case dotted DNS name".to_owned(),
        ));
    }
    Ok(())
}

fn valid_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
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
    }

    #[test]
    fn uuid_requires_canonical_version_four() {
        assert!(canonical_uuid_v4("0f337c0c-51d6-44a9-a691-a31fce775ab1"));
        assert!(!canonical_uuid_v4("0f337c0c-51d6-14a9-a691-a31fce775ab1"));
        assert!(!canonical_uuid_v4("0F337C0C-51D6-44A9-A691-A31FCE775AB1"));
    }
}
