use std::collections::HashMap;

use onelastleaf_plugin_sdk::{
    ActionResult, Plugin, SdkError,
    protocol::{
        self as oll, config_value, document_snapshot, host_call_request, host_call_response,
    },
};
use sha2::{Digest, Sha256};

#[tokio::main]
async fn main() -> Result<(), SdkError> {
    Plugin::builder("org.onelastleaf.conformance", "0.1.0")
        .action("echo", "Echo arguments", |_, arguments| async move {
            ActionResult::value(string_value(arguments.join(" ")))
        })?
        .action("wait", "Wait for cancellation", |context, _| async move {
            context.cancellation().cancelled().await;
            Ok(ActionResult::empty())
        })?
        .action(
            "host",
            "Exercise host capabilities",
            |context, _| async move {
                let configured = context.get_config(None).await?;
                let function = match configured.value.and_then(|value| value.kind) {
                    Some(config_value::Kind::FunctionValue(function)) => function,
                    _ => return Err(SdkError::Protocol("GetConfig omitted function".to_owned())),
                };
                let invoked = context
                    .invoke_config_function(function, vec![string_value("config")])
                    .await?;
                let function_value =
                    invoked
                        .results
                        .first()
                        .and_then(config_string)
                        .ok_or_else(|| {
                            SdkError::Protocol("function result is not a string".to_owned())
                        })?;
                let document = context
                    .host_call(host_call_request::Call::ReadDocument(
                        oll::ReadDocumentRequest {
                            path: Some(oll::DocumentPath {
                                value: "/conformance.md".to_owned(),
                            }),
                            projection: oll::DocumentProjection::Content as i32,
                        },
                    ))
                    .await?;
                let content = match document.result {
                    Some(host_call_response::Result::ReadDocument(response)) => response
                        .document
                        .and_then(|snapshot| snapshot.representation)
                        .and_then(|value| match value {
                            document_snapshot::Representation::Content(value) => Some(value),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            SdkError::Protocol("document response omitted text".to_owned())
                        })?,
                    _ => {
                        return Err(SdkError::Protocol(
                            "unexpected document response".to_owned(),
                        ));
                    }
                };
                context
                    .log(
                        oll::LogLevel::Info,
                        "conformance",
                        "host action complete",
                        HashMap::new(),
                    )
                    .await?;
                ActionResult::value(string_value(format!("{function_value}|{content}")))
            },
        )?
        .action(
            "artifact",
            "Exercise artifact transfer",
            |context, _| async move {
                let descriptor = oll::ArtifactDescriptor {
                    artifact_id: Some(oll::PluginArtifactId {
                        value: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_owned(),
                    }),
                    file_name: "conformance.txt".to_owned(),
                    media_type: "text/plain".to_owned(),
                    size_bytes: 16,
                    sha256: Sha256::digest(b"artifact payload").to_vec(),
                };
                let stored = context
                    .store_artifact(
                        descriptor,
                        std::io::Cursor::new(b"artifact payload".to_vec()),
                    )
                    .await?;
                Ok(ActionResult::value(string_value("artifact"))?.with_artifact(stored))
            },
        )?
        .build()?
        .run()
        .await
}

fn string_value(value: impl Into<String>) -> oll::ConfigValue {
    oll::ConfigValue {
        kind: Some(config_value::Kind::StringValue(value.into())),
    }
}

fn config_string(value: &oll::ConfigValue) -> Option<&str> {
    match value.kind.as_ref()? {
        config_value::Kind::StringValue(value) => Some(value),
        _ => None,
    }
}
