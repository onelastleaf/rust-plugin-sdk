use std::io::SeekFrom;

use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt as _};

use crate::protocol as oll;

use super::super::{Cancellation, SdkError, validation};

// A local memory/chunk target, not a total artifact or gRPC envelope limit.
// The negotiated host value remains the hard maximum for each chunk.
const ARTIFACT_IO_BUFFER_BYTES: usize = 64 * 1024;
const MAXIMUM_ARTIFACT_FILE_NAME_BYTES: usize = 191;
const SHA256_DIGEST_BYTES: usize = 32;

pub(super) struct ArtifactPlan {
    pub(super) chunk_bytes: usize,
    pub(super) chunk_count: u32,
}

pub(super) fn validate_descriptor(
    descriptor: &oll::ArtifactDescriptor,
) -> Result<oll::PluginArtifactId, SdkError> {
    let artifact_id = descriptor
        .artifact_id
        .as_ref()
        .filter(|id| validation::canonical_uuid_v4(&id.value))
        .ok_or_else(|| {
            SdkError::InvalidArgument("artifact ID must be a canonical UUID v4".to_owned())
        })?;
    let file_name = descriptor.file_name.as_bytes();
    if file_name.is_empty()
        || file_name.len() > MAXIMUM_ARTIFACT_FILE_NAME_BYTES
        || descriptor.file_name == "."
        || descriptor.file_name == ".."
        || file_name.contains(&0)
        || descriptor.file_name.contains('/')
    {
        return Err(SdkError::InvalidArgument(
            "artifact file name must be one safe UTF-8 basename of at most 191 bytes".to_owned(),
        ));
    }
    if descriptor.media_type.is_empty() || descriptor.sha256.len() != SHA256_DIGEST_BYTES {
        return Err(SdkError::InvalidArgument(
            "artifact media type and 32-byte SHA-256 are required".to_owned(),
        ));
    }
    Ok(artifact_id.clone())
}

pub(super) async fn validate_source<R>(
    descriptor: &oll::ArtifactDescriptor,
    source: &mut R,
    cancellation: &Cancellation,
) -> Result<(), SdkError>
where
    R: AsyncRead + AsyncSeek + Unpin,
{
    let start = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(SdkError::Cancelled),
        position = source.stream_position() => position,
    }
    .map_err(|source| SdkError::runtime("inspect artifact source position", source))?;
    let mut buffer = vec![0_u8; ARTIFACT_IO_BUFFER_BYTES];
    let mut size = 0_u64;
    let mut sha256 = Sha256::new();
    loop {
        let read = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(SdkError::Cancelled),
            read = source.read(&mut buffer) => read,
        }
        .map_err(|source| SdkError::runtime("read artifact source for validation", source))?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| SdkError::InvalidArgument("artifact size overflowed".to_owned()))?;
        sha256.update(&buffer[..read]);
    }
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(SdkError::Cancelled),
        position = source.seek(SeekFrom::Start(start)) => position,
    }
    .map_err(|source| SdkError::runtime("rewind validated artifact source", source))?;
    if size != descriptor.size_bytes || sha256.finalize().as_slice() != descriptor.sha256 {
        return Err(SdkError::InvalidArgument(
            "artifact size or SHA-256 does not match its source".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn artifact_plan(
    size_bytes: u64,
    maximum_chunk_bytes: u64,
) -> Result<ArtifactPlan, SdkError> {
    if maximum_chunk_bytes == 0 {
        return Err(SdkError::Protocol(
            "host advertised a zero artifact chunk limit".to_owned(),
        ));
    }
    let chunk_bytes = usize::try_from(maximum_chunk_bytes)
        .unwrap_or(usize::MAX)
        .min(ARTIFACT_IO_BUFFER_BYTES);
    let count = if size_bytes == 0 {
        0
    } else {
        size_bytes
            .checked_add(chunk_bytes as u64 - 1)
            .ok_or_else(|| {
                SdkError::InvalidArgument("artifact chunk count overflowed".to_owned())
            })?
            / chunk_bytes as u64
    };
    Ok(ArtifactPlan {
        chunk_bytes,
        chunk_count: u32::try_from(count)
            .map_err(|_| SdkError::InvalidArgument("artifact has too many chunks".to_owned()))?,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::ReadBuf;

    use super::*;

    struct RecordingSource {
        inner: std::io::Cursor<Vec<u8>>,
        maximum_read_buffer: usize,
    }

    impl AsyncRead for RecordingSource {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.maximum_read_buffer = self.maximum_read_buffer.max(buffer.remaining());
            Pin::new(&mut self.inner).poll_read(context, buffer)
        }
    }

    impl AsyncSeek for RecordingSource {
        fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
            Pin::new(&mut self.inner).start_seek(position)
        }

        fn poll_complete(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<std::io::Result<u64>> {
            Pin::new(&mut self.inner).poll_complete(context)
        }
    }

    #[test]
    fn empty_artifacts_use_the_canonical_zero_chunk_plan() {
        let plan = artifact_plan(0, 64 * 1024).unwrap();
        assert_eq!(plan.chunk_count, 0);
        assert_eq!(plan.chunk_bytes, 64 * 1024);
    }

    #[tokio::test]
    async fn source_validation_is_bounded_and_rewinds() {
        let bytes = vec![7_u8; ARTIFACT_IO_BUFFER_BYTES * 3 + 17];
        let descriptor = oll::ArtifactDescriptor {
            artifact_id: Some(oll::PluginArtifactId {
                value: "0f337c0c-51d6-44a9-a691-a31fce775ab1".to_owned(),
            }),
            file_name: "result.txt".to_owned(),
            media_type: "text/plain".to_owned(),
            size_bytes: bytes.len() as u64,
            sha256: Sha256::digest(&bytes).to_vec(),
        };
        let (_, cancellation) = super::super::super::cancellation::CancellationController::new();
        let mut source = RecordingSource {
            inner: std::io::Cursor::new(bytes.clone()),
            maximum_read_buffer: 0,
        };
        validate_source(&descriptor, &mut source, &cancellation)
            .await
            .unwrap();
        assert_eq!(source.maximum_read_buffer, ARTIFACT_IO_BUFFER_BYTES);
        let mut replayed = Vec::new();
        source.read_to_end(&mut replayed).await.unwrap();
        assert_eq!(replayed, bytes);
    }

    #[tokio::test]
    async fn empty_source_is_a_valid_zero_byte_artifact() {
        let descriptor = oll::ArtifactDescriptor {
            artifact_id: Some(oll::PluginArtifactId {
                value: "0f337c0c-51d6-44a9-a691-a31fce775ab1".to_owned(),
            }),
            file_name: "empty.txt".to_owned(),
            media_type: "text/plain".to_owned(),
            size_bytes: 0,
            sha256: Sha256::digest([]).to_vec(),
        };
        validate_descriptor(&descriptor).unwrap();
        let (_, cancellation) = super::super::super::cancellation::CancellationController::new();
        let mut source = std::io::Cursor::new(Vec::<u8>::new());
        validate_source(&descriptor, &mut source, &cancellation)
            .await
            .unwrap();
    }

    #[test]
    fn descriptors_reject_unsafe_file_names() {
        for file_name in ["", ".", "..", "../secret", "/tmp/secret", "nul\0name"] {
            let descriptor = oll::ArtifactDescriptor {
                artifact_id: Some(oll::PluginArtifactId {
                    value: "0f337c0c-51d6-44a9-a691-a31fce775ab1".to_owned(),
                }),
                file_name: file_name.to_owned(),
                media_type: "text/plain".to_owned(),
                size_bytes: 0,
                sha256: Sha256::digest([]).to_vec(),
            };
            assert!(validate_descriptor(&descriptor).is_err(), "{file_name:?}");
        }
    }
}
