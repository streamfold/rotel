use crate::bounded_channel::BoundedReceiver;
use crate::exporters::file::{Result, TypedFileExporter};
use crate::topology::payload::{Ack, Message};
use chrono::Utc;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Generic event loop for processing telemetry data
pub async fn run_generic_loop<E, Resource>(
    exporter: std::sync::Arc<E>,
    output_dir: std::path::PathBuf,
    mut receiver: BoundedReceiver<Vec<Message<Resource>>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
    telemetry_type: &str,
) -> Result<()>
where
    E: TypedFileExporter<Resource> + Send + Sync,
    Resource: Clone + Send,
{
    let file_ext = exporter.file_extension();
    let mut buffer: Vec<E::Data> = Vec::new();
    let mut flush_timer = tokio::time::interval(flush_interval);

    loop {
        tokio::select! {
            result = receiver.next() => {
                match result {
                    Some(messages) => {
                        // Process incoming telemetry data and collect metadata for acknowledgment
                        let mut metadata_to_ack = Vec::new();

                        for message in messages {
                            if let Some(metadata) = message.metadata {
                                metadata_to_ack.push(metadata);
                            }

                            for resource in &message.payload {
                                let mut converted_data = exporter.convert(resource)?;
                                buffer.append(&mut converted_data);
                            }
                        }

                        // If we successfully processed the messages, acknowledge them immediately
                        for metadata in metadata_to_ack {
                            if let Err(e) = metadata.ack().await {
                                tracing::warn!("Failed to acknowledge file message: {:?}", e);
                            }
                        }
                    }
                    None => {
                        // Channel closed, flush and shutdown gracefully
                        flush_generic(exporter.clone(), &mut buffer, &output_dir, file_ext, telemetry_type)?;
                        return Ok(());
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush_generic(exporter.clone(), &mut buffer, &output_dir, file_ext, telemetry_type)?;
            }
            _ = token.cancelled() => {
                info!("{} exporter received shutdown signal", telemetry_type);
                flush_generic(exporter.clone(), &mut buffer, &output_dir, file_ext, telemetry_type)?;
                return Ok(());
            }
        }
    }
}

/// Event loop for processing trace data
pub async fn run_traces_loop<E>(
    exporter: std::sync::Arc<E>,
    traces_dir: std::path::PathBuf,
    traces_rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter<ResourceSpans> + Send + Sync,
{
    run_generic_loop(
        exporter,
        traces_dir,
        traces_rx,
        flush_interval,
        token,
        "Traces",
    )
    .await
}

/// Event loop for processing metrics data
pub async fn run_metrics_loop<E>(
    exporter: std::sync::Arc<E>,
    metrics_dir: std::path::PathBuf,
    metrics_rx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter<ResourceMetrics> + Send + Sync,
{
    run_generic_loop(
        exporter,
        metrics_dir,
        metrics_rx,
        flush_interval,
        token,
        "Metrics",
    )
    .await
}

/// Event loop for processing logs data
pub async fn run_logs_loop<E>(
    exporter: std::sync::Arc<E>,
    logs_dir: std::path::PathBuf,
    logs_rx: BoundedReceiver<Vec<Message<ResourceLogs>>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter<ResourceLogs> + Send + Sync,
{
    run_generic_loop(exporter, logs_dir, logs_rx, flush_interval, token, "Logs").await
}

/// Generic helper function to flush telemetry data to disk
fn flush_generic<E, Resource>(
    exporter: std::sync::Arc<E>,
    buffer: &mut Vec<E::Data>,
    output_dir: &std::path::Path,
    file_ext: &str,
    telemetry_type: &str,
) -> Result<()>
where
    E: TypedFileExporter<Resource>,
{
    if !buffer.is_empty() {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let rows = buffer.len();
        let file_name = format!(
            "{}_{}{}",
            telemetry_type.to_lowercase(),
            timestamp,
            file_ext
        );
        let file_path = output_dir.join(file_name);
        exporter.export(buffer, &file_path)?;
        debug!(rows, path=%file_path.display(), "Flushed {} file", telemetry_type.to_lowercase());
        buffer.clear();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::bounded_channel::bounded;
    use crate::exporters::file::task::run_traces_loop;
    use crate::exporters::file::{Result, TypedFileExporter};
    use crate::topology::payload::{KafkaAcknowledgement, KafkaMetadata, Message, MessageMetadata};
    use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    // Mock exporter that does nothing but implements TypedFileExporter
    #[derive(Default)]
    struct MockFileExporter;

    impl TypedFileExporter<ResourceSpans> for MockFileExporter {
        type Data = String;

        fn convert(&self, _data: &ResourceSpans) -> Result<Vec<Self::Data>> {
            Ok(vec!["mock_data".to_string()])
        }

        fn export(&self, _data: &[Self::Data], _path: &Path) -> Result<()> {
            Ok(()) // Do nothing, just return success
        }

        fn file_extension(&self) -> &'static str {
            ".mock"
        }
    }

    #[tokio::test]
    async fn test_message_acknowledgment_flow() {
        // Create metadata with acknowledgment channel
        let (ack_tx, mut ack_rx) = bounded(1);
        let expected_offset = 456;
        let expected_partition = 2;
        let expected_topic_id = 3;
        let metadata = MessageMetadata::kafka(KafkaMetadata {
            offset: expected_offset,
            partition: expected_partition,
            topic_id: expected_topic_id,
            ack_chan: Some(ack_tx),
        });

        // Create a channel for sending messages with metadata
        let (trace_btx, trace_brx) = bounded::<Vec<Message<ResourceSpans>>>(10);

        // Create mock exporter and temp directory
        let exporter = Arc::new(MockFileExporter::default());
        let temp_dir = tempdir().unwrap();
        let output_dir = temp_dir.path().to_path_buf();

        // Start the file task in the background
        let cancellation_token = CancellationToken::new();
        let cancel_clone = cancellation_token.clone();
        let task_handle = tokio::spawn(async move {
            run_traces_loop(
                exporter,
                output_dir,
                trace_brx,
                Duration::from_secs(10), // Long flush interval
                cancel_clone,
            )
            .await
        });

        // Send a message with metadata
        let message = Message {
            metadata: Some(metadata),
            payload: vec![ResourceSpans::default()], // Empty spans
        };
        trace_btx.send(vec![message]).await.unwrap();

        // Wait for acknowledgment
        let received_ack = tokio::time::timeout(Duration::from_secs(5), ack_rx.next())
            .await
            .expect("Timeout waiting for acknowledgment")
            .expect("Failed to receive acknowledgment");

        // Verify the acknowledgment contains the expected information
        match received_ack {
            KafkaAcknowledgement::Ack(ack) => {
                assert_eq!(ack.offset, expected_offset, "Offset should match");
                assert_eq!(ack.partition, expected_partition, "Partition should match");
                assert_eq!(ack.topic_id, expected_topic_id, "Topic ID should match");
            }
            KafkaAcknowledgement::Nack(_) => {
                panic!("Received Nack instead of Ack");
            }
        }

        // Clean up
        drop(trace_btx);
        cancellation_token.cancel();
        let _ = task_handle.await;
    }
}
