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
                        for mut metadata in metadata_to_ack {
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
