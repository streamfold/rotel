use crate::bounded_channel::BoundedReceiver;
use crate::exporters::file::{Result, TypedFileExporter};
use chrono::Utc;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Event loop for processing trace data
pub async fn run_traces_loop<E>(
    exporter: std::sync::Arc<E>,
    traces_dir: std::path::PathBuf,
    mut traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter<ResourceSpans> + Send + Sync,
{
    let file_ext = exporter.file_extension();
    let mut span_buffer: Vec<E::Data> = Vec::new();
    let mut flush_timer = tokio::time::interval(flush_interval);

    loop {
        tokio::select! {
            traces_result = traces_rx.next() => {
                match traces_result {
                    Some(traces) => {
                        // Process incoming traces
                        for resource_spans in traces {
                            let mut converted_spans = exporter.convert(&resource_spans)?;
                            span_buffer.append(&mut converted_spans);
                        }
                    }
                    None => {
                        // Channel closed, flush and shutdown gracefully
                        debug!("Traces channel closed, flushing final data");
                        flush_spans(&*exporter, &mut span_buffer, &traces_dir, file_ext)?;
                        info!("Traces exporter shutting down gracefully");
                        return Ok(());
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush_spans(&*exporter, &mut span_buffer, &traces_dir, file_ext)?;
            }
            _ = token.cancelled() => {
                info!("Traces exporter received shutdown signal");
                flush_spans(&*exporter, &mut span_buffer, &traces_dir, file_ext)?;
                return Ok(());
            }
        }
    }
}

/// Event loop for processing metrics data
pub async fn run_metrics_loop<E>(
    exporter: std::sync::Arc<E>,
    metrics_dir: std::path::PathBuf,
    mut metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter<ResourceMetrics> + Send + Sync,
{
    let file_ext = exporter.file_extension();
    let mut metric_buffer: Vec<E::Data> = Vec::new();
    let mut flush_timer = tokio::time::interval(flush_interval);

    loop {
        tokio::select! {
            metrics_result = metrics_rx.next() => {
                match metrics_result {
                    Some(metrics) => {
                        // Process incoming metrics
                        for resource_metrics in metrics {
                            let mut converted_metrics = exporter.convert(&resource_metrics)?;
                            metric_buffer.append(&mut converted_metrics);
                        }
                    }
                    None => {
                        // Channel closed, flush and shutdown gracefully
                        debug!("Metrics channel closed, flushing final data");
                        flush_metrics(&*exporter, &mut metric_buffer, &metrics_dir, file_ext)?;
                        info!("Metrics exporter shutting down gracefully");
                        return Ok(());
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush_metrics(&*exporter, &mut metric_buffer, &metrics_dir, file_ext)?;
            }
            _ = token.cancelled() => {
                info!("Metrics exporter received shutdown signal");
                flush_metrics(&*exporter, &mut metric_buffer, &metrics_dir, file_ext)?;
                return Ok(());
            }
        }
    }
}

/// Event loop for processing logs data
pub async fn run_logs_loop<E>(
    exporter: std::sync::Arc<E>,
    logs_dir: std::path::PathBuf,
    mut logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
    flush_interval: std::time::Duration,
    token: CancellationToken,
) -> Result<()>
where
    E: TypedFileExporter<ResourceLogs> + Send + Sync,
{
    let file_ext = exporter.file_extension();
    let mut log_buffer: Vec<E::Data> = Vec::new();
    let mut flush_timer = tokio::time::interval(flush_interval);

    loop {
        tokio::select! {
            logs_result = logs_rx.next() => {
                match logs_result {
                    Some(logs) => {
                        // Process incoming logs
                        for resource_logs in logs {
                            let mut converted_logs = exporter.convert(&resource_logs)?;
                            log_buffer.append(&mut converted_logs);
                        }
                    }
                    None => {
                        // Channel closed, flush and shutdown gracefully
                        debug!("Logs channel closed, flushing final data");
                        flush_logs(&*exporter, &mut log_buffer, &logs_dir, file_ext)?;
                        info!("Logs exporter shutting down gracefully");
                        return Ok(());
                    }
                }
            }
            _ = flush_timer.tick() => {
                flush_logs(&*exporter, &mut log_buffer, &logs_dir, file_ext)?;
            }
            _ = token.cancelled() => {
                info!("Logs exporter received shutdown signal");
                flush_logs(&*exporter, &mut log_buffer, &logs_dir, file_ext)?;
                return Ok(());
            }
        }
    }
}

/// Helper function to flush span data to disk
fn flush_spans<E>(
    exporter: &E,
    span_buffer: &mut Vec<E::Data>,
    traces_dir: &std::path::Path,
    file_ext: &str,
) -> Result<()>
where
    E: TypedFileExporter<ResourceSpans>,
{
    if !span_buffer.is_empty() {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let rows = span_buffer.len();
        let file_path = traces_dir.join(format!("spans_{}{}", timestamp, file_ext));
        exporter.export(span_buffer, &file_path)?;
        debug!(rows, path=%file_path.display(), "Flushed spans file");
        span_buffer.clear();
    }
    Ok(())
}

/// Helper function to flush metrics data to disk
fn flush_metrics<E>(
    exporter: &E,
    metric_buffer: &mut Vec<E::Data>,
    metrics_dir: &std::path::Path,
    file_ext: &str,
) -> Result<()>
where
    E: TypedFileExporter<ResourceMetrics>,
{
    if !metric_buffer.is_empty() {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let rows = metric_buffer.len();
        let file_path = metrics_dir.join(format!("metrics_{}{}", timestamp, file_ext));
        exporter.export(metric_buffer, &file_path)?;
        debug!(rows, path=%file_path.display(), "Flushed metrics file");
        metric_buffer.clear();
    }
    Ok(())
}

/// Helper function to flush logs data to disk
fn flush_logs<E>(
    exporter: &E,
    log_buffer: &mut Vec<E::Data>,
    logs_dir: &std::path::Path,
    file_ext: &str,
) -> Result<()>
where
    E: TypedFileExporter<ResourceLogs>,
{
    if !log_buffer.is_empty() {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let rows = log_buffer.len();
        let file_path = logs_dir.join(format!("logs_{}{}", timestamp, file_ext));
        exporter.export(log_buffer, &file_path)?;
        debug!(rows, path=%file_path.display(), "Flushed logs file");
        log_buffer.clear();
    }
    Ok(())
}
