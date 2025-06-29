// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::kafka::config::KafkaExporterConfig;
use crate::exporters::kafka::errors::{KafkaExportError, Result};
use crate::exporters::kafka::request_builder::KafkaRequestBuilder;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::time::Duration;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Kafka exporter for OpenTelemetry data
pub struct KafkaExporter {
    config: KafkaExporterConfig,
    producer: FutureProducer,
    request_builder: KafkaRequestBuilder,
    traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
    logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
}

impl KafkaExporter {
    /// Create a new Kafka exporter
    pub fn new(
        config: KafkaExporterConfig,
        traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
        metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
        logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
    ) -> Result<Self> {
        // Build Kafka producer
        let client_config = config.build_client_config();
        let producer: FutureProducer = client_config.create().map_err(|e| {
            KafkaExportError::ConfigurationError(format!("Failed to create producer: {}", e))
        })?;

        let request_builder = KafkaRequestBuilder::new(config.serialization_format.clone());

        Ok(Self {
            config,
            producer,
            request_builder,
            traces_rx,
            metrics_rx,
            logs_rx,
        })
    }

    /// Start the exporter
    pub async fn start(&mut self, cancel_token: CancellationToken) {
        info!(
            "Starting Kafka exporter with brokers: {}",
            self.config.brokers
        );

        loop {
            select! {
                traces = self.traces_rx.next() => {
                    match traces {
                        Some(spans) => {
                            if let Err(e) = self.export_traces(spans).await {
                                error!("Failed to export traces: {}", e);
                            }
                        }
                        None => break,
                    }
                }
                metrics = self.metrics_rx.next() => {
                    match metrics {
                        Some(metrics) => {
                            if let Err(e) = self.export_metrics(metrics).await {
                                error!("Failed to export metrics: {}", e);
                            }
                        }
                        None => break,
                    }
                }
                logs = self.logs_rx.next() => {
                    match logs {
                        Some(logs) => {
                            if let Err(e) = self.export_logs(logs).await {
                                error!("Failed to export logs: {}", e);
                            }
                        }
                        None => break,
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("Kafka exporter received cancellation signal");
                    break;
                }
            }
        }

        // Flush any pending messages
        debug!("Flushing Kafka producer");
        let _ = self.producer.flush(Timeout::After(Duration::from_secs(5)));

        info!("Kafka exporter stopped");
    }

    /// Export traces to Kafka
    async fn export_traces(&self, spans: Vec<ResourceSpans>) -> Result<()> {
        let topic = self
            .config
            .traces_topic
            .as_ref()
            .ok_or_else(|| KafkaExportError::TopicNotConfigured("traces".to_string()))?;

        debug!(
            "Exporting {} trace resource spans to Kafka topic: {}",
            spans.len(),
            topic
        );

        let (key, payload) = self.request_builder.build_trace_message(&spans)?;
        let key_str = key.to_string();

        let record = FutureRecord::to(topic).key(&key_str).payload(&payload[..]);

        let delivery_result = self
            .producer
            .send(record, Timeout::After(self.config.request_timeout))
            .await;

        match delivery_result {
            Ok((partition, offset)) => {
                debug!(
                    "Traces sent successfully to partition {} at offset {}",
                    partition, offset
                );
                Ok(())
            }
            Err((e, _)) => {
                error!("Failed to send traces: {}", e);
                Err(KafkaExportError::ProducerError(e))
            }
        }
    }

    /// Export metrics to Kafka
    async fn export_metrics(&self, metrics: Vec<ResourceMetrics>) -> Result<()> {
        let topic = self
            .config
            .metrics_topic
            .as_ref()
            .ok_or_else(|| KafkaExportError::TopicNotConfigured("metrics".to_string()))?;

        debug!(
            "Exporting {} metric resource spans to Kafka topic: {}",
            metrics.len(),
            topic
        );

        let (key, payload) = self.request_builder.build_metrics_message(&metrics)?;
        let key_str = key.to_string();

        let record = FutureRecord::to(topic).key(&key_str).payload(&payload[..]);

        let delivery_result = self
            .producer
            .send(record, Timeout::After(self.config.request_timeout))
            .await;

        match delivery_result {
            Ok((partition, offset)) => {
                debug!(
                    "Metrics sent successfully to partition {} at offset {}",
                    partition, offset
                );
                Ok(())
            }
            Err((e, _)) => {
                error!("Failed to send metrics: {}", e);
                Err(KafkaExportError::ProducerError(e))
            }
        }
    }

    /// Export logs to Kafka
    async fn export_logs(&self, logs: Vec<ResourceLogs>) -> Result<()> {
        let topic = self
            .config
            .logs_topic
            .as_ref()
            .ok_or_else(|| KafkaExportError::TopicNotConfigured("logs".to_string()))?;

        debug!(
            "Exporting {} log resource spans to Kafka topic: {}",
            logs.len(),
            topic
        );

        let (key, payload) = self.request_builder.build_logs_message(&logs)?;
        let key_str = key.to_string();

        let record = FutureRecord::to(topic).key(&key_str).payload(&payload[..]);

        let delivery_result = self
            .producer
            .send(record, Timeout::After(self.config.request_timeout))
            .await;

        match delivery_result {
            Ok((partition, offset)) => {
                debug!(
                    "Logs sent successfully to partition {} at offset {}",
                    partition, offset
                );
                Ok(())
            }
            Err((e, _)) => {
                error!("Failed to send logs: {}", e);
                Err(KafkaExportError::ProducerError(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel::bounded;
    use crate::exporters::kafka::config::{KafkaExporterConfig, SerializationFormat};

    #[tokio::test]
    async fn test_exporter_creation() {
        let (traces_tx, traces_rx) = bounded(10);
        let (metrics_tx, metrics_rx) = bounded(10);
        let (logs_tx, logs_rx) = bounded(10);

        let config = KafkaExporterConfig::new("localhost:9092".to_string())
            .with_serialization_format(SerializationFormat::Json);

        // This will fail if Kafka is not running, which is expected in tests
        let result = KafkaExporter::new(config, traces_rx, metrics_rx, logs_rx);

        // Drop senders to avoid warnings
        drop(traces_tx);
        drop(metrics_tx);
        drop(logs_tx);

        // We expect this to succeed in creating the exporter structure
        // even if Kafka is not available
        assert!(result.is_ok());
    }
}
