// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::kafka::config::KafkaExporterConfig;
use crate::exporters::kafka::errors::{KafkaExportError, Result};
use crate::exporters::kafka::request_builder::KafkaRequestBuilder;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinError;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const MAX_CONCURRENT_ENCODERS: usize = 20;
const MAX_CONCURRENT_SENDS: usize = 10;

type EncodingFuture =
    Pin<Box<dyn Future<Output = std::result::Result<Result<EncodedMessage>, JoinError>> + Send>>;
type SendFuture = Pin<
    Box<
        dyn Future<
                Output = std::result::Result<
                    (i32, i64),
                    (rdkafka::error::KafkaError, rdkafka::message::OwnedMessage),
                >,
            > + Send,
    >,
>;

/// Encoded Kafka message ready to be sent
#[derive(Debug)]
struct EncodedMessage {
    key: String,
    payload: Bytes,
}

/// Trait for telemetry resources that can be exported to Kafka
pub trait KafkaExportable: Debug + Send + Sized + 'static {
    /// Build a Kafka message from this telemetry data
    fn build_kafka_message(
        builder: &KafkaRequestBuilder,
        data: &[Self],
    ) -> Result<(crate::exporters::kafka::request_builder::MessageKey, Bytes)>;

    /// Get the telemetry type name
    fn telemetry_type() -> &'static str;
}

impl KafkaExportable for ResourceSpans {
    fn build_kafka_message(
        builder: &KafkaRequestBuilder,
        spans: &[Self],
    ) -> Result<(crate::exporters::kafka::request_builder::MessageKey, Bytes)> {
        builder.build_trace_message(spans)
    }

    fn telemetry_type() -> &'static str {
        "traces"
    }
}

impl KafkaExportable for ResourceMetrics {
    fn build_kafka_message(
        builder: &KafkaRequestBuilder,
        metrics: &[Self],
    ) -> Result<(crate::exporters::kafka::request_builder::MessageKey, Bytes)> {
        builder.build_metrics_message(metrics)
    }

    fn telemetry_type() -> &'static str {
        "metrics"
    }
}

impl KafkaExportable for ResourceLogs {
    fn build_kafka_message(
        builder: &KafkaRequestBuilder,
        logs: &[Self],
    ) -> Result<(crate::exporters::kafka::request_builder::MessageKey, Bytes)> {
        builder.build_logs_message(logs)
    }

    fn telemetry_type() -> &'static str {
        "logs"
    }
}

/// Generic Kafka exporter for OpenTelemetry data
pub struct KafkaExporter<Resource>
where
    Resource: KafkaExportable,
{
    config: KafkaExporterConfig,
    producer: FutureProducer,
    request_builder: KafkaRequestBuilder,
    rx: BoundedReceiver<Vec<Resource>>,
    topic: String,
    encoding_futures: FuturesUnordered<EncodingFuture>,
    send_futures: FuturesUnordered<SendFuture>,
}

impl<Resource> std::fmt::Debug for KafkaExporter<Resource>
where
    Resource: KafkaExportable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaExporter")
            .field("config", &self.config)
            .field("topic", &self.topic)
            .field("telemetry_type", &Resource::telemetry_type())
            .finish_non_exhaustive()
    }
}

impl<Resource> KafkaExporter<Resource>
where
    Resource: KafkaExportable,
{
    /// Create a new Kafka exporter
    pub fn new(
        config: KafkaExporterConfig,
        rx: BoundedReceiver<Vec<Resource>>,
        topic: String,
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
            rx,
            topic,
            encoding_futures: FuturesUnordered::new(),
            send_futures: FuturesUnordered::new(),
        })
    }

    /// Start the exporter
    pub async fn start(&mut self, cancel_token: CancellationToken) {
        let telemetry_type = Resource::telemetry_type();
        info!(
            "Starting Kafka {} exporter with brokers: {} topic: {}",
            telemetry_type, self.config.brokers, self.topic
        );

        loop {
            select! {
                biased;

                // Process completed sends
                Some(send_result) = self.send_futures.next() => {
                    match send_result {
                        Ok((partition, offset)) => {
                            debug!(
                                "{} sent successfully to partition {} at offset {}",
                                telemetry_type, partition, offset
                            );
                        }
                        Err((e, _)) => {
                            error!("Failed to send {}: {}", telemetry_type, e);
                        }
                    }
                }

                // Process encoded messages ready to send
                Some(encoding_result) = self.encoding_futures.next(), if self.send_futures.len() < MAX_CONCURRENT_SENDS => {
                    match encoding_result {
                        Ok(Ok(encoded_msg)) => {
                            let topic = self.topic.clone();
                            let key = encoded_msg.key;
                            let payload = encoded_msg.payload.to_vec();
                            let producer = self.producer.clone();
                            let timeout = self.config.request_timeout;

                            let send_future = async move {
                                let record = FutureRecord::to(&topic)
                                    .key(&key)
                                    .payload(&payload);
                                producer.send(record, Timeout::After(timeout)).await
                            };

                            self.send_futures.push(Box::pin(send_future));
                        }
                        Ok(Err(e)) => {
                            error!("Failed to encode {}: {}", telemetry_type, e);
                        }
                        Err(e) => {
                            error!("Encoding task failed for {}: {}", telemetry_type, e);
                        }
                    }
                }

                // Receive new data to encode
                data = self.rx.next(), if self.encoding_futures.len() < MAX_CONCURRENT_ENCODERS => {
                    match data {
                        Some(payload) => {
                            debug!(
                                "Received {} {} resources to export",
                                payload.len(),
                                telemetry_type
                            );

                            let req_builder = self.request_builder.clone();
                            let encoding_future = tokio::task::spawn_blocking(move || {
                                Resource::build_kafka_message(&req_builder, &payload)
                                    .map(|(key, payload)| EncodedMessage {
                                        key: key.to_string(),
                                        payload,
                                    })
                            });

                            self.encoding_futures.push(Box::pin(encoding_future));
                        }
                        None => {
                            debug!("Kafka {} exporter receiver closed", telemetry_type);
                            break;
                        }
                    }
                }

                _ = cancel_token.cancelled() => {
                    info!("Kafka {} exporter received cancellation signal", telemetry_type);
                    break;
                }
            }
        }

        // Drain remaining futures
        self.drain_futures().await;

        // Flush any pending messages
        debug!("Flushing Kafka {} producer", telemetry_type);
        let _ = self.producer.flush(Timeout::After(Duration::from_secs(5)));

        info!("Kafka {} exporter stopped", telemetry_type);
    }

    /// Drain remaining futures during shutdown
    async fn drain_futures(&mut self) {
        let telemetry_type = Resource::telemetry_type();

        // First drain all encoding futures
        while !self.encoding_futures.is_empty() {
            if let Some(result) = self.encoding_futures.next().await {
                match result {
                    Ok(Ok(encoded_msg)) => {
                        let topic = self.topic.clone();
                        let key = encoded_msg.key;
                        let payload = encoded_msg.payload.to_vec();
                        let producer = self.producer.clone();
                        let timeout = self.config.request_timeout;

                        let send_future = async move {
                            let record = FutureRecord::to(&topic).key(&key).payload(&payload);
                            producer.send(record, Timeout::After(timeout)).await
                        };

                        self.send_futures.push(Box::pin(send_future));
                    }
                    Ok(Err(e)) => {
                        error!("Failed to encode {} during drain: {}", telemetry_type, e);
                    }
                    Err(e) => {
                        error!(
                            "Encoding task failed during drain for {}: {}",
                            telemetry_type, e
                        );
                    }
                }
            }
        }

        // Then drain all send futures
        while !self.send_futures.is_empty() {
            if let Some(result) = self.send_futures.next().await {
                match result {
                    Ok((partition, offset)) => {
                        debug!(
                            "{} sent successfully to partition {} at offset {} during drain",
                            telemetry_type, partition, offset
                        );
                    }
                    Err((e, _)) => {
                        error!("Failed to send {} during drain: {}", telemetry_type, e);
                    }
                }
            }
        }
    }
}

/// Builder functions for creating specific exporter types

/// Creates a Kafka traces exporter
pub fn build_traces_exporter(
    config: KafkaExporterConfig,
    traces_rx: BoundedReceiver<Vec<ResourceSpans>>,
) -> Result<KafkaExporter<ResourceSpans>> {
    let topic = config
        .traces_topic
        .clone()
        .ok_or_else(|| KafkaExportError::TopicNotConfigured("traces".to_string()))?;

    KafkaExporter::new(config, traces_rx, topic)
}

/// Creates a Kafka metrics exporter
pub fn build_metrics_exporter(
    config: KafkaExporterConfig,
    metrics_rx: BoundedReceiver<Vec<ResourceMetrics>>,
) -> Result<KafkaExporter<ResourceMetrics>> {
    let topic = config
        .metrics_topic
        .clone()
        .ok_or_else(|| KafkaExportError::TopicNotConfigured("metrics".to_string()))?;

    KafkaExporter::new(config, metrics_rx, topic)
}

/// Creates a Kafka logs exporter
pub fn build_logs_exporter(
    config: KafkaExporterConfig,
    logs_rx: BoundedReceiver<Vec<ResourceLogs>>,
) -> Result<KafkaExporter<ResourceLogs>> {
    let topic = config
        .logs_topic
        .clone()
        .ok_or_else(|| KafkaExportError::TopicNotConfigured("logs".to_string()))?;

    KafkaExporter::new(config, logs_rx, topic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel::bounded;
    use crate::exporters::kafka::config::{KafkaExporterConfig, SerializationFormat};

    #[tokio::test]
    async fn test_traces_exporter_creation() {
        let (_, traces_rx) = bounded(10);

        let config = KafkaExporterConfig::new("localhost:9092".to_string())
            .with_traces_topic("test_traces".to_string())
            .with_serialization_format(SerializationFormat::Json);

        let result = build_traces_exporter(config, traces_rx);
        assert!(result.is_ok());

        let exporter = result.unwrap();
        assert_eq!(exporter.topic, "test_traces");
    }

    #[tokio::test]
    async fn test_metrics_exporter_creation() {
        let (_, metrics_rx) = bounded(10);

        let config = KafkaExporterConfig::new("localhost:9092".to_string())
            .with_metrics_topic("test_metrics".to_string())
            .with_serialization_format(SerializationFormat::Json);

        let result = build_metrics_exporter(config, metrics_rx);
        assert!(result.is_ok());

        let exporter = result.unwrap();
        assert_eq!(exporter.topic, "test_metrics");
    }

    #[tokio::test]
    async fn test_logs_exporter_creation() {
        let (_, logs_rx) = bounded(10);

        let config = KafkaExporterConfig::new("localhost:9092".to_string())
            .with_logs_topic("test_logs".to_string())
            .with_serialization_format(SerializationFormat::Json);

        let result = build_logs_exporter(config, logs_rx);
        assert!(result.is_ok());

        let exporter = result.unwrap();
        assert_eq!(exporter.topic, "test_logs");
    }

    #[tokio::test]
    async fn test_missing_topic_error() {
        let (_, traces_rx) = bounded(10);

        let mut config = KafkaExporterConfig::new("localhost:9092".to_string());
        // Explicitly remove traces_topic to test error case
        config.traces_topic = None;

        let result = build_traces_exporter(config, traces_rx);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            KafkaExportError::TopicNotConfigured(_)
        ));
    }

    #[test]
    fn test_telemetry_types() {
        assert_eq!(ResourceSpans::telemetry_type(), "traces");
        assert_eq!(ResourceMetrics::telemetry_type(), "metrics");
        assert_eq!(ResourceLogs::telemetry_type(), "logs");
    }
}
