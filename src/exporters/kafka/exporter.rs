// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::BoundedReceiver;
use crate::exporters::kafka::config::KafkaExporterConfig;
use crate::exporters::kafka::errors::{KafkaExportError, Result};
use crate::exporters::kafka::request_builder::KafkaRequestBuilder;
use crate::topology::payload::{Ack, Message, MessageMetadata, OTLPFrom};
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures_util::stream::FuturesOrdered;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rdkafka::producer::future_producer::Delivery;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::Serialize;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinError;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

const MAX_CONCURRENT_ENCODERS: usize = 1000;
const MAX_CONCURRENT_SENDS: usize = 1000;

#[rustfmt::skip]
type EncodingFuture = Pin<Box<dyn Future<Output = std::result::Result<Result<EncodedMessage>, JoinError>> + Send>>;

/// Encoded Kafka message ready to be sent
#[derive(Debug)]
struct EncodedMessage {
    key: String,
    payload: Bytes,
    metadata: Option<Vec<MessageMetadata>>,
}

/// Wrapper for send future that includes metadata for acknowledgment
type SendFutureWithMetadata = Pin<
    Box<
        dyn Future<
                Output = (
                    std::result::Result<
                        Delivery,
                        (rdkafka::error::KafkaError, rdkafka::message::OwnedMessage),
                    >,
                    Option<Vec<MessageMetadata>>,
                ),
            > + Send,
    >,
>;

/// Kafka-specific acknowledger that acknowledges messages on successful send
#[derive(Default, Clone)]
pub struct KafkaAcknowledger;

impl KafkaAcknowledger {
    pub async fn acknowledge_metadata(&self, metadata: Option<Vec<MessageMetadata>>) {
        if let Some(metadata_vec) = metadata {
            for metadata in metadata_vec {
                if let Err(e) = metadata.ack().await {
                    tracing::warn!("Failed to acknowledge Kafka message: {:?}", e);
                }
            }
        }
    }
}

/// Trait for telemetry resources that can be exported to Kafka
pub trait KafkaExportable:
    Debug + Send + Sized + 'static + prost::Message + Serialize + Clone
{
    /// The OTLP request type for this telemetry type
    type Request: prost::Message + OTLPFrom<Vec<Message<Self>>> + Serialize + Clone;

    /// Create a new request builder for this telemetry type
    fn create_request_builder(
        format: crate::exporters::kafka::config::SerializationFormat,
    ) -> KafkaRequestBuilder<Self, Self::Request>;

    /// Build a Kafka message from this telemetry data
    fn build_kafka_message(
        builder: &KafkaRequestBuilder<Self, Self::Request>,
        config: &KafkaExporterConfig,
        data: Vec<Message<Self>>,
    ) -> Result<(String, Bytes, Option<Vec<MessageMetadata>>)>;

    /// Get the telemetry type name
    fn telemetry_type() -> &'static str;

    /// Split data for partition-based processing if needed
    /// Default implementation returns data unchanged
    fn split_for_partitioning(
        _config: &KafkaExporterConfig,
        data: Vec<Message<Self>>,
    ) -> Vec<Vec<Message<Self>>> {
        vec![data]
    }
}

/// Calculate a deterministic hash of resource attributes
/// Maps with the same key/value pairs in different order produce the same hash
fn calculate_resource_attributes_hash(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<u8> {
    use std::collections::BTreeMap;
    use std::hash::{Hash, Hasher};

    if attributes.is_empty() {
        return Vec::new();
    }

    // Use BTreeMap to ensure deterministic ordering
    let mut sorted_attrs = BTreeMap::new();

    for attr in attributes {
        // For each attribute, create a hash of its value
        let mut value_hasher = std::collections::hash_map::DefaultHasher::new();
        hash_any_value(&mut value_hasher, &attr.value);
        let value_hash = value_hasher.finish();

        // Store with key -> value_hash to handle potential duplicate keys
        // (though they shouldn't exist per OTLP spec)
        sorted_attrs.insert(attr.key.clone(), value_hash);
    }

    // Now hash the sorted map
    let mut final_hasher = std::collections::hash_map::DefaultHasher::new();
    for (key, value_hash) in sorted_attrs {
        key.hash(&mut final_hasher);
        value_hash.hash(&mut final_hasher);
    }

    let hash_u64 = final_hasher.finish();
    hash_u64.to_be_bytes().to_vec()
}

/// Hash an AnyValue recursively
fn hash_any_value<H: std::hash::Hasher>(
    hasher: &mut H,
    value: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
) {
    use std::collections::BTreeMap;
    use std::hash::{Hash, Hasher};

    match value {
        Some(any_value) => match &any_value.value {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                "string".hash(hasher);
                s.hash(hasher);
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => {
                "bool".hash(hasher);
                b.hash(hasher);
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                "int".hash(hasher);
                i.hash(hasher);
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
                "double".hash(hasher);
                d.to_bits().hash(hasher); // Use to_bits for deterministic hashing of floats
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b)) => {
                "bytes".hash(hasher);
                b.hash(hasher);
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(arr)) => {
                "array".hash(hasher);
                arr.values.len().hash(hasher);
                // Hash each value in the array (order matters for arrays)
                for v in &arr.values {
                    hash_any_value(hasher, &Some(v.clone()));
                }
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(kvlist)) => {
                "kvlist".hash(hasher);

                // Use BTreeMap for deterministic ordering of nested key-value pairs
                let mut sorted_kvs = BTreeMap::new();
                for kv in &kvlist.values {
                    let mut value_hasher = std::collections::hash_map::DefaultHasher::new();
                    hash_any_value(&mut value_hasher, &kv.value);
                    let value_hash = value_hasher.finish();
                    sorted_kvs.insert(kv.key.clone(), value_hash);
                }

                // Hash the sorted map
                sorted_kvs.len().hash(hasher);
                for (key, value_hash) in sorted_kvs {
                    key.hash(hasher);
                    value_hash.hash(hasher);
                }
            }
            None => {
                "empty".hash(hasher);
            }
        },
        None => {
            "empty".hash(hasher);
        }
    }
}

/// Extract resource attributes hash from the first ResourceLogs
fn extract_resource_attributes_hash_from_logs(logs: &[Message<ResourceLogs>]) -> Option<Vec<u8>> {
    for message in logs {
        for resource_log in &message.payload {
            if let Some(resource) = &resource_log.resource {
                return Some(calculate_resource_attributes_hash(&resource.attributes));
            }
        }
    }
    None
}

/// Extract resource attributes hash from the first ResourceMetrics
fn extract_resource_attributes_hash_from_metrics(
    metrics: &[Message<ResourceMetrics>],
) -> Option<Vec<u8>> {
    for message in metrics {
        for resource_metric in &message.payload {
            if let Some(resource) = &resource_metric.resource {
                return Some(calculate_resource_attributes_hash(&resource.attributes));
            }
        }
    }
    None
}

impl KafkaExportable for ResourceSpans {
    type Request = ExportTraceServiceRequest;

    fn create_request_builder(
        format: crate::exporters::kafka::config::SerializationFormat,
    ) -> KafkaRequestBuilder<Self, Self::Request> {
        KafkaRequestBuilder::new(format)
    }

    fn build_kafka_message(
        builder: &KafkaRequestBuilder<Self, Self::Request>,
        _config: &KafkaExporterConfig,
        spans: Vec<Message<Self>>,
    ) -> Result<(String, Bytes, Option<Vec<MessageMetadata>>)> {
        // Use empty message key and send all spans together
        let key = String::new();

        // Extract metadata from all messages
        let mut metadata: Vec<MessageMetadata> = Vec::new();
        let mut payloads: Vec<ResourceSpans> = Vec::new();

        for message in spans {
            for resource_spans in message.payload {
                payloads.push(resource_spans);
            }
            if let Some(md) = message.metadata {
                metadata.push(md)
            }
        }

        let metadata = if metadata.is_empty() {
            None
        } else {
            Some(metadata)
        };

        let payload = builder.build_message(payloads)?;
        Ok((key, payload, metadata))
    }

    fn telemetry_type() -> &'static str {
        "traces"
    }

    /// Split for partitioning - traces are not split
    fn split_for_partitioning(
        _config: &KafkaExporterConfig,
        data: Vec<Message<Self>>,
    ) -> Vec<Vec<Message<Self>>> {
        vec![data]
    }
}

impl KafkaExportable for ResourceMetrics {
    type Request = ExportMetricsServiceRequest;

    fn create_request_builder(
        format: crate::exporters::kafka::config::SerializationFormat,
    ) -> KafkaRequestBuilder<Self, Self::Request> {
        KafkaRequestBuilder::new(format)
    }

    fn build_kafka_message(
        builder: &KafkaRequestBuilder<Self, Self::Request>,
        config: &KafkaExporterConfig,
        metrics: Vec<Message<Self>>,
    ) -> Result<(String, Bytes, Option<Vec<MessageMetadata>>)> {
        let key = if !config.partition_metrics_by_resource_attributes {
            // Default: empty message key
            String::new()
        } else {
            // When partition_metrics_by_resource_attributes is enabled, use hash of resource attributes as key
            // Expect metrics to contain only one ResourceMetrics after splitting
            let hash = extract_resource_attributes_hash_from_metrics(metrics.as_slice());
            hash.map_or(String::new(), hex::encode)
        };

        // Extract metadata from all messages
        let mut metadata: Vec<MessageMetadata> = Vec::new();
        let mut payloads: Vec<ResourceMetrics> = Vec::new();

        for message in metrics {
            for resource_metrics in message.payload {
                payloads.push(resource_metrics);
            }
            if let Some(md) = message.metadata {
                metadata.push(md)
            }
        }

        let metadata = if metadata.is_empty() {
            None
        } else {
            Some(metadata)
        };
        let payload = builder.build_message(payloads)?;
        Ok((key, payload, metadata))
    }

    fn telemetry_type() -> &'static str {
        "metrics"
    }

    /// Split metrics by resource attributes when partition_metrics_by_resource_attributes is enabled
    /// Each ResourceMetrics becomes its own message, matching the Go implementation
    fn split_for_partitioning(
        config: &KafkaExporterConfig,
        data: Vec<Message<Self>>,
    ) -> Vec<Vec<Message<Self>>> {
        if config.partition_metrics_by_resource_attributes {
            // We need to split WITHIN each Message's payload
            // Each individual ResourceMetrics should become its own Message
            let mut result = Vec::new();
            for mut message in data {
                let payload_len = message.payload.len();
                // Split the payload Vec<ResourceMetrics> into individual messages
                for (idx, resource_metric) in message.payload.into_iter().enumerate() {
                    let metadata = if payload_len == 1 {
                        // Single item: take original metadata
                        message.metadata.take()
                    } else if idx == payload_len - 1 {
                        // Last item: take original metadata so ref count can reach 0
                        message.metadata.take()
                    } else {
                        // Earlier items: clone metadata (increments ref count)
                        message.metadata.clone()
                    };

                    result.push(vec![Message {
                        metadata,
                        payload: vec![resource_metric],
                    }]);
                }
            }
            result
        } else {
            vec![data]
        }
    }
}

impl KafkaExportable for ResourceLogs {
    type Request = ExportLogsServiceRequest;

    fn create_request_builder(
        format: crate::exporters::kafka::config::SerializationFormat,
    ) -> KafkaRequestBuilder<Self, Self::Request> {
        KafkaRequestBuilder::new(format)
    }

    fn build_kafka_message(
        builder: &KafkaRequestBuilder<Self, Self::Request>,
        config: &KafkaExporterConfig,
        logs: Vec<Message<Self>>,
    ) -> Result<(String, Bytes, Option<Vec<MessageMetadata>>)> {
        let key = if !config.partition_logs_by_resource_attributes {
            // Default: empty message key
            String::new()
        } else {
            // When partition_logs_by_resource_attributes is enabled, use hash of resource attributes as key
            // Expect logs to contain only one ResourceLogs after splitting
            let hash = extract_resource_attributes_hash_from_logs(logs.as_slice());
            hash.map_or(String::new(), hex::encode)
        };

        // Extract metadata from all messages
        let mut metadata: Vec<MessageMetadata> = Vec::new();
        let mut payloads: Vec<ResourceLogs> = Vec::new();

        for message in logs {
            for resource_logs in message.payload {
                payloads.push(resource_logs);
            }
            if let Some(md) = message.metadata {
                metadata.push(md)
            }
        }

        let metadata = if metadata.is_empty() {
            None
        } else {
            Some(metadata)
        };
        let payload = builder.build_message(payloads)?;
        Ok((key, payload, metadata))
    }

    fn telemetry_type() -> &'static str {
        "logs"
    }

    /// Split logs by resource attributes when partition_logs_by_resource_attributes is enabled
    /// Each ResourceLogs becomes its own message, matching the Go implementation
    fn split_for_partitioning(
        config: &KafkaExporterConfig,
        data: Vec<Message<Self>>,
    ) -> Vec<Vec<Message<Self>>> {
        if config.partition_logs_by_resource_attributes {
            // We need to split WITHIN each Message's payload
            // Each individual ResourceLogs should become its own Message
            let mut result = Vec::new();
            for mut message in data {
                let payload_len = message.payload.len();
                // Split the payload Vec<ResourceLogs> into individual messages
                for (idx, resource_log) in message.payload.into_iter().enumerate() {
                    let metadata = if idx == payload_len - 1 {
                        // Last item: take original metadata so ref count can reach 0
                        message.metadata.take()
                    } else {
                        // Earlier items: clone metadata (increments ref count)
                        message.metadata.clone()
                    };

                    result.push(vec![Message {
                        metadata,
                        payload: vec![resource_log],
                    }]);
                }
            }
            result
        } else {
            vec![data]
        }
    }
}

/// Generic Kafka exporter for OpenTelemetry data
pub struct KafkaExporter<Resource>
where
    Resource: KafkaExportable,
{
    config: KafkaExporterConfig,
    producer: FutureProducer,
    request_builder: KafkaRequestBuilder<Resource, Resource::Request>,
    rx: BoundedReceiver<Vec<Message<Resource>>>,
    topic: String,
    acknowledger: KafkaAcknowledger,
    encoding_futures: FuturesOrdered<EncodingFuture>,
    send_futures: FuturesUnordered<SendFutureWithMetadata>,
}

impl<Resource> Debug for KafkaExporter<Resource>
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
        rx: BoundedReceiver<Vec<Message<Resource>>>,
        topic: String,
    ) -> Result<Self> {
        // Build Kafka producer
        let client_config = config.build_client_config();
        let producer: FutureProducer = client_config.create().map_err(|e| {
            KafkaExportError::ConfigurationError(format!("Failed to create producer: {}", e))
        })?;

        let request_builder = Resource::create_request_builder(config.serialization_format.clone());

        Ok(Self {
            config,
            producer,
            request_builder,
            rx,
            topic,
            acknowledger: KafkaAcknowledger::default(),
            encoding_futures: FuturesOrdered::new(),
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
                Some((send_result, metadata)) = self.send_futures.next() => {
                    match send_result {
                        Ok(d) => {
                            debug!(
                                "{} sent successfully to partition {} at offset {}",
                                telemetry_type, d.partition, d.offset
                            );
                            // Acknowledge successful send
                            self.acknowledger.acknowledge_metadata(metadata).await;
                        }
                        Err((e, _)) => {
                            error!("Failed to send {}: {}", telemetry_type, e);
                            // Don't acknowledge on failure - messages remain unacknowledged
                        }
                    }
                }

                // Process encoded messages ready to send
                Some(encoding_result) = self.encoding_futures.next(), if self.send_futures.len() < MAX_CONCURRENT_SENDS => {
                    match encoding_result {
                        Ok(Ok(encoded_msg)) => {
                            let topic = self.topic.clone();
                            let key = encoded_msg.key;
                            let payload = encoded_msg.payload;
                            let metadata = encoded_msg.metadata;
                            let producer = self.producer.clone();
                            let timeout = Duration::from_millis(self.config.request_timeout_ms as u64);
                            let send_future = async move {
                                let record = FutureRecord::to(&topic)
                                    .key(&key)
                                    .payload(payload.as_ref());
                                let result = producer.send(record, timeout).await;
                                (result, metadata)
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
                            // Split data for partitioning if needed (e.g., metrics and logs by resource attributes, traces by trace_id coming later)
                            let payloads_to_process = Resource::split_for_partitioning(&self.config, payload);
                            // N.B. The splitting can result in more payloads to encode than MAX_CONCURRENT_ENCODERS,
                            // however, that is OK for the moment. We're going to allow encoding to "burst" once inside the guard at the top of the select,
                            // i.e. if self.encoding_futures.len() < MAX_CONCURRENT_ENCODERS
                            for single_payload in payloads_to_process {
                                let req_builder = self.request_builder.clone();
                                let config = self.config.clone();
                                let encoding_future = tokio::task::spawn_blocking(move || {
                                    Resource::build_kafka_message(&req_builder, &config, single_payload)
                                        .map(|(key, payload, metadata)| EncodedMessage {
                                            key,
                                            payload,
                                            metadata,
                                        })
                                });
                                self.encoding_futures.push_back(Box::pin(encoding_future));
                            }
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

        // Drain all encoding futures
        while !self.encoding_futures.is_empty() {
            if let Some(result) = self.encoding_futures.next().await {
                match result {
                    Ok(Ok(encoded_msg)) => {
                        let topic = self.topic.clone();
                        let key = encoded_msg.key;
                        let payload = encoded_msg.payload;
                        let producer = self.producer.clone();
                        let timeout = self.config.request_timeout_ms as u64;

                        let metadata = encoded_msg.metadata;
                        let send_future = async move {
                            let record =
                                FutureRecord::to(&topic).key(&key).payload(payload.as_ref());
                            let result = producer
                                .send(record, Timeout::After(Duration::from_millis(timeout)))
                                .await;
                            (result, metadata)
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
            if let Some((result, metadata)) = self.send_futures.next().await {
                match result {
                    Ok(d) => {
                        debug!(
                            "{} sent successfully to partition {} at offset {} during drain",
                            telemetry_type, d.partition, d.offset
                        );
                        // Acknowledge successful send during drain
                        self.acknowledger.acknowledge_metadata(metadata).await;
                    }
                    Err((e, _)) => {
                        error!("Failed to send {} during drain: {}", telemetry_type, e);
                        // Don't acknowledge on failure - messages remain unacknowledged
                    }
                }
            }
        }
    }
}

// Builder functions for creating specific exporter types

/// Creates a Kafka traces exporter
pub fn build_traces_exporter(
    config: KafkaExporterConfig,
    traces_rx: BoundedReceiver<Vec<Message<ResourceSpans>>>,
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
    metrics_rx: BoundedReceiver<Vec<Message<ResourceMetrics>>>,
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
    logs_rx: BoundedReceiver<Vec<Message<ResourceLogs>>>,
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

    #[test]
    fn test_calculate_resource_attributes_hash_empty() {
        let attrs = vec![];
        let hash = calculate_resource_attributes_hash(&attrs);
        assert!(hash.is_empty());
    }

    #[test]
    fn test_calculate_resource_attributes_hash_all_value_types() {
        use opentelemetry_proto::tonic::common::v1::{
            AnyValue, ArrayValue, KeyValue, KeyValueList, any_value,
        };

        // Create a comprehensive set of attributes with all value types
        let attrs = vec![
            KeyValue {
                key: "string_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("test-service".to_string())),
                }),
            },
            KeyValue {
                key: "bool_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::BoolValue(true)),
                }),
            },
            KeyValue {
                key: "int_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(42)),
                }),
            },
            KeyValue {
                key: "double_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::DoubleValue(std::f64::consts::PI)),
                }),
            },
            KeyValue {
                key: "bytes_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::BytesValue(vec![0xDE, 0xAD, 0xBE, 0xEF])),
                }),
            },
            KeyValue {
                key: "array_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::ArrayValue(ArrayValue {
                        values: vec![
                            AnyValue {
                                value: Some(any_value::Value::StringValue("item1".to_string())),
                            },
                            AnyValue {
                                value: Some(any_value::Value::IntValue(100)),
                            },
                            AnyValue {
                                value: Some(any_value::Value::BoolValue(false)),
                            },
                        ],
                    })),
                }),
            },
            KeyValue {
                key: "kvlist_attr".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::KvlistValue(KeyValueList {
                        values: vec![
                            KeyValue {
                                key: "nested_key1".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue(
                                        "nested_value1".to_string(),
                                    )),
                                }),
                            },
                            KeyValue {
                                key: "nested_key2".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::IntValue(200)),
                                }),
                            },
                        ],
                    })),
                }),
            },
            KeyValue {
                key: "null_attr".to_string(),
                value: Some(AnyValue { value: None }),
            },
            KeyValue {
                key: "missing_value_attr".to_string(),
                value: None,
            },
        ];

        let hash = calculate_resource_attributes_hash(&attrs);
        assert_eq!(hash.len(), 8); // u64 as bytes

        // Verify hash is non-zero
        assert_ne!(hash, vec![0, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_calculate_resource_attributes_hash_order_independence() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

        // Create attributes in one order
        let attrs_order1 = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("my-service".to_string())),
                }),
            },
            KeyValue {
                key: "service.version".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("1.2.3".to_string())),
                }),
            },
            KeyValue {
                key: "host.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("host-123".to_string())),
                }),
            },
            KeyValue {
                key: "environment".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("production".to_string())),
                }),
            },
        ];

        // Same attributes in different order
        let attrs_order2 = vec![
            attrs_order1[2].clone(), // host.name
            attrs_order1[0].clone(), // service.name
            attrs_order1[3].clone(), // environment
            attrs_order1[1].clone(), // service.version
        ];

        // Reverse order
        let mut attrs_order3 = attrs_order1.clone();
        attrs_order3.reverse();

        let hash1 = calculate_resource_attributes_hash(&attrs_order1);
        let hash2 = calculate_resource_attributes_hash(&attrs_order2);
        let hash3 = calculate_resource_attributes_hash(&attrs_order3);

        // All hashes should be identical
        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
    }

    #[test]
    fn test_calculate_resource_attributes_hash_deep_recursion() {
        use opentelemetry_proto::tonic::common::v1::{
            AnyValue, ArrayValue, KeyValue, KeyValueList, any_value,
        };

        // Create deeply nested structure with arrays containing kvlists containing arrays
        let attrs = vec![KeyValue {
            key: "deeply_nested".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(any_value::Value::StringValue("top_level_string".to_string())),
                        },
                        AnyValue {
                            value: Some(any_value::Value::KvlistValue(KeyValueList {
                                values: vec![
                                    KeyValue {
                                        key: "mid_level_key".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::ArrayValue(ArrayValue {
                                                values: vec![
                                                    AnyValue {
                                                        value: Some(any_value::Value::IntValue(42)),
                                                    },
                                                    AnyValue {
                                                        value: Some(any_value::Value::KvlistValue(KeyValueList {
                                                            values: vec![
                                                                KeyValue {
                                                                    key: "deep_key1".to_string(),
                                                                    value: Some(AnyValue {
                                                                        value: Some(any_value::Value::StringValue("deep_value1".to_string())),
                                                                    }),
                                                                },
                                                                KeyValue {
                                                                    key: "deep_key2".to_string(),
                                                                    value: Some(AnyValue {
                                                                        value: Some(any_value::Value::ArrayValue(ArrayValue {
                                                                            values: vec![
                                                                                AnyValue {
                                                                                    value: Some(any_value::Value::BoolValue(true)),
                                                                                },
                                                                                AnyValue {
                                                                                    value: Some(any_value::Value::DoubleValue(std::f64::consts::E)),
                                                                                },
                                                                            ],
                                                                        })),
                                                                    }),
                                                                },
                                                            ],
                                                        })),
                                                    },
                                                ],
                                            })),
                                        }),
                                    },
                                    KeyValue {
                                        key: "another_mid_key".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::BytesValue(vec![1, 2, 3])),
                                        }),
                                    },
                                ],
                            })),
                        },
                        AnyValue {
                            value: Some(any_value::Value::IntValue(999)),
                        },
                    ],
                })),
            }),
        }];

        let hash = calculate_resource_attributes_hash(&attrs);
        assert_eq!(hash.len(), 8);

        // Ensure it doesn't crash and produces a valid hash
        assert_ne!(hash, vec![0, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_calculate_resource_attributes_hash_kvlist_order_independence() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, KeyValueList, any_value};

        // KvList with keys in one order
        let attrs1 = vec![KeyValue {
            key: "metadata".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::KvlistValue(KeyValueList {
                    values: vec![
                        KeyValue {
                            key: "region".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("us-west-2".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "zone".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "us-west-2a".to_string(),
                                )),
                            }),
                        },
                        KeyValue {
                            key: "instance_type".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("t2.micro".to_string())),
                            }),
                        },
                    ],
                })),
            }),
        }];

        // Same KvList with keys in different order
        let attrs2 = vec![KeyValue {
            key: "metadata".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::KvlistValue(KeyValueList {
                    values: vec![
                        KeyValue {
                            key: "instance_type".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("t2.micro".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "region".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("us-west-2".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "zone".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "us-west-2a".to_string(),
                                )),
                            }),
                        },
                    ],
                })),
            }),
        }];

        let hash1 = calculate_resource_attributes_hash(&attrs1);
        let hash2 = calculate_resource_attributes_hash(&attrs2);

        // Hashes should be identical (KvList is unordered)
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_calculate_resource_attributes_hash_array_order_dependence() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, any_value};

        // Array with elements in one order
        let attrs1 = vec![KeyValue {
            key: "tags".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(any_value::Value::StringValue("production".to_string())),
                        },
                        AnyValue {
                            value: Some(any_value::Value::StringValue("critical".to_string())),
                        },
                        AnyValue {
                            value: Some(any_value::Value::StringValue("monitored".to_string())),
                        },
                    ],
                })),
            }),
        }];

        // Same array with elements in different order
        let attrs2 = vec![KeyValue {
            key: "tags".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(any_value::Value::StringValue("critical".to_string())),
                        },
                        AnyValue {
                            value: Some(any_value::Value::StringValue("monitored".to_string())),
                        },
                        AnyValue {
                            value: Some(any_value::Value::StringValue("production".to_string())),
                        },
                    ],
                })),
            }),
        }];

        let hash1 = calculate_resource_attributes_hash(&attrs1);
        let hash2 = calculate_resource_attributes_hash(&attrs2);

        // Hashes should be different (arrays are ordered)
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_calculate_resource_attributes_hash_slight_key_difference() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

        // First set of attributes
        let attrs1 = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("my-service".to_string())),
                }),
            },
            KeyValue {
                key: "service.version".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("1.2.3".to_string())),
                }),
            },
            KeyValue {
                key: "deployment.environment".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("production".to_string())),
                }),
            },
        ];

        // Almost identical, but one key is slightly different
        let attrs2 = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("my-service".to_string())),
                }),
            },
            KeyValue {
                key: "service.version".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("1.2.3".to_string())),
                }),
            },
            KeyValue {
                key: "deployment.env".to_string(), // Changed from deployment.environment
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("production".to_string())),
                }),
            },
        ];

        let hash1 = calculate_resource_attributes_hash(&attrs1);
        let hash2 = calculate_resource_attributes_hash(&attrs2);

        // Hashes should be different due to different key
        assert_ne!(hash1, hash2);

        // Also test with slight value difference
        let attrs3 = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("my-service".to_string())),
                }),
            },
            KeyValue {
                key: "service.version".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("1.2.4".to_string())), // Changed version
                }),
            },
            KeyValue {
                key: "deployment.environment".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("production".to_string())),
                }),
            },
        ];

        let hash3 = calculate_resource_attributes_hash(&attrs3);

        // Should be different from attrs1 due to different value
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_calculate_resource_attributes_hash_type_differences() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

        // Same key and "value" but different types
        let attrs_string = vec![KeyValue {
            key: "port".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("8080".to_string())),
            }),
        }];

        let attrs_int = vec![KeyValue {
            key: "port".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(8080)),
            }),
        }];

        let attrs_double = vec![KeyValue {
            key: "port".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::DoubleValue(8080.0)),
            }),
        }];

        let hash_string = calculate_resource_attributes_hash(&attrs_string);
        let hash_int = calculate_resource_attributes_hash(&attrs_int);
        let hash_double = calculate_resource_attributes_hash(&attrs_double);

        // All should be different because types are different
        assert_ne!(hash_string, hash_int);
        assert_ne!(hash_string, hash_double);
        assert_ne!(hash_int, hash_double);
    }
}
