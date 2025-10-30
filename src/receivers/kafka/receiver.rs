use crate::receivers::kafka::config::{DeserializationFormat, KafkaReceiverConfig};
use crate::receivers::kafka::error::{KafkaReceiverError, Result};
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use crate::topology::payload::KafkaMetadata;
use bytes::Bytes;
use futures::stream::StreamExt;
use futures_util::stream::FuturesOrdered;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use tokio::select;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[rustfmt::skip]
type DecodingFuture = Pin<Box<dyn Future<Output = std::result::Result<std::result::Result<DecodedResult, Box<dyn Error + Send + Sync>>, JoinError>> + Send>>;

// In the future if we support arbitrary topics with non OTLP data we might replace these
// with a map.
const TRACES_TOPIC_ID: u8 = 0;
const METRICS_TOPIC_ID: u8 = 1;
const LOGS_TOPIC_ID: u8 = 2;

const MAX_CONCURRENT_DECODERS: usize = 20;

#[allow(dead_code)] // Just to stop warning on unused metadata
enum DecodedResult {
    Traces {
        resources: Vec<ResourceSpans>,
        metadata: KafkaMetadata,
    },
    Metrics {
        resources: Vec<ResourceMetrics>,
        metadata: KafkaMetadata,
    },
    Logs {
        resources: Vec<ResourceLogs>,
        metadata: KafkaMetadata,
    },
}

pub struct KafkaReceiver {
    pub consumer: StreamConsumer,
    pub traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
    pub metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
    pub logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    pub traces_topic: String,
    pub metrics_topic: String,
    pub logs_topic: String,
    pub format: DeserializationFormat,
    decoding_futures: FuturesOrdered<DecodingFuture>,
}

impl KafkaReceiver {}

impl KafkaReceiver {
    pub fn new(
        config: KafkaReceiverConfig,
        traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
        metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> Result<Self> {
        // Get the list of topics to subscribe to
        let topics = config.get_topics();

        if topics.is_empty() {
            return Err(KafkaReceiverError::ConfigurationError(
                "No topics configured for subscription. Enable at least one signal type (traces, metrics, or logs) with a corresponding topic".to_string()
            ));
        }

        // Build the Kafka client configuration
        let cc = config.build_client_config();
        let consumer: StreamConsumer = cc.create().map_err(|e| {
            KafkaReceiverError::ConfigurationError(format!("Failed to create receiver: {}", e))
        })?;

        // Subscribe to the configured topics
        let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
        consumer.subscribe(&topic_refs).map_err(|e| {
            KafkaReceiverError::ConfigurationError(format!("Failed to subscribe to topics: {}", e))
        })?;

        let traces_topic = config.traces_topic.clone();
        let traces_topic = traces_topic.unwrap_or("".into());

        let metrics_topic = config.metrics_topic.clone();
        let metrics_topic = metrics_topic.unwrap_or("".into());

        let logs_topic = config.logs_topic.clone();
        let logs_topic = logs_topic.unwrap_or("".into());

        Ok(Self {
            consumer,
            traces_output,
            metrics_output,
            logs_output,
            traces_topic,
            metrics_topic,
            logs_topic,
            format: config.deserialization_format,
            decoding_futures: FuturesOrdered::new(),
        })
    }

    fn spawn_decode<T, R>(
        &mut self,
        data: Vec<u8>,
        metadata: KafkaMetadata,
        extract_resources: impl Fn(T) -> Vec<R> + Send + 'static,
        make_result: impl Fn(Vec<R>, KafkaMetadata) -> DecodedResult + Send + 'static,
    ) where
        T: serde::de::DeserializeOwned + prost::Message + Default + Send + 'static,
        R: Send + 'static,
    {
        let format = self.format;

        let f = tokio::task::spawn_blocking(move || {
            match Self::decode_kafka_message::<T>(data, format) {
                Ok(req) => {
                    let resources = extract_resources(req);
                    Ok(make_result(resources, metadata))
                }
                Err(e) => Err(e),
            }
        });
        self.decoding_futures.push_back(Box::pin(f));
    }

    // Static method for decoding Kafka messages
    fn decode_kafka_message<T>(
        data: Vec<u8>,
        format: DeserializationFormat,
    ) -> std::result::Result<T, Box<dyn Error + Send + Sync>>
    where
        T: serde::de::DeserializeOwned + prost::Message + Default,
    {
        let request = match format {
            DeserializationFormat::Json => serde_json::from_slice::<T>(&data).map_err(|e| {
                debug!("Failed to decode {}", e);
                e
            })?,
            DeserializationFormat::Protobuf => T::decode(Bytes::from(data)).map_err(|e| {
                debug!("Failed to decode {}", e);
                e
            })?,
        };
        Ok(request)
    }

    // Helper method to send messages with cancellation support
    async fn send_with_cancellation<T>(
        output: &OTLPOutput<payload::Message<T>>,
        message: payload::Message<T>,
        cancel_token: &CancellationToken,
        signal_type: &str,
    ) -> Result<()>
    where
        T: Send + 'static,
    {
        // Use send_async which returns a future we can select against
        // This avoids both cloning and spinning - proper async coordination
        let send_fut = output.send_async(message);
        tokio::pin!(send_fut);

        select! {
            result = send_fut => {
                match result {
                    Ok(()) => Ok(()),
                    Err(_e) => {
                        // flume::SendError means channel disconnected
                        warn!("Failed to send {} to pipeline: channel disconnected", signal_type);
                        Err(KafkaReceiverError::SendFailed {
                            signal_type: signal_type.to_string(),
                            error: "Channel disconnected".to_string(),
                        })
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                debug!("Received cancellation signal while waiting to send {}", signal_type);
                Err(KafkaReceiverError::SendCancelled)
            }
        }
    }

    pub(crate) async fn run(
        &mut self,
        receivers_cancel: CancellationToken,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        debug!("Starting Kafka receiver");

        // The consumer will automatically start from the position defined by auto.offset.reset
        // which is set to "earliest" by default in the config
        let subscription = self.consumer.subscription().unwrap();
        debug!("Initial subscriptions: {:?}", subscription);

        loop {
            select! {
                record = self.consumer.recv(),  if self.decoding_futures.len() < MAX_CONCURRENT_DECODERS => {
                    match record {
                        Ok(m) => {
                            let topic = m.topic().to_string();
                            let partition = m.partition();
                            let offset = m.offset();

                            debug!("Message received - topic: {}, partition: {}, offset: {}", topic, partition, offset);

                            match m.payload() {
                                None => debug!("Empty payload from Kafka"),
                                Some(data) => {
                                    let data = data.to_vec();
                                    match topic.as_str() {
                                        t if t == self.traces_topic => {
                                            let metadata = KafkaMetadata::new(offset, partition, TRACES_TOPIC_ID, None);
                                            self.spawn_decode(
                                                data, metadata,
                                                |req: ExportTraceServiceRequest| req.resource_spans,
                                                |resources, metadata| DecodedResult::Traces { resources, metadata }
                                            );
                                        }
                                        t if t == self.metrics_topic => {
                                            let metadata = KafkaMetadata::new(offset, partition, METRICS_TOPIC_ID, None);
                                            self.spawn_decode(
                                                data, metadata,
                                                |req: ExportMetricsServiceRequest| req.resource_metrics,
                                                |resources, metadata| DecodedResult::Metrics { resources, metadata }
                                            );
                                        }
                                        t if t == self.logs_topic => {
                                            let metadata = KafkaMetadata::new(offset, partition, LOGS_TOPIC_ID, None);
                                            self.spawn_decode(
                                                data, metadata,
                                                |req: ExportLogsServiceRequest| req.resource_logs,
                                                |resources, metadata| DecodedResult::Logs { resources, metadata }
                                            );
                                        }
                                        _ => {
                                            debug!("Received data from kafka for unknown topic: {}", topic);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => info!("Error reading from Kafka: {:?}", e),
                    }
                },
                // Process completed decoding futures
                decoded_result = self.decoding_futures.select_next_some(), if !self.decoding_futures.is_empty() => {
                    match decoded_result {
                        Ok(decode_result) => {
                            match decode_result {
                                Ok(decoded) => {
                                    match decoded {
                                    // N.B - Explicitly disabling sending any metadata for now on this next commit.
                                    // We are doing this because we are wiring in Message<T> handing with acknowledgement
                                    // end-to-end all the way to the exporters. However, as we're not doing anything
                                    // with the acknowledgement, we disable their creation for now out of an abundance of caution.
                                    // This will allow us to land these changes and others while iterating on the additional pieces
                                    // of Kafka offset tracking. Finally, once everything is in place, we can "wire up" sending the metadata
                                    // and verify with some aggressive end-to-end tests that everything is working as expected.
                                    // metadata: Some(payload::MessageMetadata::Kafka(kafka_metadata)),
                                        DecodedResult::Traces { resources, metadata: _ } => {
                                            if let Some(ref output) = self.traces_output {
                                                let message = payload::Message::new(None, resources);
                                                if let Err(KafkaReceiverError::SendCancelled) = Self::send_with_cancellation(output, message, &receivers_cancel, "traces").await {
                                                    break;
                                                }
                                            }
                                        }
                                        DecodedResult::Metrics { resources, metadata: _ } => {
                                            if let Some(ref output) = self.metrics_output {
                                                let message = payload::Message::new(None, resources);
                                                if let Err(KafkaReceiverError::SendCancelled) = Self::send_with_cancellation(output, message, &receivers_cancel, "metrics").await {
                                                    break;
                                                }
                                                // Other errors already logged in send_with_cancellation
                                            }
                                        }
                                        DecodedResult::Logs { resources, metadata: _ } => {
                                            if let Some(ref output) = self.logs_output {
                                                let message = payload::Message::new(None, resources);
                                                if let Err(KafkaReceiverError::SendCancelled) = Self::send_with_cancellation(output, message, &receivers_cancel, "logs").await {
                                                    break;
                                                }
                                                // Other errors already logged in send_with_cancellation
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to decode Kafka message: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Decoding task failed: {}", e);
                        }
                    }
                },
                _ = receivers_cancel.cancelled() => {
                    debug!("Kafka receiver cancelled, shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel::bounded;
    use crate::receivers::kafka::config::AutoOffsetReset;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
    use prost::Message;
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::util::Timeout;
    use std::time::Duration;

    fn create_test_topic_name(prefix: &str) -> String {
        use std::time::SystemTime;
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("{}-{}", prefix, timestamp)
    }

    async fn create_test_topics(brokers: &str, topics: &[String]) {
        let admin: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("Failed to create admin client");

        let new_topics: Vec<NewTopic> = topics
            .iter()
            .map(|topic| NewTopic::new(topic, 1, TopicReplication::Fixed(1)))
            .collect();

        let results = admin
            .create_topics(new_topics.iter(), &AdminOptions::new())
            .await
            .expect("Failed to create topics");

        for result in results {
            match result {
                Ok(topic) => tracing::debug!("Created topic: {}", topic),
                Err((topic, err)) => {
                    if !err.to_string().contains("already exists") {
                        panic!("Failed to create topic {}: {}", topic, err);
                    }
                }
            }
        }
    }

    fn create_test_producer(brokers: &str) -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create producer")
    }

    fn create_test_resource_spans() -> Vec<ResourceSpans> {
        vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "test-service".to_string(),
                            ),
                        ),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    trace_state: "".to_string(),
                    parent_span_id: vec![],
                    flags: 0,
                    name: "test-span".to_string(),
                    kind: 1,
                    start_time_unix_nano: 1000000000,
                    end_time_unix_nano: 2000000000,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        code: 0,
                        message: "OK".to_string(),
                    }),
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }]
    }

    fn create_test_resource_metrics() -> Vec<ResourceMetrics> {
        vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "test-service".to_string(),
                            )),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "test.metric".to_string(),
                        description: "A test metric".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(
                            Gauge {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![],
                                    start_time_unix_nano: 1000000000,
                                    time_unix_nano: 2000000000,
                                    exemplars: vec![],
                                    flags: 0,
                                    value: Some(
                                        opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(
                                            42,
                                        ),
                                    ),
                                }],
                            },
                        )),
                    }],
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }]
    }

    fn create_test_resource_logs() -> Vec<ResourceLogs> {
        vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "test-service".to_string(),
                            ),
                        ),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1000000000,
                    observed_time_unix_nano: 1000000000,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "Test log message".to_string(),
                            ),
                        ),
                    }),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }]
    }

    #[tokio::test]
    async fn test_kafka_receiver_creation_with_valid_config() {
        let config =
            KafkaReceiverConfig::new("localhost:9092".to_string(), "test-group".to_string())
                .with_traces(true)
                .with_traces_topic("test-traces".to_string());

        let (tx, _rx) = bounded::<crate::topology::payload::Message<ResourceSpans>>(100);
        let traces_output = OTLPOutput::new(tx);

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None);
        assert!(receiver.is_ok());
    }

    #[tokio::test]
    async fn test_decode_kafka_message_protobuf_traces() {
        let test_data = ExportTraceServiceRequest {
            resource_spans: create_test_resource_spans(),
        };
        let mut buf = Vec::new();
        test_data.encode(&mut buf).expect("Failed to encode");

        let result = KafkaReceiver::decode_kafka_message::<ExportTraceServiceRequest>(
            buf,
            DeserializationFormat::Protobuf,
        );

        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert_eq!(decoded.resource_spans.len(), 1);
        assert_eq!(decoded.resource_spans[0].scope_spans.len(), 1);
        assert_eq!(decoded.resource_spans[0].scope_spans[0].spans.len(), 1);
        assert_eq!(
            decoded.resource_spans[0].scope_spans[0].spans[0].name,
            "test-span"
        );
    }

    #[tokio::test]
    async fn test_decode_kafka_message_protobuf_metrics() {
        let test_data = ExportMetricsServiceRequest {
            resource_metrics: create_test_resource_metrics(),
        };
        let mut buf = Vec::new();
        test_data.encode(&mut buf).expect("Failed to encode");

        let result = KafkaReceiver::decode_kafka_message::<ExportMetricsServiceRequest>(
            buf,
            DeserializationFormat::Protobuf,
        );

        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert_eq!(decoded.resource_metrics.len(), 1);
        assert_eq!(decoded.resource_metrics[0].scope_metrics.len(), 1);
        assert_eq!(
            decoded.resource_metrics[0].scope_metrics[0].metrics.len(),
            1
        );
        assert_eq!(
            decoded.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "test.metric"
        );
    }

    #[tokio::test]
    async fn test_decode_kafka_message_protobuf_logs() {
        let test_data = ExportLogsServiceRequest {
            resource_logs: create_test_resource_logs(),
        };
        let mut buf = Vec::new();
        test_data.encode(&mut buf).expect("Failed to encode");

        let result = KafkaReceiver::decode_kafka_message::<ExportLogsServiceRequest>(
            buf,
            DeserializationFormat::Protobuf,
        );

        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert_eq!(decoded.resource_logs.len(), 1);
        assert_eq!(decoded.resource_logs[0].scope_logs.len(), 1);
        assert_eq!(decoded.resource_logs[0].scope_logs[0].log_records.len(), 1);
    }

    #[tokio::test]
    async fn test_decode_kafka_message_json_metrics() {
        let test_data = ExportMetricsServiceRequest {
            resource_metrics: create_test_resource_metrics(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        let result = KafkaReceiver::decode_kafka_message::<ExportMetricsServiceRequest>(
            json_data,
            DeserializationFormat::Json,
        );

        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert_eq!(decoded.resource_metrics.len(), 1);
        assert_eq!(
            decoded.resource_metrics[0].scope_metrics[0].metrics[0].name,
            "test.metric"
        );
    }

    #[tokio::test]
    async fn test_decode_kafka_message_json_traces() {
        let test_data = ExportTraceServiceRequest {
            resource_spans: create_test_resource_spans(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        let result = KafkaReceiver::decode_kafka_message::<ExportTraceServiceRequest>(
            json_data,
            DeserializationFormat::Json,
        );

        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert_eq!(decoded.resource_spans.len(), 1);
        assert_eq!(decoded.resource_spans[0].scope_spans.len(), 1);
        assert_eq!(decoded.resource_spans[0].scope_spans[0].spans.len(), 1);
        assert_eq!(
            decoded.resource_spans[0].scope_spans[0].spans[0].name,
            "test-span"
        );
    }

    #[tokio::test]
    async fn test_decode_kafka_message_json_logs() {
        let test_data = ExportLogsServiceRequest {
            resource_logs: create_test_resource_logs(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        let result = KafkaReceiver::decode_kafka_message::<ExportLogsServiceRequest>(
            json_data,
            DeserializationFormat::Json,
        );

        assert!(result.is_ok());
        let decoded = result.unwrap();
        assert_eq!(decoded.resource_logs.len(), 1);
        let v = decoded.resource_logs[0].scope_logs[0].log_records[0]
            .body
            .as_ref()
            .unwrap()
            .value
            .as_ref()
            .unwrap();
        match v {
            Value::StringValue(s) => {
                assert_eq!(s, "Test log message");
            }
            _ => {
                panic!("Expected value type");
            }
        }
    }

    #[tokio::test]
    async fn test_decode_kafka_message_invalid_data() {
        let invalid_data = b"invalid protobuf data".to_vec();

        let result = KafkaReceiver::decode_kafka_message::<ExportTraceServiceRequest>(
            invalid_data,
            DeserializationFormat::Protobuf,
        );

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_spawn_decode_traces_processing() {
        let config =
            KafkaReceiverConfig::new("localhost:9092".to_string(), "test-group".to_string())
                .with_traces(true)
                .with_traces_topic("test-traces".to_string());

        let (tx, mut rx) = bounded::<crate::topology::payload::Message<ResourceSpans>>(100);
        let traces_output = OTLPOutput::new(tx);

        let mut receiver = KafkaReceiver::new(config, Some(traces_output), None, None)
            .expect("Failed to create receiver");

        // Create test data
        let test_data = ExportTraceServiceRequest {
            resource_spans: create_test_resource_spans(),
        };
        let mut buf = Vec::new();
        test_data.encode(&mut buf).expect("Failed to encode");

        let metadata = KafkaMetadata::new(123, 0, TRACES_TOPIC_ID, None);

        // Spawn decode task
        receiver.spawn_decode(
            buf,
            metadata,
            |req: ExportTraceServiceRequest| req.resource_spans,
            |resources, metadata| DecodedResult::Traces {
                resources,
                metadata,
            },
        );

        // Process the futures manually since we can't run the full main loop
        assert_eq!(receiver.decoding_futures.len(), 1);

        // Wait for the decode task to complete and get the result
        let decoded_result = receiver.decoding_futures.select_next_some().await;
        assert!(decoded_result.is_ok());

        let inner_result = decoded_result.unwrap();
        assert!(inner_result.is_ok());

        let decoded = inner_result.unwrap();
        match decoded {
            DecodedResult::Traces {
                resources,
                metadata: _,
            } => {
                assert_eq!(resources.len(), 1);
                assert_eq!(resources[0].scope_spans[0].spans[0].name, "test-span");

                // Now test the result processing by sending to pipeline
                if let Some(ref output) = receiver.traces_output {
                    let message = payload::Message {
                        metadata: None,
                        payload: resources,
                    };
                    output.send(message).await.expect("Failed to send");
                }

                // Verify we received the data on the output channel
                let received = rx.next().await.expect("Failed to receive traces");
                assert_eq!(received.payload.len(), 1);
                assert_eq!(
                    received.payload[0].scope_spans[0].spans[0].name,
                    "test-span"
                );
            }
            _ => panic!("Expected traces result"),
        }
    }

    #[tokio::test]
    async fn test_spawn_decode_metrics_processing() {
        let config =
            KafkaReceiverConfig::new("localhost:9092".to_string(), "test-group".to_string())
                .with_metrics(true)
                .with_metrics_topic(Some("test-metrics".to_string()))
                .with_deserialization_format(DeserializationFormat::Json);

        let (tx, mut rx) = bounded::<crate::topology::payload::Message<ResourceMetrics>>(100);
        let metrics_output = OTLPOutput::new(tx);

        let mut receiver = KafkaReceiver::new(config, None, Some(metrics_output), None)
            .expect("Failed to create receiver");

        // Create test data
        let test_data = ExportMetricsServiceRequest {
            resource_metrics: create_test_resource_metrics(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        let metadata = KafkaMetadata::new(456, 1, METRICS_TOPIC_ID, None);

        // Test with JSON format
        receiver.spawn_decode(
            json_data,
            metadata,
            |req: ExportMetricsServiceRequest| req.resource_metrics,
            |resources, metadata| DecodedResult::Metrics {
                resources,
                metadata,
            },
        );

        // Process the future
        let decoded_result = receiver.decoding_futures.select_next_some().await;
        assert!(decoded_result.is_ok());

        let inner_result = decoded_result.unwrap();
        assert!(inner_result.is_ok());

        let decoded = inner_result.unwrap();
        match decoded {
            DecodedResult::Metrics {
                resources,
                metadata: _,
            } => {
                assert_eq!(resources.len(), 1);
                assert_eq!(resources[0].scope_metrics[0].metrics[0].name, "test.metric");

                // Send to pipeline
                if let Some(ref output) = receiver.metrics_output {
                    let message = payload::Message {
                        metadata: None,
                        payload: resources,
                    };
                    output.send(message).await.expect("Failed to send");
                }

                // Verify received data
                let received = rx.next().await.expect("Failed to receive metrics");
                assert_eq!(received.payload.len(), 1);
                assert_eq!(
                    received.payload[0].scope_metrics[0].metrics[0].name,
                    "test.metric"
                );
            }
            _ => panic!("Expected metrics result"),
        }
    }

    #[test]
    fn test_kafka_receiver_creation_fails_without_topics() {
        let config =
            KafkaReceiverConfig::new("localhost:9092".to_string(), "test-group".to_string());

        let result = KafkaReceiver::new(config, None, None, None);
        assert!(result.is_err());
        match result {
            Err(e) => assert!(e.to_string().contains("No topics configured")),
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    #[tokio::test]
    #[ignore = "integration test - requires Kafka broker"]
    async fn test_protobuf_traces_processing() {
        let _ = tracing_subscriber::fmt::try_init();

        let brokers = "localhost:9092";
        let traces_topic = create_test_topic_name("test-traces-proto");

        create_test_topics(brokers, &[traces_topic.clone()]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let config = KafkaReceiverConfig::new(brokers.to_string(), "test-proto-group".to_string())
            .with_traces(true)
            .with_traces_topic(traces_topic.clone())
            .with_deserialization_format(DeserializationFormat::Protobuf)
            .with_auto_offset_reset(AutoOffsetReset::Earliest);

        let (tx, mut rx) = bounded::<crate::topology::payload::Message<ResourceSpans>>(100);
        let traces_output = OTLPOutput::new(tx);

        let mut receiver = KafkaReceiver::new(config, Some(traces_output), None, None)
            .expect("Failed to create receiver");

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let _ = receiver.run(cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let producer = create_test_producer(brokers);
        let test_data = ExportTraceServiceRequest {
            resource_spans: create_test_resource_spans(),
        };
        let mut buf = Vec::new();
        test_data
            .encode(&mut buf)
            .expect("Failed to encode protobuf");

        producer
            .send(
                FutureRecord::to(&traces_topic)
                    .payload(&buf)
                    .key("test-key"),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Failed to send message");

        tokio::time::sleep(Duration::from_millis(500)).await;

        let received = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("Timeout waiting for traces")
            .expect("Failed to receive traces");

        assert_eq!(received.len(), 1);
        assert_eq!(received.payload[0].scope_spans.len(), 1);
        assert_eq!(received.payload[0].scope_spans[0].spans.len(), 1);
        assert_eq!(
            received.payload[0].scope_spans[0].spans[0].name,
            "test-span"
        );

        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    #[ignore = "integration test - requires Kafka broker"]
    async fn test_json_metrics_processing() {
        let _ = tracing_subscriber::fmt::try_init();

        let brokers = "localhost:9092";
        let metrics_topic = create_test_topic_name("test-metrics-json");

        create_test_topics(brokers, &[metrics_topic.clone()]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let config = KafkaReceiverConfig::new(brokers.to_string(), "test-json-group".to_string())
            .with_metrics(true)
            .with_metrics_topic(Some(metrics_topic.clone()))
            .with_deserialization_format(DeserializationFormat::Json)
            .with_auto_offset_reset(AutoOffsetReset::Earliest);

        let (tx, mut rx) = bounded::<crate::topology::payload::Message<ResourceMetrics>>(100);
        let metrics_output = OTLPOutput::new(tx);

        let mut receiver = KafkaReceiver::new(config, None, Some(metrics_output), None)
            .expect("Failed to create receiver");

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let _ = receiver.run(cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let producer = create_test_producer(brokers);
        let test_data = ExportMetricsServiceRequest {
            resource_metrics: create_test_resource_metrics(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        producer
            .send(
                FutureRecord::to(&metrics_topic)
                    .payload(&json_data)
                    .key("test-key"),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Failed to send message");

        tokio::time::sleep(Duration::from_millis(500)).await;

        let received = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("Timeout waiting for metrics")
            .expect("Failed to receive metrics");

        assert_eq!(received.len(), 1);
        assert_eq!(received.payload[0].scope_metrics.len(), 1);
        assert_eq!(received.payload[0].scope_metrics[0].metrics.len(), 1);
        assert_eq!(
            received.payload[0].scope_metrics[0].metrics[0].name,
            "test.metric"
        );

        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    #[ignore = "integration test - requires Kafka broker"]
    async fn test_logs_processing() {
        let _ = tracing_subscriber::fmt::try_init();

        let brokers = "localhost:9092";
        let logs_topic = create_test_topic_name("test-logs");

        create_test_topics(brokers, &[logs_topic.clone()]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let config = KafkaReceiverConfig::new(brokers.to_string(), "test-logs-group".to_string())
            .with_logs(true)
            .with_logs_topic(logs_topic.clone())
            .with_deserialization_format(DeserializationFormat::Protobuf)
            .with_auto_offset_reset(AutoOffsetReset::Earliest);

        let (tx, mut rx) = bounded::<crate::topology::payload::Message<ResourceLogs>>(100);
        let logs_output = OTLPOutput::new(tx);

        let mut receiver = KafkaReceiver::new(config, None, None, Some(logs_output))
            .expect("Failed to create receiver");

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let _ = receiver.run(cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let producer = create_test_producer(brokers);
        let test_data = ExportLogsServiceRequest {
            resource_logs: create_test_resource_logs(),
        };
        let mut buf = Vec::new();
        test_data
            .encode(&mut buf)
            .expect("Failed to encode protobuf");

        producer
            .send(
                FutureRecord::to(&logs_topic).payload(&buf).key("test-key"),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Failed to send message");

        tokio::time::sleep(Duration::from_millis(500)).await;

        let received = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("Timeout waiting for logs")
            .expect("Failed to receive logs");

        assert_eq!(received.len(), 1);
        assert_eq!(received.payload[0].scope_logs.len(), 1);
        assert_eq!(received.payload[0].scope_logs[0].log_records.len(), 1);

        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    #[ignore = "integration test - requires Kafka broker"]
    async fn test_multiple_signals_processing() {
        let _ = tracing_subscriber::fmt::try_init();

        let brokers = "localhost:9092";
        let traces_topic = create_test_topic_name("test-multi-traces");
        let metrics_topic = create_test_topic_name("test-multi-metrics");
        let logs_topic = create_test_topic_name("test-multi-logs");

        create_test_topics(
            brokers,
            &[
                traces_topic.clone(),
                metrics_topic.clone(),
                logs_topic.clone(),
            ],
        )
        .await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let config = KafkaReceiverConfig::new(brokers.to_string(), "test-multi-group".to_string())
            .with_traces(true)
            .with_traces_topic(traces_topic.clone())
            .with_metrics(true)
            .with_metrics_topic(Some(metrics_topic.clone()))
            .with_logs(true)
            .with_logs_topic(logs_topic.clone())
            .with_deserialization_format(DeserializationFormat::Protobuf)
            .with_auto_offset_reset(AutoOffsetReset::Earliest);

        let (traces_tx, mut traces_rx) =
            bounded::<crate::topology::payload::Message<ResourceSpans>>(100);
        let traces_output = OTLPOutput::new(traces_tx);

        let (metrics_tx, mut metrics_rx) =
            bounded::<crate::topology::payload::Message<ResourceMetrics>>(100);
        let metrics_output = OTLPOutput::new(metrics_tx);

        let (logs_tx, mut logs_rx) =
            bounded::<crate::topology::payload::Message<ResourceLogs>>(100);
        let logs_output = OTLPOutput::new(logs_tx);

        let mut receiver = KafkaReceiver::new(
            config,
            Some(traces_output),
            Some(metrics_output),
            Some(logs_output),
        )
        .expect("Failed to create receiver");

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let _ = receiver.run(cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let producer = create_test_producer(brokers);

        let traces_data = ExportTraceServiceRequest {
            resource_spans: create_test_resource_spans(),
        };
        let mut traces_buf = Vec::new();
        traces_data
            .encode(&mut traces_buf)
            .expect("Failed to encode protobuf");

        producer
            .send(
                FutureRecord::to(&traces_topic)
                    .payload(&traces_buf)
                    .key("test-key"),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Failed to send traces");

        let metrics_data = ExportMetricsServiceRequest {
            resource_metrics: create_test_resource_metrics(),
        };
        let mut metrics_buf = Vec::new();
        metrics_data
            .encode(&mut metrics_buf)
            .expect("Failed to encode protobuf");

        producer
            .send(
                FutureRecord::to(&metrics_topic)
                    .payload(&metrics_buf)
                    .key("test-key"),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Failed to send metrics");

        let logs_data = ExportLogsServiceRequest {
            resource_logs: create_test_resource_logs(),
        };
        let mut logs_buf = Vec::new();
        logs_data
            .encode(&mut logs_buf)
            .expect("Failed to encode protobuf");

        producer
            .send(
                FutureRecord::to(&logs_topic)
                    .payload(&logs_buf)
                    .key("test-key"),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Failed to send logs");

        tokio::time::sleep(Duration::from_millis(500)).await;

        let received_traces = tokio::time::timeout(Duration::from_secs(5), traces_rx.next())
            .await
            .expect("Timeout waiting for traces")
            .expect("Failed to receive traces");
        assert_eq!(received_traces.len(), 1);

        let received_metrics = tokio::time::timeout(Duration::from_secs(5), metrics_rx.next())
            .await
            .expect("Timeout waiting for metrics")
            .expect("Failed to receive metrics");
        assert_eq!(received_metrics.len(), 1);

        let received_logs = tokio::time::timeout(Duration::from_secs(5), logs_rx.next())
            .await
            .expect("Timeout waiting for logs")
            .expect("Failed to receive logs");
        assert_eq!(received_logs.len(), 1);

        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    #[ignore = "integration test - requires Kafka broker"]
    async fn test_invalid_message_handling() {
        let _ = tracing_subscriber::fmt::try_init();

        let brokers = "localhost:9092";
        let traces_topic = create_test_topic_name("test-invalid");

        create_test_topics(brokers, &[traces_topic.clone()]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let config =
            KafkaReceiverConfig::new(brokers.to_string(), "test-invalid-group".to_string())
                .with_traces(true)
                .with_traces_topic(traces_topic.clone())
                .with_deserialization_format(DeserializationFormat::Protobuf)
                .with_auto_offset_reset(AutoOffsetReset::Earliest);

        let (tx, _rx) = bounded::<crate::topology::payload::Message<ResourceSpans>>(100);
        let traces_output = OTLPOutput::new(tx);

        let mut receiver = KafkaReceiver::new(config, Some(traces_output), None, None)
            .expect("Failed to create receiver");

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let _ = receiver.run(cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let producer = create_test_producer(brokers);
        let invalid_data = b"invalid protobuf data";

        producer
            .send(
                FutureRecord::to(&traces_topic)
                    .payload(invalid_data)
                    .key("test-key"),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Failed to send message");

        tokio::time::sleep(Duration::from_millis(500)).await;

        cancel_token.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore = "integration test - requires Kafka broker"]
    async fn test_empty_payload_handling() {
        let _ = tracing_subscriber::fmt::try_init();

        let brokers = "localhost:9092";
        let traces_topic = create_test_topic_name("test-empty");

        create_test_topics(brokers, &[traces_topic.clone()]).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let config = KafkaReceiverConfig::new(brokers.to_string(), "test-empty-group".to_string())
            .with_traces(true)
            .with_traces_topic(traces_topic.clone())
            .with_auto_offset_reset(AutoOffsetReset::Earliest);

        let (tx, _rx) = bounded::<crate::topology::payload::Message<ResourceSpans>>(100);
        let traces_output = OTLPOutput::new(tx);

        let mut receiver = KafkaReceiver::new(config, Some(traces_output), None, None)
            .expect("Failed to create receiver");

        let cancel_token = CancellationToken::new();
        let cancel_clone = cancel_token.clone();

        let handle = tokio::spawn(async move {
            let _ = receiver.run(cancel_clone).await;
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        let producer = create_test_producer(brokers);

        producer
            .send(
                FutureRecord::<(), ()>::to(&traces_topic),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .expect("Failed to send message");

        tokio::time::sleep(Duration::from_millis(500)).await;

        cancel_token.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_send_with_cancellation_blocked_channel() {
        use crate::bounded_channel::bounded;
        use crate::receivers::otlp_output::OTLPOutput;
        use crate::topology::payload;
        use tokio::time::Duration;
        use tokio_util::sync::CancellationToken;

        // Create a channel with size 1 that we'll fill to block it
        let (tx, mut rx) = bounded::<payload::Message<ResourceSpans>>(1);
        let output = OTLPOutput::new(tx);

        // Fill the channel to make it block by sending a message but not receiving it
        let blocking_message = payload::Message::new(None, vec![ResourceSpans::default()]);
        output
            .send(blocking_message)
            .await
            .expect("Should be able to send first message");

        // Now the channel is full (size 1) - any new send will block

        // Create a message to send
        let message = payload::Message::new(None, vec![ResourceSpans::default()]);

        // Create cancellation token
        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();

        // Clone output for the async task
        let output_clone = output.clone();

        // Spawn task that will try to send with cancellation
        let send_task = tokio::spawn(async move {
            KafkaReceiver::send_with_cancellation(
                &output_clone,
                message,
                &cancel_token_clone,
                "test",
            )
            .await
        });

        // Give the task time to start and block on the send
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Cancel the operation
        cancel_token.cancel();

        // Wait for the task to complete and verify it returns SendCancelled error
        let result = tokio::time::timeout(Duration::from_millis(500), send_task)
            .await
            .expect("Task should complete within timeout")
            .expect("Task should not panic");

        assert!(matches!(result, Err(KafkaReceiverError::SendCancelled)));

        // Clean up: drain the channel to prevent any hanging
        rx.next().await;
    }
}
