use crate::bounded_channel::SendError;
use crate::receivers::kafka::config::{DeserializationFormat, KafkaReceiverConfig};
use crate::receivers::kafka::error::{KafkaReceiverError, Result};
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use crate::topology::payload::KafkaMetadata;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::error::Error;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

// In the future if we support arbitrary topics with non OTLP data we might replace these
// with a map.
const TRACES_TOPIC_ID: u8 = 0;
const METRICS_TOPIC_ID: u8 = 1;
const LOGS_TOPIC_ID: u8 = 2;

// Struct to hold topic processing configuration
struct TopicConfig<'a, T> {
    name: &'static str,
    topic_id: u8,
    output: &'a Option<OTLPOutput<payload::Message<T>>>,
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
        })
    }

    async fn send_to_pipeline<T>(
        &self,
        kafka_metadata: KafkaMetadata,
        request: Vec<T>,
        output: &Option<OTLPOutput<payload::Message<T>>>,
    ) -> std::result::Result<(), SendError> {
        if let Some(output) = output {
            output
                .send(payload::Message {
                    // N.B - Explicitly disabling sending any metadata for now on this next commit.
                    // We are doing this because we are wiring in Message<T> handing with acknowledgement
                    // end-to-end all the way to the exporters. However, as we're not doing anything
                    // with the acknowledegements, we disable their creation for now out of an abundance of caution.
                    // This will allow us to land these changes and others while iterating on the additional pieces
                    // of Kafka offset tracking. Finally, once everything is in place, we can "wire up" sending the metadata
                    // and verify with some aggressive end-to-end tests that everything is working as expected.
                    // metadata: Some(payload::MessageMetadata::Kafka(kafka_metadata)),
                    metadata: None,
                    payload: request,
                })
                .await?;
        }
        Ok(())
    }

    // Generic function to handle deserialization and pipeline sending
    async fn process_kafka_message<T>(
        &self,
        data: &[u8],
    ) -> std::result::Result<T, Box<dyn Error + Send + Sync>>
    where
        T: serde::de::DeserializeOwned + prost::Message + Default,
    {
        let request = match self.format {
            DeserializationFormat::Json => serde_json::from_slice::<T>(data).map_err(|e| {
                debug!("Failed to decode {}", e);
                e
            })?,
            DeserializationFormat::Protobuf => {
                T::decode(Bytes::from(data.to_vec())).map_err(|e| {
                    debug!("Failed to decode {}", e);
                    e
                })?
            }
        };
        Ok(request)
    }

    // Helper function to process payload data for any topic type
    async fn process_payload<T, R>(
        &self,
        data: &[u8],
        offset: i64,
        partition: i32,
        config: TopicConfig<'_, R>,
        extract_resources: impl Fn(T) -> Vec<R>,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>>
    where
        T: serde::de::DeserializeOwned + prost::Message + Default,
    {
        debug!(
            "Processing {} message with {} bytes",
            config.name,
            data.len()
        );
        let req = self.process_kafka_message::<T>(data).await?;
        let resources = extract_resources(req);

        let metadata = KafkaMetadata {
            offset,
            partition,
            topic_id: config.topic_id,
            ack_chan: None,
        };

        if let Err(e) = self
            .send_to_pipeline(metadata, resources, config.output)
            .await
        {
            warn!("error sending {} to pipeline {}", config.name, e);
        }

        Ok(())
    }

    pub(crate) async fn run(
        &self,
        receivers_cancel: CancellationToken,
    ) -> std::result::Result<(), Box<dyn Error + Send + Sync>> {
        debug!("Starting Kafka receiver");

        // The consumer will automatically start from the position defined by auto.offset.reset
        // which is set to "earliest" by default in the config
        let subscription = self.consumer.subscription().unwrap();
        debug!("Initial subscriptions: {:?}", subscription);

        loop {
            select! {
                record = self.consumer.recv() => {
                    match record {
                        Ok(m) => {
                            let topic = m.topic();
                            let partition = m.partition();
                            let offset = m.offset();

                            debug!("Message received - topic: {}, partition: {}, offset: {}", topic, partition, offset);

                            match m.payload() {
                                None => debug!("Empty payload from Kafka"),
                                Some(data) => {
                                    let result = match topic {
                                        t if t == self.traces_topic => {
                                            let config = TopicConfig {
                                                name: "traces",
                                                topic_id: TRACES_TOPIC_ID,
                                                output: &self.traces_output,
                                            };
                                            self.process_payload(
                                                data,
                                                offset,
                                                partition,
                                                config,
                                                |req: ExportTraceServiceRequest| req.resource_spans,
                                            ).await
                                        }
                                        t if t == self.metrics_topic => {
                                            let config = TopicConfig {
                                                name: "metrics",
                                                topic_id: METRICS_TOPIC_ID,
                                                output: &self.metrics_output,
                                            };
                                            self.process_payload(
                                                data,
                                                offset,
                                                partition,
                                                config,
                                                |req: ExportMetricsServiceRequest| req.resource_metrics,
                                            ).await
                                        }
                                        t if t == self.logs_topic => {
                                            let config = TopicConfig {
                                                name: "logs",
                                                topic_id: LOGS_TOPIC_ID,
                                                output: &self.logs_output,
                                            };
                                            self.process_payload(
                                                data,
                                                offset,
                                                partition,
                                                config,
                                                |req: ExportLogsServiceRequest| req.resource_logs,
                                            ).await
                                        }
                                        _ => {
                                            debug!("Unknown topic: {}", topic);
                                            Ok(())
                                        }
                                    };

                                    if let Err(e) = result {
                                        warn!("Error processing message from topic {}: {}", topic, e);
                                    }
                                }
                            }
                        }
                        Err(e) => info!("Error reading from Kafka: {:?}", e),
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

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None)
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

        let receiver = KafkaReceiver::new(config, None, Some(metrics_output), None)
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

        let receiver = KafkaReceiver::new(config, None, None, Some(logs_output))
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

        let receiver = KafkaReceiver::new(
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

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None)
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

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None)
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
}
