use crate::bounded_channel::bounded;
use crate::receivers::kafka::config::{DeserializationFormat, KafkaReceiverConfig};
use crate::receivers::kafka::error::KafkaReceiverError;
use crate::receivers::kafka::offset_ack_committer::{KafkaOffsetCommitter, commit_offset};
use crate::receivers::kafka::offset_tracker::TopicTrackers;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use crate::topology::payload::{KafkaMetadata, MessageMetadata};
use futures::FutureExt;
use futures::stream::StreamExt;
use opentelemetry_proto::tonic::logs::v1::{LogsData, ResourceLogs};
use opentelemetry_proto::tonic::metrics::v1::{MetricsData, ResourceMetrics};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, TracesData};
use rdkafka::Message;
use rdkafka::client::ClientContext;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// In the future if we support arbitrary topics with non OTLP data we might replace these
// with a map.
const TRACES_TOPIC_ID: u8 = 0;
const METRICS_TOPIC_ID: u8 = 1;
const LOGS_TOPIC_ID: u8 = 2;

const MAX_CONCURRENT_DECODERS: usize = 20;

/// Structure to hold assigned topic/partition information
#[derive(Clone, Debug)]
pub struct AssignedPartitions {
    /// Maps topic name to a set of assigned partition IDs
    pub topics: HashMap<String, HashSet<i32>>,
}

impl AssignedPartitions {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.topics.clear();
    }

    pub fn add_partition(&mut self, topic: String, partition: i32) {
        self.topics
            .entry(topic)
            .or_insert_with(HashSet::new)
            .insert(partition);
    }

    pub fn remove_partition(&mut self, topic: &str, partition: i32) {
        if let Some(partitions) = self.topics.get_mut(topic) {
            partitions.remove(&partition);
            if partitions.is_empty() {
                self.topics.remove(topic);
            }
        }
    }

    pub fn get_partitions_for_topic(&self, topic: &str) -> Option<&HashSet<i32>> {
        self.topics.get(topic)
    }
}

impl Default for AssignedPartitions {
    fn default() -> Self {
        Self::new()
    }
}

/// Consumer context that tracks partition assignments
pub struct KafkaConsumerContext {
    pub assigned_partitions: Arc<std::sync::Mutex<AssignedPartitions>>,
    pub topic_trackers: Arc<TopicTrackers>,
    pub topic_names_to_id: HashMap<String, u8>,
    pub auto_commit: bool,
}

impl KafkaConsumerContext {
    pub fn new(
        auto_commit: bool,
        assigned_partitions: Arc<std::sync::Mutex<AssignedPartitions>>,
        topic_trackers: Arc<TopicTrackers>,
        topic_names_to_id: HashMap<String, u8>,
    ) -> Self {
        Self {
            auto_commit,
            assigned_partitions,
            topic_trackers,
            topic_names_to_id,
        }
    }
}

impl ClientContext for KafkaConsumerContext {}

impl ConsumerContext for KafkaConsumerContext {
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if self.auto_commit {
            return;
        }
        match rebalance {
            Rebalance::Assign(_) => {
                debug!("Pre-rebalance: partition assignment starting");
                // Nothing to do here, if we were loading offsets from some external storage system
                // other than kafka, we'd do it here.
            }
            Rebalance::Revoke(tpl) => {
                debug!("Pre-rebalance: partition revocation starting");
                commit_offset(
                    self.assigned_partitions.clone(),
                    &self.topic_names_to_id,
                    &self.topic_trackers,
                    |tpl, mode| consumer.commit(tpl, mode),
                );
                let mut partition_guard = self.assigned_partitions.lock().unwrap();
                tpl.elements().iter().for_each(|topic_partition| {
                    partition_guard
                        .remove_partition(topic_partition.topic(), topic_partition.partition());
                    debug!(
                        "Revoked: topic {} partition {}",
                        topic_partition.topic(),
                        topic_partition.partition()
                    )
                });
                let res = consumer.unassign();
                match res {
                    Ok(_) => {
                        debug!("Successfully unassigned partitions after rebalance revoke");
                    }
                    Err(e) => {
                        debug!(
                            "Error during consumer unassign in pre_rebalance::Revoke {}",
                            e
                        );
                    }
                }
            }
            Rebalance::Error(err) => {
                warn!("Rebalance error: {}", err);
            }
        }
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if self.auto_commit {
            return;
        }
        match rebalance {
            Rebalance::Assign(assignment) => {
                debug!("Post-rebalance: partitions assigned");
                let mut assigned = self.assigned_partitions.lock().unwrap();
                assigned.clear();
                for topic_partition in assignment.elements() {
                    assigned.add_partition(
                        topic_partition.topic().to_string(),
                        topic_partition.partition(),
                    );
                    debug!(
                        "Assigned: topic {} partition {}",
                        topic_partition.topic(),
                        topic_partition.partition()
                    );
                }
            }
            Rebalance::Revoke(_) => {
                debug!("Post-rebalance: partitions revoked");
            }
            Rebalance::Error(err) => {
                warn!("Rebalance error: {}", err);
            }
        }
    }
}

enum DecodeType<'a> {
    Traces {
        payload: payload::Message<ResourceSpans>,
        output: &'a OTLPOutput<payload::Message<ResourceSpans>>,
    },
    Metrics {
        payload: payload::Message<ResourceMetrics>,
        output: &'a OTLPOutput<payload::Message<ResourceMetrics>>,
    },
    Logs {
        payload: payload::Message<ResourceLogs>,
        output: &'a OTLPOutput<payload::Message<ResourceLogs>>,
    },
}

pub struct KafkaReceiver {
    pub consumer: Arc<StreamConsumer<KafkaConsumerContext>>,
    pub traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
    pub metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
    pub logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    pub traces_topic: String,
    pub metrics_topic: String,
    pub logs_topic: String,
    pub format: DeserializationFormat,
    pub topic_trackers: Arc<TopicTrackers>,
    pub ack_sender: crate::bounded_channel::BoundedSender<payload::KafkaAcknowledgement>,
    auto_commit: bool,
    offset_committer: Option<KafkaOffsetCommitter>,
}

impl KafkaReceiver {
    pub fn new(
        config: KafkaReceiverConfig,
        traces_output: Option<OTLPOutput<payload::Message<ResourceSpans>>>,
        metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
        finite_retry_enabled: bool,
    ) -> Result<Self, KafkaReceiverError> {
        // Get the list of topics to subscribe to
        let topics = config.get_topics();

        if topics.is_empty() {
            return Err(KafkaReceiverError::ConfigurationError(
                "No topics configured for subscription. Enable at least one signal type (traces, metrics, or logs) with a corresponding topic".to_string()
            ));
        }

        // Create an assigned partitions tracker
        let assigned_partitions = Arc::new(std::sync::Mutex::new(AssignedPartitions::new()));
        let topic_trackers = Arc::new(TopicTrackers::new(finite_retry_enabled));

        let traces_topic = config.traces_topic.clone();
        let traces_topic = traces_topic.unwrap_or("".into());

        let metrics_topic = config.metrics_topic.clone();
        let metrics_topic = metrics_topic.unwrap_or("".into());

        let logs_topic = config.logs_topic.clone();
        let logs_topic = logs_topic.unwrap_or("".into());

        // Build topic names map for the offset committer
        let mut topic_names = HashMap::new();
        if !traces_topic.is_empty() {
            topic_names.insert(TRACES_TOPIC_ID, traces_topic.clone());
        }
        if !metrics_topic.is_empty() {
            topic_names.insert(METRICS_TOPIC_ID, metrics_topic.clone());
        }
        if !logs_topic.is_empty() {
            topic_names.insert(LOGS_TOPIC_ID, logs_topic.clone());
        }

        // Build reverse map: topic name -> topic ID
        let topic_name_to_id: HashMap<String, u8> = topic_names
            .iter()
            .map(|(id, name)| (name.clone(), *id))
            .collect();

        let is_auto_commit = config.enable_auto_commit;
        let cc = config.build_client_config();

        // Create consumer context
        let context = KafkaConsumerContext::new(
            is_auto_commit,
            assigned_partitions.clone(),
            topic_trackers.clone(),
            topic_name_to_id.clone(),
        );

        // Build the Kafka client configuration
        let consumer = StreamConsumer::from_config_and_context(&cc, context).map_err(|e| {
            KafkaReceiverError::ConfigurationError(format!("Failed to create receiver: {}", e))
        })?;

        let consumer = Arc::new(consumer);

        // Subscribe to the configured topics
        let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
        consumer.subscribe(&topic_refs).map_err(|e| {
            KafkaReceiverError::ConfigurationError(format!("Failed to subscribe to topics: {}", e))
        })?;

        let (ack_sender, ack_receiver) = bounded(1000);
        let tick_interval = std::time::Duration::from_secs(config.manual_commit_interval_secs);

        let offset_committer = match is_auto_commit {
            true => None,
            false => Some(KafkaOffsetCommitter::new(
                tick_interval,
                ack_receiver,
                topic_trackers.clone(),
                topic_name_to_id,
                assigned_partitions.clone(),
                consumer.clone(),
            )),
        };

        Ok(Self {
            consumer,
            traces_output,
            metrics_output,
            logs_output,
            traces_topic,
            metrics_topic,
            logs_topic,
            format: config.deserialization_format,
            topic_trackers,
            ack_sender,
            auto_commit: is_auto_commit,
            offset_committer,
        })
    }

    // Static method for decoding Kafka messages
    fn decode_kafka_message<T>(
        data: &[u8],
        format: DeserializationFormat,
    ) -> Result<T, Box<dyn Error + Send + Sync>>
    where
        T: serde::de::DeserializeOwned + prost::Message + Default,
    {
        let request = match format {
            DeserializationFormat::Json => serde_json::from_slice::<T>(&data).map_err(|e| {
                debug!("Failed to decode {}", e);
                e
            })?,
            DeserializationFormat::Protobuf => T::decode(data).map_err(|e| {
                debug!("Failed to decode {}", e);
                e
            })?,
        };
        Ok(request)
    }

    pub fn take_offset_committer(&mut self) -> Option<KafkaOffsetCommitter> {
        self.offset_committer.take()
    }

    pub(crate) async fn run(
        self,
        receivers_cancel: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("Starting Kafka receiver");

        // The consumer will automatically start from the position defined by auto.offset.reset
        // which is set to "earliest" by default in the config
        let consumer = self.consumer.clone();
        let subscription = consumer.subscription()?;
        debug!("Initial subscriptions: {:?}", subscription);

        // Create a reusable buffer for batching messages
        let mut batch = Vec::with_capacity(MAX_CONCURRENT_DECODERS);
        let stream = consumer.stream();
        // Pin the stream so we can call poll_next on it
        tokio::pin!(stream);

        loop {
            // Clear the batch from the previous iteration. We don't really need this
            // as we're draining the buffer, but it's still a safe guard
            batch.clear();

            // 1. Wait for the first message (or cancellation)
            select! {
                maybe_msg = stream.next() => {
                    let Some(msg) = maybe_msg else {
                        debug!("Kafka stream ended");
                        break;
                    };
                    batch.push(msg);
                }
                _ = receivers_cancel.cancelled() => {
                    debug!("Kafka receiver cancelled, shutting down");
                    break;
                }
            }

            // 2. Greedily pull any other immediately ready messages up to the limit
            while batch.len() < MAX_CONCURRENT_DECODERS {
                // Check if the next message is ready without blocking
                match stream.next().now_or_never() {
                    Some(Some(msg)) => batch.push(msg),
                    _ => break,
                }
            }

            // 3. Process the batch (or cancellation)
            select! {
                _ = self.process_batch(&mut batch) => {},
                _ = receivers_cancel.cancelled() => {
                    debug!("Kafka receiver cancelled, shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn process_batch<M>(&self, batch: &mut Vec<KafkaResult<M>>)
    where
        M: Message + Send,
    {
        use rayon::prelude::*;

        let messages = batch
            .par_drain(..)
            .filter_map(|message| self.process_message(message))
            .collect::<Vec<_>>();

        for decoded in messages {
            match decoded {
                DecodeType::Traces { payload, output } => {
                    if output.send(payload).await.is_err() {
                        warn!("Failed to send traces to pipeline: channel disconnected");
                    }
                }
                DecodeType::Metrics { payload, output } => {
                    if output.send(payload).await.is_err() {
                        warn!("Failed to send metrics to pipeline: channel disconnected");
                    }
                }
                DecodeType::Logs { payload, output } => {
                    if output.send(payload).await.is_err() {
                        warn!("Failed to send logs to pipeline: channel disconnected");
                    }
                }
            }
        }
    }

    fn process_message<M>(&self, message: KafkaResult<M>) -> Option<DecodeType<'_>>
    where
        M: Message,
    {
        let message = match message {
            Ok(message) => message,
            Err(e) => {
                info!("Error reading from Kafka: {:?}", e);
                return None;
            }
        };

        let topic = message.topic();
        let partition = message.partition();
        let offset = message.offset();

        debug!(topic, partition, offset, "Message received");

        let Some(data) = message.payload() else {
            debug!("Empty payload from Kafka");
            return None;
        };

        match topic {
            t if t == self.traces_topic => self._process_message::<TracesData>(
                data,
                offset,
                partition,
                self.traces_output.as_ref(),
            )?,
            t if t == self.metrics_topic => self._process_message::<MetricsData>(
                data,
                offset,
                partition,
                self.metrics_output.as_ref(),
            )?,
            t if t == self.logs_topic => self._process_message::<LogsData>(
                data,
                offset,
                partition,
                self.logs_output.as_ref(),
            )?,
            _ => {
                debug!("Received data from kafka for unknown topic: {}", topic);

                None
            }
        }
    }

    fn _process_message<'a, T>(
        &self,
        data: &[u8],
        offset: i64,
        partition: i32,
        output: Option<&'a OTLPOutput<payload::Message<T::ResourceType>>>,
    ) -> Option<Option<DecodeType<'a>>>
    where
        T: OtlpResourceProvider,
    {
        self.topic_trackers.track(T::TOPIC_ID, partition, offset);

        let output = output?;

        let md = (!self.auto_commit).then(|| {
            MessageMetadata::kafka(KafkaMetadata {
                offset,
                partition,
                topic_id: TracesData::TOPIC_ID,
                ack_chan: Some(self.ack_sender.clone()),
            })
        });

        match Self::decode_kafka_message::<T>(data, self.format) {
            Ok(payload) => Some(Some(payload.extract(md, output))),
            Err(e) => {
                error!("Failed to process Kafka message: {}", e);
                None
            }
        }
    }
}

// Handy trait for extracting OTLP resources from different signal types (Traces, Metrics, Logs).
trait OtlpResourceProvider: serde::de::DeserializeOwned + prost::Message + Default {
    const TOPIC_ID: u8;
    type ResourceType: Send + 'static;

    fn extract<'a>(
        self,
        md: Option<MessageMetadata>,
        output: &'a OTLPOutput<payload::Message<Self::ResourceType>>,
    ) -> DecodeType<'a>;
}

impl OtlpResourceProvider for TracesData {
    const TOPIC_ID: u8 = TRACES_TOPIC_ID;
    type ResourceType = ResourceSpans;

    fn extract<'a>(
        self,
        md: Option<MessageMetadata>,
        output: &'a OTLPOutput<payload::Message<Self::ResourceType>>,
    ) -> DecodeType<'a> {
        DecodeType::Traces {
            payload: payload::Message::new(md, self.resource_spans),
            output,
        }
    }
}

impl OtlpResourceProvider for MetricsData {
    const TOPIC_ID: u8 = METRICS_TOPIC_ID;
    type ResourceType = ResourceMetrics;

    fn extract<'a>(
        self,
        md: Option<MessageMetadata>,
        output: &'a OTLPOutput<payload::Message<Self::ResourceType>>,
    ) -> DecodeType<'a> {
        DecodeType::Metrics {
            payload: payload::Message::new(md, self.resource_metrics),
            output,
        }
    }
}

impl OtlpResourceProvider for LogsData {
    const TOPIC_ID: u8 = LOGS_TOPIC_ID;
    type ResourceType = ResourceLogs;

    fn extract<'a>(
        self,
        md: Option<MessageMetadata>,
        output: &'a OTLPOutput<payload::Message<Self::ResourceType>>,
    ) -> DecodeType<'a> {
        DecodeType::Logs {
            payload: payload::Message::new(md, self.resource_logs),
            output,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel::bounded;
    use crate::receivers::kafka::config::AutoOffsetReset;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
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
    use rdkafka::message::BorrowedHeaders;
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

    struct TestMessage {
        payload: Vec<u8>,
        topic: &'static str,
        partition: i32,
        offset: i64,
    }

    impl rdkafka::Message for TestMessage {
        type Headers = BorrowedHeaders;

        fn key(&self) -> Option<&[u8]> {
            None
        }

        fn payload(&self) -> Option<&[u8]> {
            Some(&self.payload)
        }

        unsafe fn payload_mut(&mut self) -> Option<&mut [u8]> {
            Some(&mut self.payload)
        }

        fn topic(&self) -> &str {
            self.topic
        }

        fn partition(&self) -> i32 {
            self.partition
        }

        fn offset(&self) -> i64 {
            self.offset
        }

        fn timestamp(&self) -> rdkafka::Timestamp {
            rdkafka::Timestamp::NotAvailable
        }

        fn headers(&self) -> Option<&Self::Headers> {
            None
        }
    }

    #[tokio::test]
    async fn test_kafka_receiver_creation_with_valid_config() {
        let config =
            KafkaReceiverConfig::new("localhost:9092".to_string(), "test-group".to_string())
                .with_traces(true)
                .with_traces_topic("test-traces".to_string());

        let (tx, _rx) = bounded::<crate::topology::payload::Message<ResourceSpans>>(100);
        let traces_output = OTLPOutput::new(tx);

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None, false);
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
            &buf,
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
            &buf,
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
            &buf,
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
        let test_data = MetricsData {
            resource_metrics: create_test_resource_metrics(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        let result = KafkaReceiver::decode_kafka_message::<MetricsData>(
            &json_data,
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
        let test_data = TracesData {
            resource_spans: create_test_resource_spans(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        let result = KafkaReceiver::decode_kafka_message::<TracesData>(
            &json_data,
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
        let test_data = LogsData {
            resource_logs: create_test_resource_logs(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        let result = KafkaReceiver::decode_kafka_message::<LogsData>(
            &json_data,
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
            &invalid_data,
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

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None, false)
            .expect("Failed to create receiver");

        // Create test data
        let test_data = ExportTraceServiceRequest {
            resource_spans: create_test_resource_spans(),
        };
        let mut buf = Vec::new();
        test_data.encode(&mut buf).expect("Failed to encode");

        let message = TestMessage {
            payload: buf,
            topic: "test-traces",
            partition: 0,
            offset: 0,
        };

        receiver.process_batch(&mut vec![Ok(message)]).await;

        // Verify we received the data on the output channel
        let received = rx.next().await.expect("Failed to receive traces");
        assert_eq!(received.payload.len(), 1);
        assert_eq!(
            received.payload[0].scope_spans[0].spans[0].name,
            "test-span"
        );
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

        let receiver = KafkaReceiver::new(config, None, Some(metrics_output), None, false)
            .expect("Failed to create receiver");

        // Create test data
        let test_data = ExportMetricsServiceRequest {
            resource_metrics: create_test_resource_metrics(),
        };
        let json_data = serde_json::to_vec(&test_data).expect("Failed to encode JSON");

        let message = TestMessage {
            payload: json_data,
            topic: "test-metrics",
            partition: 0,
            offset: 0,
        };

        receiver.process_batch(&mut vec![Ok(message)]).await;

        // Verify received data
        let received = rx.next().await.expect("Failed to receive metrics");
        assert_eq!(received.payload.len(), 1);
        assert_eq!(
            received.payload[0].scope_metrics[0].metrics[0].name,
            "test.metric"
        );
    }

    #[test]
    fn test_kafka_receiver_creation_fails_without_topics() {
        let config =
            KafkaReceiverConfig::new("localhost:9092".to_string(), "test-group".to_string());

        let result = KafkaReceiver::new(config, None, None, None, false);
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

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None, false)
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

        let receiver = KafkaReceiver::new(config, None, Some(metrics_output), None, false)
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

        let receiver = KafkaReceiver::new(config, None, None, Some(logs_output), false)
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
            false,
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

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None, false)
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

        let receiver = KafkaReceiver::new(config, Some(traces_output), None, None, false)
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

    // #[tokio::test]
    // async fn test_send_with_cancellation_blocked_channel() {
    //     use crate::bounded_channel::bounded;
    //     use crate::receivers::otlp_output::OTLPOutput;
    //     use crate::topology::payload;
    //     use tokio::time::Duration;
    //     use tokio_util::sync::CancellationToken;

    //     // Create a channel with size 1 that we'll fill to block it
    //     let (tx, mut rx) = bounded::<payload::Message<ResourceSpans>>(1);
    //     let output = OTLPOutput::new(tx);

    //     // Fill the channel to make it block by sending a message but not receiving it
    //     let blocking_message = payload::Message::new(None, vec![ResourceSpans::default()]);
    //     output
    //         .send(blocking_message)
    //         .await
    //         .expect("Should be able to send first message");

    //     // Now the channel is full (size 1) - any new send will block

    //     // Create a message to send
    //     let message = payload::Message::new(None, vec![ResourceSpans::default()]);

    //     // Create cancellation token
    //     let cancel_token = CancellationToken::new();
    //     let cancel_token_clone = cancel_token.clone();

    //     // Clone output for the async task
    //     let output_clone = output.clone();

    //     // Spawn task that will try to send with cancellation
    //     let send_task = tokio::spawn(async move {
    //         KafkaReceiver::send_with_cancellation(
    //             &output_clone,
    //             message,
    //             &cancel_token_clone,
    //             "test",
    //         )
    //         .await
    //     });

    //     // Give the task time to start and block on the send
    //     tokio::time::sleep(Duration::from_millis(250)).await;

    //     // Cancel the operation
    //     cancel_token.cancel();

    //     // Wait for the task to complete and verify it returns SendCancelled error
    //     let result = tokio::time::timeout(Duration::from_millis(500), send_task)
    //         .await
    //         .expect("Task should complete within timeout")
    //         .expect("Task should not panic");

    //     assert!(matches!(result, Err(KafkaReceiverError::SendCancelled)));

    //     // Clean up: drain the channel to prevent any hanging
    //     rx.next().await;
    // }
}
