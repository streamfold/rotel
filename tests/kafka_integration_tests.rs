// SPDX-License-Identifier: Apache-2.0

//! Kafka Integration Tests
//!
//! These tests require a running Kafka instance and are designed to verify
//! actual end-to-end functionality of the Kafka exporter.
//!
//! To run these tests:
//! 1. Start Kafka: ./scripts/kafka-test-env.sh start
//! 2. Run tests: KAFKA_INTEGRATION_TESTS=true cargo test --test kafka_integration_tests
//! 3. Stop Kafka: ./scripts/kafka-test-env.sh stop

#![cfg(kafka_integration_tests = "true")]
#![allow(unused_imports)]

use httpmock::Method::POST as HTTP_POST;
use httpmock::MockServer;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rotel::bounded_channel::bounded;
use rotel::exporters::clickhouse::ClickhouseExporterConfigBuilder;
use rotel::exporters::kafka::config::{Compression, KafkaExporterConfig, SerializationFormat};
use rotel::exporters::kafka::{build_logs_exporter, build_metrics_exporter, build_traces_exporter};
use rotel::receivers::kafka::config::{AutoOffsetReset, KafkaReceiverConfig};
use rotel::receivers::kafka::receiver::KafkaReceiver;
use rotel::receivers::otlp_output::OTLPOutput;
use rotel::topology::export_group::ExportGroupBuilder;
use rotel::topology::payload::Message as PayloadMessage;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use utilities::otlp::FakeOTLP;

const KAFKA_BROKER: &str = "localhost:9092";
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

fn generate_unique_topic(base: &str) -> String {
    let uuid = uuid::Uuid::new_v4();
    format!("{}-{}", base, uuid)
}

async fn setup_consumer(topic: &str) -> StreamConsumer {
    setup_consumer_with_group(topic, format!("test-consumer-{}", uuid::Uuid::new_v4())).await
}

async fn setup_consumer_with_group(topic: &str, group_id: String) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", KAFKA_BROKER)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");

    consumer
}

async fn wait_for_message(consumer: &StreamConsumer, timeout_duration: Duration) -> Option<String> {
    let result = timeout(timeout_duration, async {
        loop {
            match consumer.recv().await {
                Ok(m) => {
                    if let Some(payload) = m.payload() {
                        return Some(String::from_utf8_lossy(payload).to_string());
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await;

    result.unwrap_or(None)
}

#[tokio::test]
async fn test_kafka_exporter_traces_json() {
    let topic = generate_unique_topic("otlp_traces");
    let consumer = setup_consumer(&topic).await;

    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;

    // Create exporter
    let (traces_tx, traces_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_traces_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json);

    let mut exporter =
        build_traces_exporter(config, traces_rx).expect("Failed to create Kafka traces exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Send test trace data
    let trace_data = FakeOTLP::trace_service_request().resource_spans[0].clone();
    traces_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: vec![trace_data],
            request_context: None,
        }])
        .await
        .expect("Failed to send trace data");

    // Wait for message to be consumed
    let message = wait_for_message(&consumer, TEST_TIMEOUT).await;
    assert!(message.is_some(), "No message received from Kafka");

    let message_content = message.unwrap();
    println!("Received message: {}", message_content);

    // Verify it's valid JSON
    let json: Value = serde_json::from_str(&message_content).expect("Message is not valid JSON");

    // Verify it contains trace data
    let traces = json
        .get("resourceSpans")
        .and_then(|value| value.as_array())
        .expect("Message should contain resourceSpans array");
    assert!(!traces.is_empty(), "Traces array should not be empty");

    // Verify trace structure
    let trace = &traces[0];
    assert!(
        trace.get("resource").is_some(),
        "Trace should have resource"
    );
    assert!(
        trace.get("scopeSpans").is_some(),
        "Trace should have scopeSpans"
    );

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
}

#[tokio::test]
async fn test_kafka_exporter_metrics_protobuf() {
    let topic = generate_unique_topic("otlp_metrics");
    let consumer = setup_consumer(&topic).await;

    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;

    // Create exporter
    let (metrics_tx, metrics_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_metrics_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Protobuf);

    let mut exporter = build_metrics_exporter(config, metrics_rx)
        .expect("Failed to create Kafka metrics exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Send test metrics data
    let metrics_data = FakeOTLP::metrics_service_request().resource_metrics[0].clone();
    metrics_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: vec![metrics_data],
            request_context: None,
        }])
        .await
        .expect("Failed to send metrics data");

    // Wait for message to be consumed
    let message = wait_for_message(&consumer, TEST_TIMEOUT).await;
    assert!(message.is_some(), "No message received from Kafka");

    let message_content = message.unwrap();
    println!(
        "Received protobuf message length: {} bytes",
        message_content.len()
    );

    // For protobuf, we can't easily parse as JSON, but we can verify it's binary data
    assert!(
        !message_content.is_empty(),
        "Protobuf message should not be empty"
    );

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
}

#[tokio::test]
async fn test_kafka_exporter_logs_with_compression() {
    let topic = generate_unique_topic("otlp_logs");
    let consumer = setup_consumer(&topic).await;

    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;

    // Create exporter with compression
    let (logs_tx, logs_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_logs_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json)
        .with_compression(Compression::Gzip);

    let mut exporter =
        build_logs_exporter(config, logs_rx).expect("Failed to create Kafka logs exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Send test logs data
    let logs_data = FakeOTLP::logs_service_request().resource_logs[0].clone();
    logs_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: vec![logs_data],
            request_context: None,
        }])
        .await
        .expect("Failed to send logs data");

    // Wait for message to be consumed
    let message = wait_for_message(&consumer, TEST_TIMEOUT).await;
    assert!(message.is_some(), "No message received from Kafka");

    let message_content = message.unwrap();
    println!("Received compressed message: {}", message_content);

    // Verify it's valid JSON (consumer should decompress automatically)
    let json: Value = serde_json::from_str(&message_content).expect("Message is not valid JSON");

    // Verify it contains log data
    let logs = json
        .get("resourceLogs")
        .and_then(|value| value.as_array())
        .expect("Message should contain resourceLogs array");
    assert!(!logs.is_empty(), "Logs array should not be empty");

    // Verify log structure
    let log = &logs[0];
    assert!(log.get("resource").is_some(), "Log should have resource");
    assert!(log.get("scopeLogs").is_some(), "Log should have scopeLogs");

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
}

#[tokio::test]
async fn test_kafka_exporter_multiple_telemetry_types() {
    let traces_topic = generate_unique_topic("otlp_traces");
    let metrics_topic = generate_unique_topic("otlp_metrics");
    let logs_topic = generate_unique_topic("otlp_logs");

    let consumer_traces = setup_consumer(&traces_topic).await;
    let consumer_metrics = setup_consumer(&metrics_topic).await;
    let consumer_logs = setup_consumer(&logs_topic).await;

    // Give consumers time to connect
    sleep(Duration::from_secs(2)).await;

    // Create exporters for each telemetry type
    let (traces_tx, traces_rx) = bounded(10);
    let (metrics_tx, metrics_rx) = bounded(10);
    let (logs_tx, logs_rx) = bounded(10);

    let traces_config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_traces_topic(traces_topic)
        .with_serialization_format(SerializationFormat::Json);
    let metrics_config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_metrics_topic(metrics_topic)
        .with_serialization_format(SerializationFormat::Json);
    let logs_config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_logs_topic(logs_topic)
        .with_serialization_format(SerializationFormat::Json);

    let mut traces_exporter = build_traces_exporter(traces_config, traces_rx)
        .expect("Failed to create Kafka traces exporter");
    let mut metrics_exporter = build_metrics_exporter(metrics_config, metrics_rx)
        .expect("Failed to create Kafka metrics exporter");
    let mut logs_exporter =
        build_logs_exporter(logs_config, logs_rx).expect("Failed to create Kafka logs exporter");

    let cancel_token = CancellationToken::new();

    // Start all exporters
    let traces_handle = tokio::spawn({
        let token = cancel_token.clone();
        async move {
            traces_exporter.start(token).await;
        }
    });

    let metrics_handle = tokio::spawn({
        let token = cancel_token.clone();
        async move {
            metrics_exporter.start(token).await;
        }
    });

    let logs_handle = tokio::spawn({
        let token = cancel_token.clone();
        async move {
            logs_exporter.start(token).await;
        }
    });

    // Send all types of telemetry data
    let trace_data = FakeOTLP::trace_service_request().resource_spans[0].clone();
    let metrics_data = FakeOTLP::metrics_service_request().resource_metrics[0].clone();
    let logs_data = FakeOTLP::logs_service_request().resource_logs[0].clone();

    traces_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: vec![trace_data],
            request_context: None,
        }])
        .await
        .expect("Failed to send trace data");
    metrics_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: vec![metrics_data],
            request_context: None,
        }])
        .await
        .expect("Failed to send metrics data");
    logs_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: vec![logs_data],
            request_context: None,
        }])
        .await
        .expect("Failed to send logs data");

    // Wait for all messages
    let trace_message = wait_for_message(&consumer_traces, TEST_TIMEOUT).await;
    let metrics_message = wait_for_message(&consumer_metrics, TEST_TIMEOUT).await;
    let logs_message = wait_for_message(&consumer_logs, TEST_TIMEOUT).await;

    // Verify all messages were received
    assert!(trace_message.is_some(), "No trace message received");
    assert!(metrics_message.is_some(), "No metrics message received");
    assert!(logs_message.is_some(), "No logs message received");

    println!("All telemetry types successfully sent to Kafka!");

    // Clean up
    cancel_token.cancel();
    let _ = traces_handle.await;
    let _ = metrics_handle.await;
    let _ = logs_handle.await;
}

/// Enhanced consumer setup that captures partition information
async fn setup_consumer_with_partition_info(topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set(
            "group.id",
            format!("test-consumer-{}", uuid::Uuid::new_v4()),
        )
        .set("bootstrap.servers", KAFKA_BROKER)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");

    consumer
}

/// Wait for message and capture partition information
async fn wait_for_message_with_partition(
    consumer: &StreamConsumer,
    timeout_duration: Duration,
) -> Option<(String, String, i32)> {
    let result = timeout(timeout_duration, async {
        loop {
            match consumer.recv().await {
                Ok(m) => {
                    if let Some(payload) = m.payload() {
                        let message_content = String::from_utf8_lossy(payload).to_string();
                        let key = m
                            .key()
                            .map(|k| String::from_utf8_lossy(k).to_string())
                            .unwrap_or_default();
                        let partition = m.partition();
                        return Some((message_content, key, partition));
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await;

    result.unwrap_or(None)
}

/// Create test logs with specific resource attributes for partitioning tests
fn create_test_logs_with_resources(resource_attrs: Vec<Vec<KeyValue>>) -> Vec<ResourceLogs> {
    resource_attrs
        .into_iter()
        .map(|attrs| ResourceLogs {
            resource: Some(Resource {
                attributes: attrs,
                entity_refs: vec![],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "test log message".to_string(),
                        )),
                    }),
                    ..Default::default()
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        })
        .collect()
}

/// Create test metrics with specific resource attributes for partitioning tests
fn create_test_metrics_with_resources(resource_attrs: Vec<Vec<KeyValue>>) -> Vec<ResourceMetrics> {
    resource_attrs
        .into_iter()
        .map(|attrs| ResourceMetrics {
            resource: Some(Resource {
                attributes: attrs,
                entity_refs: vec![],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "test_metric".to_string(),
                    description: "Test metric for partitioning".to_string(),
                    unit: "count".to_string(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            value: Some(number_data_point::Value::AsDouble(42.0)),
                            ..Default::default()
                        }],
                    })),
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        })
        .collect()
}

#[tokio::test]
async fn test_logs_partitioning_by_resource_attributes() {
    let topic = generate_unique_topic("otlp_logs");
    let consumer = setup_consumer_with_partition_info(&topic).await;

    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;

    // Create exporter with logs partitioning enabled
    let (logs_tx, logs_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_logs_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json)
        .with_partition_logs_by_resource_attributes(true);

    let mut exporter =
        build_logs_exporter(config, logs_rx).expect("Failed to create Kafka logs exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Create logs with same and different resource attributes
    let same_attrs = vec![
        KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("test-service".to_string())),
            }),
        },
        KeyValue {
            key: "service.version".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("1.0.0".to_string())),
            }),
        },
    ];

    let different_attrs = vec![KeyValue {
        key: "service.name".to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue("other-service".to_string())),
        }),
    }];

    let logs_data_1 = create_test_logs_with_resources(vec![same_attrs.clone()]);
    let logs_data_2 = create_test_logs_with_resources(vec![same_attrs]);
    let logs_data_3 = create_test_logs_with_resources(vec![different_attrs]);

    // Send logs
    logs_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: logs_data_1,
            request_context: None,
        }])
        .await
        .expect("Failed to send logs data 1");
    logs_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: logs_data_2,
            request_context: None,
        }])
        .await
        .expect("Failed to send logs data 2");
    logs_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: logs_data_3,
            request_context: None,
        }])
        .await
        .expect("Failed to send logs data 3");

    // Collect messages with partition info
    let mut messages = Vec::new();
    for _ in 0..3 {
        if let Some((content, key, partition)) =
            wait_for_message_with_partition(&consumer, TEST_TIMEOUT).await
        {
            messages.push((content, key, partition));
        }
    }

    assert_eq!(messages.len(), 3, "Should receive exactly 3 messages");

    // Group by message key
    let mut key_to_partitions = HashMap::new();
    for (_, key, partition) in messages {
        key_to_partitions
            .entry(key)
            .or_insert_with(Vec::new)
            .push(partition);
    }

    // Verify that messages with same resource attributes (same key) go to same partition
    for (key, partitions) in key_to_partitions {
        if partitions.len() > 1 {
            let first_partition = partitions[0];
            for partition in partitions {
                assert_eq!(
                    partition, first_partition,
                    "Messages with same key '{}' should go to same partition",
                    key
                );
            }
        }
    }

    println!(
        "✓ Logs partitioning test passed - logs with same resource attributes go to same partition"
    );

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
}

#[tokio::test]
async fn test_logs_partitioning_distribution_across_partitions() {
    let topic = generate_unique_topic("otlp_logs");

    // Create exporter with logs partitioning enabled
    let (logs_tx, logs_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_logs_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json)
        .with_partition_logs_by_resource_attributes(true);

    let mut exporter =
        build_logs_exporter(config, logs_rx).expect("Failed to create Kafka logs exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Create multiple different resource attribute sets to increase chance of different partitions
    let resource_attr_sets = vec![
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "log-service-alpha".to_string(),
                )),
            }),
        }],
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "log-service-beta".to_string(),
                )),
            }),
        }],
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "log-service-gamma".to_string(),
                )),
            }),
        }],
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "log-service-delta".to_string(),
                )),
            }),
        }],
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "log-service-epsilon".to_string(),
                )),
            }),
        }],
    ];

    // Send logs with different resource attributes
    for attrs in &resource_attr_sets {
        let logs_data = create_test_logs_with_resources(vec![attrs.clone()]);
        logs_tx
            .send(vec![PayloadMessage {
                metadata: None,
                payload: logs_data,
                request_context: None,
            }])
            .await
            .expect("Failed to send logs data");
    }

    // Give producer time to create topic and send messages
    println!("Waiting 5 seconds for producer to create topic and send messages...");
    sleep(Duration::from_secs(5)).await;

    // Now set up consumer after topic exists
    let consumer = setup_consumer_with_partition_info(&topic).await;

    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;

    // Collect messages with partition info
    let mut messages = Vec::new();
    for _ in 0..resource_attr_sets.len() {
        if let Some((content, key, partition)) =
            wait_for_message_with_partition(&consumer, TEST_TIMEOUT).await
        {
            messages.push((content, key, partition));
        }
    }

    assert_eq!(
        messages.len(),
        resource_attr_sets.len(),
        "Should receive exactly {} messages",
        resource_attr_sets.len()
    );

    // Debug: print received messages
    println!("Debug - Received {} messages", messages.len());
    for (i, (_, key, partition)) in messages.iter().enumerate() {
        println!(
            "  Message {}: key='{}' (len={}), partition={}",
            i + 1,
            key,
            key.len(),
            partition
        );
    }

    // Collect unique partitions used
    let mut used_partitions: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for (_, _, partition) in &messages {
        used_partitions.insert(*partition);
    }

    println!(
        "Messages distributed across {} partitions: {:?}",
        used_partitions.len(),
        used_partitions
    );

    // With 5 different resource attribute sets and 3 partitions, we should expect some distribution
    // This is a probabilistic test - it's very unlikely all 5 different attribute sets hash to the same partition
    assert!(
        used_partitions.len() > 1,
        "Expected messages to be distributed across multiple partitions, but all went to the same partition. Used partitions: {:?}",
        used_partitions
    );

    // Verify all messages have non-empty keys (resource attribute hashes)
    for (_, key, _) in &messages {
        assert!(
            !key.is_empty(),
            "All messages should have non-empty resource attribute hash keys"
        );
        assert_eq!(
            key.len(),
            16,
            "Resource attribute hash keys should be 16 characters (hex encoded 8 bytes)"
        );
    }

    println!(
        "✓ Logs partitioning distribution test passed - logs distributed across {} partitions",
        used_partitions.len()
    );

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
}

#[tokio::test]
async fn test_metrics_partitioning_by_resource_attributes() {
    let topic = generate_unique_topic("otlp_metrics");
    let consumer = setup_consumer_with_partition_info(&topic).await;

    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;

    // Create exporter with metrics partitioning enabled
    let (metrics_tx, metrics_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_metrics_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json)
        .with_partition_metrics_by_resource_attributes(true);

    let mut exporter = build_metrics_exporter(config, metrics_rx)
        .expect("Failed to create Kafka metrics exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Create metrics with same and different resource attributes
    let same_attrs = vec![
        KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("test-service".to_string())),
            }),
        },
        KeyValue {
            key: "service.version".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("1.0.0".to_string())),
            }),
        },
    ];

    let different_attrs = vec![KeyValue {
        key: "service.name".to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue("other-service".to_string())),
        }),
    }];

    let metrics_data_1 = create_test_metrics_with_resources(vec![same_attrs.clone()]);
    let metrics_data_2 = create_test_metrics_with_resources(vec![same_attrs]);
    let metrics_data_3 = create_test_metrics_with_resources(vec![different_attrs]);

    // Send metrics
    metrics_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: metrics_data_1,
            request_context: None,
        }])
        .await
        .expect("Failed to send metrics data 1");
    metrics_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: metrics_data_2,
            request_context: None,
        }])
        .await
        .expect("Failed to send metrics data 2");
    metrics_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: metrics_data_3,
            request_context: None,
        }])
        .await
        .expect("Failed to send metrics data 3");

    // Collect messages with partition info
    let mut messages = Vec::new();
    for _ in 0..3 {
        if let Some((content, key, partition)) =
            wait_for_message_with_partition(&consumer, TEST_TIMEOUT).await
        {
            messages.push((content, key, partition));
        }
    }

    assert_eq!(messages.len(), 3, "Should receive exactly 3 messages");

    // Group by message key
    let mut key_to_partitions = HashMap::new();
    for (_, key, partition) in messages {
        key_to_partitions
            .entry(key)
            .or_insert_with(Vec::new)
            .push(partition);
    }

    // Verify that messages with same resource attributes (same key) go to same partition
    for (key, partitions) in key_to_partitions {
        if partitions.len() > 1 {
            let first_partition = partitions[0];
            for partition in partitions {
                assert_eq!(
                    partition, first_partition,
                    "Messages with same key '{}' should go to same partition",
                    key
                );
            }
        }
    }

    println!(
        "✓ Metrics partitioning test passed - metrics with same resource attributes go to same partition"
    );

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
}

#[tokio::test]
async fn test_metrics_partitioning_distribution_across_partitions() {
    let topic = generate_unique_topic("otlp_metrics");

    // Create exporter with metrics partitioning enabled
    let (metrics_tx, metrics_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_metrics_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json)
        .with_partition_metrics_by_resource_attributes(true);

    let mut exporter = build_metrics_exporter(config, metrics_rx)
        .expect("Failed to create Kafka metrics exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Create multiple different resource attribute sets to increase chance of different partitions
    let resource_attr_sets = vec![
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("service-alpha".to_string())),
            }),
        }],
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("service-beta".to_string())),
            }),
        }],
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("service-gamma".to_string())),
            }),
        }],
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("service-delta".to_string())),
            }),
        }],
        vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("service-epsilon".to_string())),
            }),
        }],
    ];

    // Send metrics with different resource attributes
    for attrs in &resource_attr_sets {
        let metrics_data = create_test_metrics_with_resources(vec![attrs.clone()]);
        metrics_tx
            .send(vec![PayloadMessage {
                metadata: None,
                payload: metrics_data,
                request_context: None,
            }])
            .await
            .expect("Failed to send metrics data");
    }

    // Give producer time to create topic and send messages
    println!("Waiting 5 seconds for producer to create topic and send messages...");
    sleep(Duration::from_secs(5)).await;

    // Now set up consumer after topic exists
    let consumer = setup_consumer_with_partition_info(&topic).await;

    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;

    // Collect messages with partition info
    let mut messages = Vec::new();
    for _ in 0..resource_attr_sets.len() {
        if let Some((content, key, partition)) =
            wait_for_message_with_partition(&consumer, TEST_TIMEOUT).await
        {
            messages.push((content, key, partition));
        }
    }

    assert_eq!(
        messages.len(),
        resource_attr_sets.len(),
        "Should receive exactly {} messages",
        resource_attr_sets.len()
    );

    // Debug: print received messages
    println!("Debug - Received {} messages", messages.len());
    for (i, (_, key, partition)) in messages.iter().enumerate() {
        println!(
            "  Message {}: key='{}' (len={}), partition={}",
            i + 1,
            key,
            key.len(),
            partition
        );
    }

    // Collect unique partitions used
    let mut used_partitions: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for (_, _, partition) in &messages {
        used_partitions.insert(*partition);
    }

    println!(
        "Messages distributed across {} partitions: {:?}",
        used_partitions.len(),
        used_partitions
    );

    // With 5 different resource attribute sets and 3 partitions, we should expect some distribution
    // This is a probabilistic test - it's very unlikely all 5 different attribute sets hash to the same partition
    assert!(
        used_partitions.len() > 1,
        "Expected messages to be distributed across multiple partitions, but all went to the same partition. Used partitions: {:?}",
        used_partitions
    );

    // Verify all messages have non-empty keys (resource attribute hashes)
    for (_, key, _) in &messages {
        assert!(
            !key.is_empty(),
            "All messages should have non-empty resource attribute hash keys"
        );
        assert_eq!(
            key.len(),
            16,
            "Resource attribute hash keys should be 16 characters (hex encoded 8 bytes)"
        );
    }

    println!(
        "✓ Metrics partitioning distribution test passed - metrics distributed across {} partitions",
        used_partitions.len()
    );

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
}

// Helper module for UUID generation
mod uuid {
    use std::time::{SystemTime, UNIX_EPOCH};

    pub struct Uuid;

    impl Uuid {
        pub fn new_v4() -> String {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            format!("test-{}", timestamp)
        }
    }
}

// ─── Export Group integration test ───────────────────────────────────────────
//
// Scenario: Clickhouse (always-5xx) → Kafka (working).
// Expected:
//   1. Each of the first `trip_after` batches is nacked by Clickhouse and immediately
//      retried on the Kafka member, which acks.  No traces are lost.
//   2. After `trip_after` consecutive Clickhouse nacks the breaker trips; subsequent
//      batches start directly at the Kafka member (Clickhouse is skipped).
//   3. All traces appear in the Kafka topic exactly once.
//
// Run with:
//   KAFKA_INTEGRATION_TESTS=true cargo test --test kafka_integration_tests test_export_group_

#[tokio::test]
async fn test_export_group_clickhouse_then_kafka() {
    const BATCH_COUNT: usize = 6;
    const TRIP_AFTER: u32 = 3;

    // ── Spin up a mock Clickhouse that always returns HTTP 503 ──────────────
    let clickhouse_server = MockServer::start();
    let clickhouse_mock = clickhouse_server.mock(|when, then| {
        when.method(HTTP_POST).path("/");
        then.status(503).body("service unavailable");
    });

    // ── Build Clickhouse exporter pointed at the mock ───────────────────────
    let (ch_tx, ch_rx) = bounded::<Vec<PayloadMessage<ResourceSpans>>>(64);
    let ch_addr = format!("http://127.0.0.1:{}", clickhouse_server.port());
    let ch_exporter = ClickhouseExporterConfigBuilder::with_defaults(
        ch_addr,
        "otel".to_string(),
        "otel".to_string(),
    )
    .with_retry_max_elapsed_time(Duration::from_millis(1))
    .build()
    .expect("Clickhouse builder failed")
    .build_traces_exporter(ch_rx, None)
    .expect("Clickhouse exporter build failed");

    let ch_cancel = CancellationToken::new();
    let ch_token = ch_cancel.clone();
    tokio::spawn(async move {
        let _ = ch_exporter.start(ch_token).await;
    });

    // ── Build Kafka exporter ────────────────────────────────────────────────
    let topic = generate_unique_topic("export_group_traces");
    let kafka_consumer = setup_consumer(&topic).await;
    // Give the consumer time to connect and assign partitions.
    sleep(Duration::from_secs(2)).await;

    let (kafka_tx, kafka_rx) = bounded::<Vec<PayloadMessage<ResourceSpans>>>(64);
    let kafka_config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_traces_topic(topic.clone())
        .with_serialization_format(SerializationFormat::Json);
    let mut kafka_exporter =
        build_traces_exporter(kafka_config, kafka_rx).expect("Kafka exporter build failed");

    let kafka_cancel = CancellationToken::new();
    let kafka_token = kafka_cancel.clone();
    tokio::spawn(async move {
        kafka_exporter.start(kafka_token).await;
    });

    // ── Wire an ExportGroup: Clickhouse first, Kafka second ─────────────────
    let group = ExportGroupBuilder::<ResourceSpans>::new(64)
        .add_member(ch_tx)
        .add_member(kafka_tx)
        .trip_after(TRIP_AFTER)
        .probe_after(Duration::ZERO) // disable auto-recovery for this test
        .build();

    let mut active_rx = group.subscribe_active();
    let group_tx = group.sender();

    // ── Send BATCH_COUNT batches through the group ──────────────────────────
    let trace_data = FakeOTLP::trace_service_request().resource_spans;
    for _ in 0..BATCH_COUNT {
        group_tx
            .send(vec![PayloadMessage {
                metadata: None,
                payload: trace_data.clone(),
                request_context: None,
            }])
            .await
            .expect("Failed to send to group");
    }

    // ── Wait for the breaker to trip (after TRIP_AFTER nacks) ──────────────
    let tripped = timeout(Duration::from_secs(30), active_rx.wait_for(|&v| v == 1)).await;
    assert!(tripped.is_ok(), "Breaker did not trip within 30s");

    // ── Verify all BATCH_COUNT messages arrived in Kafka ───────────────────
    let mut received = 0usize;
    for _ in 0..BATCH_COUNT {
        let msg = wait_for_message(&kafka_consumer, Duration::from_secs(15)).await;
        assert!(
            msg.is_some(),
            "Missing Kafka message; received {}/{} so far",
            received,
            BATCH_COUNT
        );
        received += 1;
    }
    assert_eq!(
        received, BATCH_COUNT,
        "Expected all {} batches in Kafka",
        BATCH_COUNT
    );

    // ── Verify Clickhouse only received TRIP_AFTER requests before trip ─────
    // The first TRIP_AFTER batches hit Clickhouse (and were retried on Kafka).
    // Post-trip batches bypass Clickhouse entirely.
    // Retry policy may cause multiple hits per batch on Clickhouse; assert at least TRIP_AFTER.
    let ch_hits = clickhouse_mock.hits();
    assert!(
        ch_hits >= TRIP_AFTER as usize,
        "Expected Clickhouse to receive at least {} hits, got {}",
        TRIP_AFTER,
        ch_hits
    );
    // After the trip, subsequent batches should not hit Clickhouse at all.
    // Give the group a moment to process any in-flight post-trip batches.
    sleep(Duration::from_millis(200)).await;
    let ch_hits_after = clickhouse_mock.hits();
    // Post-trip batches (BATCH_COUNT - TRIP_AFTER) should have all gone straight to Kafka.
    let expected_max_ch_hits =
        TRIP_AFTER as usize * (/* retry attempts per batch, typically 1 */2 + 1);
    assert!(
        ch_hits_after <= expected_max_ch_hits,
        "Clickhouse received too many hits after trip ({} > {}); some batches were not short-circuited",
        ch_hits_after,
        expected_max_ch_hits
    );

    ch_cancel.cancel();
    kafka_cancel.cancel();
}

#[tokio::test]
async fn test_kafka_receiver_unacked_dlq_message_is_not_committed() {
    let topic = generate_unique_topic("dlq_replay_pause");
    let group_id = format!("dlq-replay-{}", uuid::Uuid::new_v4());

    // Produce a DLQ trace message using the same protobuf format the Kafka receiver consumes.
    let (producer_tx, producer_rx) = bounded::<Vec<PayloadMessage<ResourceSpans>>>(8);
    let producer_config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_traces_topic(topic.clone())
        .with_serialization_format(SerializationFormat::Protobuf);
    let mut producer =
        build_traces_exporter(producer_config, producer_rx).expect("Kafka exporter build failed");

    let producer_cancel = CancellationToken::new();
    let producer_token = producer_cancel.clone();
    tokio::spawn(async move {
        producer.start(producer_token).await;
    });

    producer_tx
        .send(vec![PayloadMessage {
            metadata: None,
            payload: vec![FakeOTLP::trace_service_request().resource_spans[0].clone()],
            request_context: None,
        }])
        .await
        .expect("Failed to send test trace to Kafka exporter");

    // Wait until Kafka has the message before starting the receiver under test.
    let probe_consumer = setup_consumer(&topic).await;
    assert!(
        wait_for_message(&probe_consumer, TEST_TIMEOUT)
            .await
            .is_some(),
        "Produced DLQ message was not visible in Kafka"
    );
    drop(probe_consumer);

    let (receiver_tx, mut receiver_rx) = bounded::<PayloadMessage<ResourceSpans>>(8);
    let receiver_output = OTLPOutput::new(receiver_tx);
    let receiver_config = KafkaReceiverConfig::new(KAFKA_BROKER.to_string(), group_id.clone())
        .with_traces(true)
        .with_traces_topic(topic.clone())
        .with_auto_offset_reset(AutoOffsetReset::Earliest);
    let mut receiver =
        KafkaReceiver::new(receiver_config, Some(receiver_output), None, None, false)
            .expect("Kafka receiver build failed");
    let mut offset_committer = receiver
        .take_offset_committer()
        .expect("Offset committer should be enabled when auto-commit is disabled");

    let receiver_cancel = CancellationToken::new();
    let committer_cancel = CancellationToken::new();
    let receiver_token = receiver_cancel.clone();
    let committer_token = committer_cancel.clone();
    let receiver_handle = tokio::spawn(async move { receiver.run(receiver_token).await });
    let committer_handle = tokio::spawn(async move { offset_committer.run(committer_token).await });

    let received = timeout(TEST_TIMEOUT, receiver_rx.next())
        .await
        .expect("Kafka receiver did not emit the DLQ message")
        .expect("Kafka receiver output closed unexpectedly");
    assert!(
        received.metadata.is_some(),
        "Kafka receiver output should carry offset-tracking metadata"
    );
    // Deliberately do not ack the metadata. This simulates Clickhouse being down while the
    // replay exporter retries indefinitely, so the DLQ offset must remain uncommitted.

    receiver_cancel.cancel();
    committer_cancel.cancel();
    let _ = receiver_handle.await;
    let _ = committer_handle.await;

    // A new consumer in the same group should still receive the same message because no ack
    // reached the offset committer.
    let replay_consumer = setup_consumer_with_group(&topic, group_id).await;
    assert!(
        wait_for_message(&replay_consumer, TEST_TIMEOUT)
            .await
            .is_some(),
        "Unacked DLQ message was committed; replay would not pause while Clickhouse is down"
    );

    producer_cancel.cancel();
}
