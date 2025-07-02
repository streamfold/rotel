// SPDX-License-Identifier: Apache-2.0

//! Kafka Integration Tests
//!
//! These tests require a running Kafka instance and are designed to verify
//! actual end-to-end functionality of the Kafka exporter.
//!
//! To run these tests:
//! 1. Start Kafka: ./scripts/kafka-test-env.sh start
//! 2. Run tests: cargo test --test kafka_integration_tests --features integration-tests
//! 3. Stop Kafka: ./scripts/kafka-test-env.sh stop

#![cfg(feature = "integration-tests")]

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rotel::bounded_channel::bounded;
use rotel::exporters::kafka::config::{KafkaExporterConfig, SerializationFormat};
use rotel::exporters::kafka::{build_logs_exporter, build_metrics_exporter, build_traces_exporter};
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
    let trace_data = vec![FakeOTLP::trace_service_request().resource_spans[0].clone()];
    traces_tx
        .send(trace_data)
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
    assert!(json.is_array(), "Message should be an array");
    let traces = json.as_array().unwrap();
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
    let metrics_data = vec![FakeOTLP::metrics_service_request().resource_metrics[0].clone()];
    metrics_tx
        .send(metrics_data)
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
        .with_compression("gzip".to_string());

    let mut exporter =
        build_logs_exporter(config, logs_rx).expect("Failed to create Kafka logs exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Send test logs data
    let logs_data = vec![FakeOTLP::logs_service_request().resource_logs[0].clone()];
    logs_tx
        .send(logs_data)
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
    assert!(json.is_array(), "Message should be an array");
    let logs = json.as_array().unwrap();
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
    let trace_data = vec![FakeOTLP::trace_service_request().resource_spans[0].clone()];
    let metrics_data = vec![FakeOTLP::metrics_service_request().resource_metrics[0].clone()];
    let logs_data = vec![FakeOTLP::logs_service_request().resource_logs[0].clone()];

    traces_tx
        .send(trace_data)
        .await
        .expect("Failed to send trace data");
    metrics_tx
        .send(metrics_data)
        .await
        .expect("Failed to send metrics data");
    logs_tx
        .send(logs_data)
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
        .send(logs_data_1)
        .await
        .expect("Failed to send logs data 1");
    logs_tx
        .send(logs_data_2)
        .await
        .expect("Failed to send logs data 2");
    logs_tx
        .send(logs_data_3)
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
            .send(logs_data)
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
        .send(metrics_data_1)
        .await
        .expect("Failed to send metrics data 1");
    metrics_tx
        .send(metrics_data_2)
        .await
        .expect("Failed to send metrics data 2");
    metrics_tx
        .send(metrics_data_3)
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
            .send(metrics_data)
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
