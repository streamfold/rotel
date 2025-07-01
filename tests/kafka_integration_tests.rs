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
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
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

/// Create test traces with specific trace IDs for partitioning tests
fn create_test_traces_with_trace_ids(trace_ids: Vec<Vec<u8>>) -> Vec<ResourceSpans> {
    trace_ids
        .into_iter()
        .map(|trace_id| ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id,
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    name: "test-span".to_string(),
                    ..Default::default()
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        })
        .collect()
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
async fn test_trace_partitioning_by_trace_id() {
    // Initialize tracing for debug output
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init();

    let topic = generate_unique_topic("otlp_traces");

    // Create exporter with trace partitioning enabled
    let (traces_tx, traces_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_traces_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json)
        .with_partition_traces_by_id(true);

    let mut exporter =
        build_traces_exporter(config, traces_rx).expect("Failed to create Kafka traces exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Create traces with same and different trace IDs
    let same_trace_id = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let different_trace_id = vec![16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];

    let trace_data_1 = create_test_traces_with_trace_ids(vec![same_trace_id.clone()]);
    let trace_data_2 = create_test_traces_with_trace_ids(vec![same_trace_id.clone()]);
    let trace_data_3 = create_test_traces_with_trace_ids(vec![different_trace_id.clone()]);

    // Debug: Verify trace data has correct trace IDs
    println!(
        "Debug - trace_data_1 trace_id: {:?}",
        hex::encode(&same_trace_id)
    );
    println!(
        "Debug - trace_data_2 trace_id: {:?}",
        hex::encode(&same_trace_id)
    );
    println!(
        "Debug - trace_data_3 trace_id: {:?}",
        hex::encode(&different_trace_id)
    );

    // Debug: Print structure of trace_data_1
    if let Some(first_resource_span) = trace_data_1.first() {
        if let Some(first_scope_span) = first_resource_span.scope_spans.first() {
            if let Some(first_span) = first_scope_span.spans.first() {
                println!(
                    "Debug - trace_data_1 first span trace_id: {:?}",
                    hex::encode(&first_span.trace_id)
                );
            }
        }
    }

    // Send traces (this will create the topic)
    traces_tx
        .send(trace_data_1)
        .await
        .expect("Failed to send trace data 1");
    traces_tx
        .send(trace_data_2)
        .await
        .expect("Failed to send trace data 2");
    traces_tx
        .send(trace_data_3)
        .await
        .expect("Failed to send trace data 3");

    // Give producer time to create topic and send messages
    println!("Waiting 5 seconds for producer to create topic and send messages...");
    sleep(Duration::from_secs(5)).await;

    // Now set up consumer after topic exists
    let consumer = setup_consumer_with_partition_info(&topic).await;

    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;

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
    for (_, key, partition) in &messages {
        key_to_partitions
            .entry(key.clone())
            .or_insert_with(Vec::new)
            .push(*partition);
    }

    // Verify that messages with same trace ID (same key) go to same partition
    // Note: Empty/NULL keys are randomly partitioned with consistent_random partitioner
    for (key, partitions) in key_to_partitions {
        if partitions.len() > 1 && !key.is_empty() {
            let first_partition = partitions[0];
            for partition in partitions {
                assert_eq!(
                    partition, first_partition,
                    "Messages with same non-empty key '{}' should go to same partition",
                    key
                );
            }
        }
    }

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

    // Verify we got messages with trace ID keys (non-empty)
    // With consistent_random partitioner, messages with same non-empty keys go to same partition
    let non_empty_keys: Vec<_> = messages
        .iter()
        .filter(|(_, key, _)| !key.is_empty())
        .collect();
    assert!(
        !non_empty_keys.is_empty(),
        "Should have at least one message with non-empty trace ID key"
    );

    println!("✓ Trace partitioning test passed - traces with same ID go to same partition");

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
}

#[tokio::test]
async fn test_trace_partitioning_distribution_across_partitions() {
    let topic = generate_unique_topic("otlp_traces");

    // Create exporter with trace partitioning enabled
    let (traces_tx, traces_rx) = bounded(10);

    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_traces_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json)
        .with_partition_traces_by_id(true);

    let mut exporter =
        build_traces_exporter(config, traces_rx).expect("Failed to create Kafka traces exporter");

    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();

    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });

    // Create multiple traces with different trace IDs to increase chance of different partitions
    let trace_ids = vec![
        vec![
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ],
        vec![
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
            0x1f, 0x20,
        ],
        vec![
            0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e,
            0x2f, 0x30,
        ],
        vec![
            0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae,
            0xaf, 0xb0,
        ],
        vec![
            0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe,
            0xff, 0x00,
        ],
    ];

    // Send traces with different trace IDs
    for trace_id in &trace_ids {
        let trace_data = create_test_traces_with_trace_ids(vec![trace_id.clone()]);
        traces_tx
            .send(trace_data)
            .await
            .expect("Failed to send trace data");
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
    for _ in 0..trace_ids.len() {
        if let Some((content, key, partition)) =
            wait_for_message_with_partition(&consumer, TEST_TIMEOUT).await
        {
            messages.push((content, key, partition));
        }
    }

    assert_eq!(
        messages.len(),
        trace_ids.len(),
        "Should receive exactly {} messages",
        trace_ids.len()
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

    // With 5 different trace IDs and 3 partitions, we should expect some distribution
    // This is a probabilistic test - it's very unlikely all 5 different trace IDs hash to the same partition
    assert!(
        used_partitions.len() > 1,
        "Expected messages to be distributed across multiple partitions, but all went to the same partition. Used partitions: {:?}",
        used_partitions
    );

    // Verify all messages have non-empty keys (trace IDs)
    for (_, key, _) in &messages {
        assert!(
            !key.is_empty(),
            "All messages should have non-empty trace ID keys"
        );
        assert_eq!(
            key.len(),
            32,
            "Trace ID keys should be 32 characters (hex encoded 16 bytes)"
        );
    }

    println!(
        "✓ Trace partitioning distribution test passed - traces distributed across {} partitions",
        used_partitions.len()
    );

    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
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
