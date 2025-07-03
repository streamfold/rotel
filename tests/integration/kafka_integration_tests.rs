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

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use rdkafka::util::Timeout;
use rotel::bounded_channel::bounded;
use rotel::exporters::kafka::config::{KafkaExporterConfig, SerializationFormat};
use rotel::exporters::kafka::KafkaExporter;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use utilities::otlp::FakeOTLP;

const KAFKA_BROKER: &str = "localhost:9092";
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

async fn setup_consumer(topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", format!("test-consumer-{}", uuid::Uuid::new_v4()))
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
    }).await;
    
    result.unwrap_or(None)
}

#[tokio::test]
async fn test_kafka_exporter_traces_json() {
    tracing_subscriber::fmt::init();
    
    let topic = "otlp_traces";
    let consumer = setup_consumer(topic).await;
    
    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;
    
    // Create exporter
    let (traces_tx, traces_rx) = bounded(10);
    let (metrics_tx, metrics_rx) = bounded(10);
    let (logs_tx, logs_rx) = bounded(10);
    
    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_traces_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json);
    
    let mut exporter = KafkaExporter::new(config, traces_rx, metrics_rx, logs_rx)
        .expect("Failed to create Kafka exporter");
    
    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();
    
    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });
    
    // Send test trace data
    let trace_data = vec![FakeOTLP::trace_service_request().resource_spans[0].clone()];
    traces_tx.send(trace_data).await.expect("Failed to send trace data");
    
    // Wait for message to be consumed
    let message = wait_for_message(&consumer, TEST_TIMEOUT).await;
    assert!(message.is_some(), "No message received from Kafka");
    
    let message_content = message.unwrap();
    println!("Received message: {}", message_content);
    
    // Verify it's valid JSON
    let json: Value = serde_json::from_str(&message_content)
        .expect("Message is not valid JSON");
    
    // Verify it contains trace data
    assert!(json.is_array(), "Message should be an array");
    let traces = json.as_array().unwrap();
    assert!(!traces.is_empty(), "Traces array should not be empty");
    
    // Verify trace structure
    let trace = &traces[0];
    assert!(trace.get("resource").is_some(), "Trace should have resource");
    assert!(trace.get("scope_spans").is_some(), "Trace should have scope_spans");
    
    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
    
    // Drop senders to avoid warnings
    drop(metrics_tx);
    drop(logs_tx);
}

#[tokio::test]
async fn test_kafka_exporter_metrics_protobuf() {
    let topic = "otlp_metrics";
    let consumer = setup_consumer(topic).await;
    
    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;
    
    // Create exporter
    let (traces_tx, traces_rx) = bounded(10);
    let (metrics_tx, metrics_rx) = bounded(10);
    let (logs_tx, logs_rx) = bounded(10);
    
    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_metrics_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Protobuf);
    
    let mut exporter = KafkaExporter::new(config, traces_rx, metrics_rx, logs_rx)
        .expect("Failed to create Kafka exporter");
    
    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();
    
    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });
    
    // Send test metrics data
    let metrics_data = vec![FakeOTLP::metrics_service_request().resource_metrics[0].clone()];
    metrics_tx.send(metrics_data).await.expect("Failed to send metrics data");
    
    // Wait for message to be consumed
    let message = wait_for_message(&consumer, TEST_TIMEOUT).await;
    assert!(message.is_some(), "No message received from Kafka");
    
    let message_content = message.unwrap();
    println!("Received protobuf message length: {} bytes", message_content.len());
    
    // For protobuf, we can't easily parse as JSON, but we can verify it's binary data
    assert!(!message_content.is_empty(), "Protobuf message should not be empty");
    
    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
    
    // Drop senders to avoid warnings
    drop(traces_tx);
    drop(logs_tx);
}

#[tokio::test]
async fn test_kafka_exporter_logs_with_compression() {
    let topic = "otlp_logs";
    let consumer = setup_consumer(topic).await;
    
    // Give consumer time to connect
    sleep(Duration::from_secs(2)).await;
    
    // Create exporter with compression
    let (traces_tx, traces_rx) = bounded(10);
    let (metrics_tx, metrics_rx) = bounded(10);
    let (logs_tx, logs_rx) = bounded(10);
    
    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_logs_topic(topic.to_string())
        .with_serialization_format(SerializationFormat::Json)
        .with_compression("gzip".to_string());
    
    let mut exporter = KafkaExporter::new(config, traces_rx, metrics_rx, logs_rx)
        .expect("Failed to create Kafka exporter");
    
    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();
    
    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });
    
    // Send test logs data
    let logs_data = vec![FakeOTLP::logs_service_request().resource_logs[0].clone()];
    logs_tx.send(logs_data).await.expect("Failed to send logs data");
    
    // Wait for message to be consumed
    let message = wait_for_message(&consumer, TEST_TIMEOUT).await;
    assert!(message.is_some(), "No message received from Kafka");
    
    let message_content = message.unwrap();
    println!("Received compressed message: {}", message_content);
    
    // Verify it's valid JSON (consumer should decompress automatically)
    let json: Value = serde_json::from_str(&message_content)
        .expect("Message is not valid JSON");
    
    // Verify it contains log data
    assert!(json.is_array(), "Message should be an array");
    let logs = json.as_array().unwrap();
    assert!(!logs.is_empty(), "Logs array should not be empty");
    
    // Verify log structure
    let log = &logs[0];
    assert!(log.get("resource").is_some(), "Log should have resource");
    assert!(log.get("scope_logs").is_some(), "Log should have scope_logs");
    
    // Clean up
    cancel_token.cancel();
    let _ = exporter_handle.await;
    
    // Drop senders to avoid warnings
    drop(traces_tx);
    drop(metrics_tx);
}

#[tokio::test]
async fn test_kafka_exporter_multiple_telemetry_types() {
    let consumer_traces = setup_consumer("otlp_traces").await;
    let consumer_metrics = setup_consumer("otlp_metrics").await;
    let consumer_logs = setup_consumer("otlp_logs").await;
    
    // Give consumers time to connect
    sleep(Duration::from_secs(2)).await;
    
    // Create exporter
    let (traces_tx, traces_rx) = bounded(10);
    let (metrics_tx, metrics_rx) = bounded(10);
    let (logs_tx, logs_rx) = bounded(10);
    
    let config = KafkaExporterConfig::new(KAFKA_BROKER.to_string())
        .with_serialization_format(SerializationFormat::Json);
    
    let mut exporter = KafkaExporter::new(config, traces_rx, metrics_rx, logs_rx)
        .expect("Failed to create Kafka exporter");
    
    let cancel_token = CancellationToken::new();
    let exporter_token = cancel_token.clone();
    
    // Start exporter
    let exporter_handle = tokio::spawn(async move {
        exporter.start(exporter_token).await;
    });
    
    // Send all types of telemetry data
    let trace_data = vec![FakeOTLP::trace_service_request().resource_spans[0].clone()];
    let metrics_data = vec![FakeOTLP::metrics_service_request().resource_metrics[0].clone()];
    let logs_data = vec![FakeOTLP::logs_service_request().resource_logs[0].clone()];
    
    traces_tx.send(trace_data).await.expect("Failed to send trace data");
    metrics_tx.send(metrics_data).await.expect("Failed to send metrics data");
    logs_tx.send(logs_data).await.expect("Failed to send logs data");
    
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