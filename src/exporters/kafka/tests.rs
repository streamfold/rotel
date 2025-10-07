// SPDX-License-Identifier: Apache-2.0
#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::exporters::kafka::config::{
        AcknowledgementMode, Compression, KafkaExporterConfig, PartitionerType, SaslMechanism,
        SecurityProtocol, SerializationFormat,
    };
    use crate::exporters::kafka::errors::KafkaExportError;
    use crate::exporters::kafka::request_builder::KafkaRequestBuilder;
    use crate::topology::payload::Message;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    #[test]
    fn test_kafka_config_builder() {
        let config = KafkaExporterConfig::new("broker1:9092,broker2:9092".to_string())
            .with_traces_topic("my_traces".to_string())
            .with_metrics_topic("my_metrics".to_string())
            .with_logs_topic("my_logs".to_string())
            .with_serialization_format(SerializationFormat::Protobuf)
            .with_compression(Compression::Gzip);

        assert_eq!(config.brokers, "broker1:9092,broker2:9092");
        assert_eq!(config.traces_topic, Some("my_traces".to_string()));
        assert_eq!(config.metrics_topic, Some("my_metrics".to_string()));
        assert_eq!(config.logs_topic, Some("my_logs".to_string()));
        assert_eq!(config.serialization_format, SerializationFormat::Protobuf);
        assert_eq!(config.compression, Compression::Gzip);
    }

    #[test]
    fn test_kafka_config_with_sasl() {
        let config = KafkaExporterConfig::new("broker:9092".to_string()).with_sasl_auth(
            "username".to_string(),
            "password".to_string(),
            SaslMechanism::Plain,
            SecurityProtocol::SaslSsl,
        );

        assert_eq!(config.sasl_username, Some("username".to_string()));
        assert_eq!(config.sasl_password, Some("password".to_string()));
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::Plain));
        assert_eq!(config.security_protocol, Some(SecurityProtocol::SaslSsl));
    }

    #[test]
    fn test_request_builder_traces_json() {
        let builder: KafkaRequestBuilder<ResourceSpans, ExportTraceServiceRequest> =
            KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_spans = vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
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
                    name: "test-span".to_string(),
                    ..Default::default()
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }];

        let result = builder.build_message(resource_spans);
        assert!(result.is_ok());

        let payload = result.unwrap();
        assert!(!payload.is_empty());
    }

    #[test]
    fn test_request_builder_metrics_protobuf() {
        let builder: KafkaRequestBuilder<ResourceMetrics, ExportMetricsServiceRequest> =
            KafkaRequestBuilder::new(SerializationFormat::Protobuf);

        let resource_metrics = vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "test.metric".to_string(),
                    description: "A test metric".to_string(),
                    unit: "1".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1234567890,
                            value: Some(number_data_point::Value::AsDouble(42.0)),
                            ..Default::default()
                        }],
                    })),
                    metadata: vec![],
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }];

        let result = builder.build_message(resource_metrics);
        assert!(result.is_ok());

        let payload = result.unwrap();
        assert!(!payload.is_empty());
    }

    #[test]
    fn test_request_builder_logs_json() {
        let builder: KafkaRequestBuilder<ResourceLogs, ExportLogsServiceRequest> =
            KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_logs = vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1234567890,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "Test log message".to_string(),
                        )),
                    }),
                    ..Default::default()
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }];

        let result = builder.build_message(resource_logs);
        assert!(result.is_ok());

        let payload = result.unwrap();
        assert!(!payload.is_empty());
    }

    #[test]
    fn test_error_conversions() {
        let json_error = serde_json::from_str::<String>("invalid json");
        assert!(json_error.is_err());
        let kafka_error: KafkaExportError = json_error.unwrap_err().into();
        assert!(matches!(kafka_error, KafkaExportError::JsonError(_)));
    }

    #[test]
    fn test_topic_not_configured_error() {
        let error = KafkaExportError::TopicNotConfigured("traces".to_string());
        assert_eq!(
            error.to_string(),
            "Topic not configured for telemetry type: traces"
        );
    }

    #[test]
    fn test_acknowledgement_mode_values() {
        assert_eq!(AcknowledgementMode::None.to_kafka_value(), "0");
        assert_eq!(AcknowledgementMode::One.to_kafka_value(), "1");
        assert_eq!(AcknowledgementMode::All.to_kafka_value(), "all");
    }

    #[test]
    fn test_acknowledgement_mode_default() {
        let mode = AcknowledgementMode::default();
        assert_eq!(mode, AcknowledgementMode::One);
    }

    #[test]
    fn test_kafka_config_with_acks() {
        let config =
            KafkaExporterConfig::new("broker:9092".to_string()).with_acks(AcknowledgementMode::All);

        assert_eq!(config.acks, AcknowledgementMode::All);
    }

    #[test]
    fn test_kafka_config_default_acks() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.acks, AcknowledgementMode::One);
    }

    #[test]
    fn test_client_config_includes_acks() {
        let config =
            KafkaExporterConfig::new("broker:9092".to_string()).with_acks(AcknowledgementMode::All);

        let client_config = config.build_client_config();

        // Unfortunately rdkafka's ClientConfig doesn't expose a way to read back the values,
        // but we can test that the method doesn't panic and returns a config
        assert!(!format!("{:?}", client_config).is_empty());
    }

    #[test]
    fn test_kafka_config_with_client_id() {
        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_client_id("my-custom-client".to_string());

        assert_eq!(config.client_id, "my-custom-client");
    }

    #[test]
    fn test_kafka_config_default_client_id() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.client_id, "rotel");
    }

    #[test]
    fn test_kafka_config_with_max_message_bytes() {
        let config =
            KafkaExporterConfig::new("broker:9092".to_string()).with_max_message_bytes(2000000);
        assert_eq!(config.max_message_bytes, 2000000);
    }

    #[test]
    fn test_kafka_config_default_max_message_bytes() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.max_message_bytes, 1000000);
    }

    #[test]
    fn test_kafka_config_with_linger_ms() {
        let config = KafkaExporterConfig::new("broker:9092".to_string()).with_linger_ms(10);
        assert_eq!(config.linger_ms, 10);
    }

    #[test]
    fn test_kafka_config_default_linger_ms() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.linger_ms, 5);
    }

    #[test]
    fn test_kafka_config_with_retries() {
        let config = KafkaExporterConfig::new("broker:9092".to_string()).with_retries(50);
        assert_eq!(config.retries, 50);
    }

    #[test]
    fn test_kafka_config_default_retries() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.retries, 2147483647);
    }

    #[test]
    fn test_kafka_config_with_retry_backoff_ms() {
        let config = KafkaExporterConfig::new("broker:9092".to_string()).with_retry_backoff_ms(200);
        assert_eq!(config.retry_backoff_ms, 200);
    }

    #[test]
    fn test_kafka_config_default_retry_backoff_ms() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.retry_backoff_ms, 100);
    }

    #[test]
    fn test_kafka_config_with_retry_backoff_max_ms() {
        let config =
            KafkaExporterConfig::new("broker:9092".to_string()).with_retry_backoff_max_ms(2000);
        assert_eq!(config.retry_backoff_max_ms, 2000);
    }

    #[test]
    fn test_kafka_config_default_retry_backoff_max_ms() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.retry_backoff_max_ms, 1000);
    }

    #[test]
    fn test_kafka_config_with_message_timeout_ms() {
        let config =
            KafkaExporterConfig::new("broker:9092".to_string()).with_message_timeout_ms(60000);
        assert_eq!(config.message_timeout_ms, 60000);
    }

    #[test]
    fn test_kafka_config_default_message_timeout_ms() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.message_timeout_ms, 300000);
    }

    #[test]
    fn test_kafka_config_with_request_timeout_ms() {
        let config =
            KafkaExporterConfig::new("broker:9092".to_string()).with_request_timeout_ms(10000);
        assert_eq!(config.request_timeout_ms, 10000);
    }

    #[test]
    fn test_kafka_config_default_request_timeout_ms() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.request_timeout_ms, 30000);
    }

    #[test]
    fn test_kafka_config_with_batch_size() {
        let config = KafkaExporterConfig::new("broker:9092".to_string()).with_batch_size(2000000);
        assert_eq!(config.batch_size, 2000000);
    }

    #[test]
    fn test_kafka_config_default_batch_size() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.batch_size, 1000000);
    }

    #[test]
    fn test_kafka_config_with_custom_config() {
        let custom_config = vec![
            ("enable.idempotence".to_string(), "true".to_string()),
            (
                "max.in.flight.requests.per.connection".to_string(),
                "1".to_string(),
            ),
        ];
        let config =
            KafkaExporterConfig::new("broker:9092".to_string()).with_custom_config(custom_config);

        assert_eq!(
            config.producer_config.get("enable.idempotence"),
            Some(&"true".to_string())
        );
        assert_eq!(
            config
                .producer_config
                .get("max.in.flight.requests.per.connection"),
            Some(&"1".to_string())
        );
        assert_eq!(config.producer_config.len(), 2);
    }

    #[test]
    fn test_kafka_config_default_custom_config() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert!(config.producer_config.is_empty());
    }

    #[test]
    fn test_custom_config_overrides_built_in_options() {
        // Test that custom config takes precedence over built-in options
        let custom_config = vec![
            ("batch.size".to_string(), "2000000".to_string()),
            ("linger.ms".to_string(), "20".to_string()),
        ];
        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_batch_size(500000) // This should be overridden
            .with_linger_ms(10) // This should be overridden
            .with_custom_config(custom_config);

        let client_config = config.build_client_config();

        // Since rdkafka doesn't expose a way to read back values, we can at least
        // verify that the method doesn't panic and produces a valid config
        assert!(!format!("{:?}", client_config).is_empty());

        // Verify the custom config was stored in our structure
        assert_eq!(
            config.producer_config.get("batch.size"),
            Some(&"2000000".to_string())
        );
        assert_eq!(
            config.producer_config.get("linger.ms"),
            Some(&"20".to_string())
        );
        assert_eq!(config.batch_size, 500000); // Built-in value should still be stored
        assert_eq!(config.linger_ms, 10); // Built-in value should still be stored
    }

    #[test]
    fn test_kafka_config_with_partitioner() {
        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partitioner(PartitionerType::Murmur2);
        assert_eq!(config.partitioner, Some(PartitionerType::Murmur2));
    }

    #[test]
    fn test_kafka_config_default_partitioner() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.partitioner, Some(PartitionerType::ConsistentRandom));
    }

    #[test]
    fn test_partitioner_type_kafka_values() {
        assert_eq!(PartitionerType::Consistent.to_kafka_value(), "consistent");
        assert_eq!(
            PartitionerType::ConsistentRandom.to_kafka_value(),
            "consistent_random"
        );
        assert_eq!(
            PartitionerType::Murmur2Random.to_kafka_value(),
            "murmur2_random"
        );
        assert_eq!(PartitionerType::Murmur2.to_kafka_value(), "murmur2");
        assert_eq!(PartitionerType::Fnv1a.to_kafka_value(), "fnv1a");
        assert_eq!(
            PartitionerType::Fnv1aRandom.to_kafka_value(),
            "fnv1a_random"
        );
    }

    #[test]
    fn test_kafka_config_with_partition_metrics_by_resource_attributes() {
        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_metrics_by_resource_attributes(true);
        assert_eq!(config.partition_metrics_by_resource_attributes, true);
    }

    #[test]
    fn test_kafka_config_default_partition_metrics_by_resource_attributes() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.partition_metrics_by_resource_attributes, false);
    }

    #[test]
    fn test_kafka_config_with_partition_logs_by_resource_attributes() {
        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_logs_by_resource_attributes(true);
        assert_eq!(config.partition_logs_by_resource_attributes, true);
    }

    #[test]
    fn test_kafka_config_default_partition_logs_by_resource_attributes() {
        let config = KafkaExporterConfig::new("broker:9092".to_string());
        assert_eq!(config.partition_logs_by_resource_attributes, false);
    }

    use opentelemetry_proto::tonic::metrics::v1::number_data_point;

    #[test]
    fn test_traces_not_partitioned() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string());
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_spans = vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    name: "test-span".to_string(),
                    ..Default::default()
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }];

        let result = ResourceSpans::build_kafka_message(
            &builder,
            &config,
            vec![Message {
                metadata: None,
                payload: resource_spans,
            }],
        );
        assert!(result.is_ok());

        let (key, _, _) = result.unwrap();
        assert_eq!(key.to_string(), ""); // Should always be empty
    }

    #[test]
    fn test_traces_not_split() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string());

        let resource_spans = vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_spans: vec![],
            schema_url: "".to_string(),
        }];

        // Test that splitting is disabled
        let split_result = ResourceSpans::split_for_partitioning(
            &config,
            vec![Message {
                metadata: None,
                payload: resource_spans.clone(),
            }],
        );

        // Should return original data unchanged
        assert_eq!(split_result.len(), 1);
        assert_eq!(split_result[0].len(), 1);
        assert_eq!(split_result[0][0].payload, resource_spans);
    }

    #[test]
    fn test_partition_logs_by_resource_attributes_disabled() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_logs_by_resource_attributes(false);
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_logs = vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![],
            schema_url: "".to_string(),
        }];

        let result = ResourceLogs::build_kafka_message(
            &builder,
            &config,
            vec![Message {
                metadata: None,
                payload: resource_logs,
            }],
        );
        assert!(result.is_ok());

        let (key, _, _) = result.unwrap();
        assert_eq!(key.to_string(), ""); // Should be empty when disabled
    }

    #[test]
    fn test_partition_logs_by_resource_attributes_enabled() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_logs_by_resource_attributes(true);
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_logs = vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![],
            schema_url: "".to_string(),
        }];

        let result = ResourceLogs::build_kafka_message(
            &builder,
            &config,
            vec![Message {
                metadata: None,
                payload: resource_logs,
            }],
        );
        assert!(result.is_ok());

        let (key, _, _) = result.unwrap();
        assert!(!key.to_string().is_empty()); // Should have a hash key when enabled
    }

    #[test]
    fn test_partition_logs_by_resource_attributes_empty_attributes() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_logs_by_resource_attributes(true);
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_logs = vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![], // Empty attributes
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![],
            schema_url: "".to_string(),
        }];

        let result = ResourceLogs::build_kafka_message(
            &builder,
            &config,
            vec![Message {
                metadata: None,
                payload: resource_logs,
            }],
        );
        assert!(result.is_ok());

        let (key, _, _) = result.unwrap();
        assert_eq!(key.to_string(), ""); // Should be empty when no attributes
    }

    #[test]
    fn test_split_logs_by_resource_attributes() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_logs_by_resource_attributes(true);

        let resource_logs = vec![
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("service-1".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![],
                schema_url: "".to_string(),
            },
            ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("service-2".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![],
                schema_url: "".to_string(),
            },
        ];

        // Test splitting
        let split_result = ResourceLogs::split_for_partitioning(
            &config,
            vec![Message {
                metadata: None,
                payload: resource_logs,
            }],
        );

        // Should be split into 2 groups (one for each ResourceLogs)
        assert_eq!(split_result.len(), 2);

        // Each group should contain exactly one Message with one ResourceLogs
        for group in &split_result {
            assert_eq!(group.len(), 1);
            assert_eq!(group[0].payload.len(), 1);
        }
    }

    #[test]
    fn test_split_logs_disabled() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_logs_by_resource_attributes(false);

        let resource_logs = vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![],
            schema_url: "".to_string(),
        }];

        // Test that splitting is disabled
        let split_result = ResourceLogs::split_for_partitioning(
            &config,
            vec![Message {
                metadata: None,
                payload: resource_logs.clone(),
            }],
        );

        // Should return original data unchanged
        assert_eq!(split_result.len(), 1);
        assert_eq!(split_result[0].len(), 1);
        assert_eq!(split_result[0][0].payload, resource_logs);
    }

    #[test]
    fn test_partition_metrics_by_resource_attributes_disabled() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_metrics_by_resource_attributes(false);
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_metrics = vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_metrics: vec![],
            schema_url: "".to_string(),
        }];

        let result = ResourceMetrics::build_kafka_message(
            &builder,
            &config,
            vec![Message {
                metadata: None,
                payload: resource_metrics,
            }],
        );
        assert!(result.is_ok());

        let (key, _, _) = result.unwrap();
        assert_eq!(key.to_string(), ""); // Should be empty when disabled
    }

    #[test]
    fn test_partition_metrics_by_resource_attributes_enabled() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_metrics_by_resource_attributes(true);
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_metrics = vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_metrics: vec![],
            schema_url: "".to_string(),
        }];

        let result = ResourceMetrics::build_kafka_message(
            &builder,
            &config,
            vec![Message {
                metadata: None,
                payload: resource_metrics,
            }],
        );
        assert!(result.is_ok());

        let (key, _, _) = result.unwrap();
        assert!(!key.to_string().is_empty()); // Should have a hash key when enabled
    }

    #[test]
    fn test_partition_metrics_by_resource_attributes_empty_attributes() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_metrics_by_resource_attributes(true);
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_metrics = vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![], // Empty attributes
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_metrics: vec![],
            schema_url: "".to_string(),
        }];

        let result = ResourceMetrics::build_kafka_message(
            &builder,
            &config,
            vec![Message {
                metadata: None,
                payload: resource_metrics,
            }],
        );
        assert!(result.is_ok());

        let (key, _, _) = result.unwrap();
        assert_eq!(key.to_string(), ""); // Should be empty when no attributes
    }

    #[test]
    fn test_split_metrics_by_resource_attributes() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_metrics_by_resource_attributes(true);

        let resource_metrics = vec![
            ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("service-1".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![],
                schema_url: "".to_string(),
            },
            ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("service-2".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![],
                schema_url: "".to_string(),
            },
        ];

        // Test splitting
        let split_result = ResourceMetrics::split_for_partitioning(
            &config,
            vec![Message {
                metadata: None,
                payload: resource_metrics,
            }],
        );

        // Should be split into 2 groups (one for each ResourceMetrics)
        assert_eq!(split_result.len(), 2);

        // Each group should contain exactly one Message with one ResourceMetrics
        for group in &split_result {
            assert_eq!(group.len(), 1);
            assert_eq!(group[0].payload.len(), 1);
        }
    }

    #[test]
    fn test_split_metrics_disabled() {
        use crate::exporters::kafka::exporter::KafkaExportable;

        let config = KafkaExporterConfig::new("broker:9092".to_string())
            .with_partition_metrics_by_resource_attributes(false);

        let resource_metrics = vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_metrics: vec![],
            schema_url: "".to_string(),
        }];

        // Test that splitting is disabled
        let split_result = ResourceMetrics::split_for_partitioning(
            &config,
            vec![Message {
                metadata: None,
                payload: resource_metrics.clone(),
            }],
        );

        // Should return original data unchanged
        assert_eq!(split_result.len(), 1);
        assert_eq!(split_result[0].len(), 1);
        assert_eq!(split_result[0][0].payload, resource_metrics);
    }
}
