// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {
    use crate::exporters::kafka::config::{KafkaExporterConfig, SerializationFormat};
    use crate::exporters::kafka::errors::KafkaExportError;
    use crate::exporters::kafka::request_builder::{KafkaRequestBuilder, MessageKey};
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
            .with_compression("gzip".to_string());

        assert_eq!(config.brokers, "broker1:9092,broker2:9092");
        assert_eq!(config.traces_topic, Some("my_traces".to_string()));
        assert_eq!(config.metrics_topic, Some("my_metrics".to_string()));
        assert_eq!(config.logs_topic, Some("my_logs".to_string()));
        assert_eq!(config.serialization_format, SerializationFormat::Protobuf);
        assert_eq!(config.compression, Some("gzip".to_string()));
    }

    #[test]
    fn test_kafka_config_with_sasl() {
        let config = KafkaExporterConfig::new("broker:9092".to_string()).with_sasl_auth(
            "username".to_string(),
            "password".to_string(),
            "PLAIN".to_string(),
            "SASL_SSL".to_string(),
        );

        assert_eq!(config.sasl_username, Some("username".to_string()));
        assert_eq!(config.sasl_password, Some("password".to_string()));
        assert_eq!(config.sasl_mechanism, Some("PLAIN".to_string()));
        assert_eq!(config.security_protocol, Some("SASL_SSL".to_string()));
    }

    #[test]
    fn test_message_key_creation() {
        let key = MessageKey::new("traces");
        assert_eq!(key.to_string(), "traces");

        let key_with_id = MessageKey::new("metrics").with_resource_id("service-123".to_string());
        assert_eq!(key_with_id.to_string(), "metrics:service-123");
    }

    #[test]
    fn test_request_builder_traces_json() {
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_spans = vec![ResourceSpans {
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
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    name: "test-span".to_string(),
                    ..Default::default()
                }],
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }];

        let result = builder.build_trace_message(&resource_spans);
        assert!(result.is_ok());

        let (key, payload) = result.unwrap();
        assert_eq!(key.telemetry_type, "traces");
        assert!(!payload.is_empty());
    }

    #[test]
    fn test_request_builder_metrics_protobuf() {
        let builder = KafkaRequestBuilder::new(SerializationFormat::Protobuf);

        let resource_metrics = vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
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

        let result = builder.build_metrics_message(&resource_metrics);
        assert!(result.is_ok());

        let (key, payload) = result.unwrap();
        assert_eq!(key.telemetry_type, "metrics");
        assert!(!payload.is_empty());
    }

    #[test]
    fn test_request_builder_logs_json() {
        let builder = KafkaRequestBuilder::new(SerializationFormat::Json);

        let resource_logs = vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
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

        let result = builder.build_logs_message(&resource_logs);
        assert!(result.is_ok());

        let (key, payload) = result.unwrap();
        assert_eq!(key.telemetry_type, "logs");
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

    use opentelemetry_proto::tonic::metrics::v1::number_data_point;
}
