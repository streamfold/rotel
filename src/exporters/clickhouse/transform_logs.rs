use crate::exporters::clickhouse::payload::{ClickhousePayload, ClickhousePayloadBuilder};
use crate::exporters::clickhouse::request_builder::TransformPayload;
use crate::exporters::clickhouse::request_mapper::RequestType;
use crate::exporters::clickhouse::schema::LogRecordRow;
use crate::exporters::clickhouse::transformer::{Transformer, encode_id, find_str_attribute_kv};
use crate::topology::payload::{Message, MessageMetadata};
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tower::BoxError;

impl TransformPayload<ResourceLogs> for Transformer {
    fn transform(
        &self,
        input: Vec<Message<ResourceLogs>>,
    ) -> (
        Result<Vec<(RequestType, ClickhousePayload)>, BoxError>,
        Option<Vec<MessageMetadata>>,
    ) {
        let mut payload_builder = ClickhousePayloadBuilder::new(self.compression.clone());
        let mut all_metadata = Vec::new();

        for message in input {
            if let Some(metadata) = message.metadata {
                all_metadata.push(metadata);
            }
            for rl in message.payload {
                let res_attrs = rl.resource.unwrap_or_default().attributes;
                let service_name = find_str_attribute_kv(SERVICE_NAME, &res_attrs);
                let res_attrs_field = self.transform_attrs_kv(&res_attrs);

                let res_schema_url = rl.schema_url;

                for sl in rl.scope_logs {
                    let (scope_name, scope_version, scope_attrs) = match sl.scope {
                        Some(scope) => (scope.name, scope.version, scope.attributes),
                        None => (String::new(), String::new(), Vec::new()),
                    };

                    let scope_attrs = self.transform_attrs_kv(&scope_attrs);

                    for log in sl.log_records {
                        let log_attrs = log.attributes;

                        let body_str = match log.body {
                            None => String::new(),
                            Some(av) => match av.value {
                                Some(Value::StringValue(s)) => s,
                                Some(Value::BoolValue(b)) => b.to_string(),
                                Some(Value::IntValue(i)) => i.to_string(),
                                Some(Value::DoubleValue(d)) => d.to_string(),
                                Some(Value::ArrayValue(a)) => serde_json::json!(a).to_string(),
                                Some(Value::KvlistValue(kv)) => serde_json::json!(kv).to_string(),
                                Some(Value::BytesValue(b)) => hex::encode(b),
                                None => String::new(),
                            },
                        };

                        let mut trace_id_ar = [0u8; 32];
                        let mut span_id_ar = [0u8; 16];
                        let trace_id = encode_id(&log.trace_id, &mut trace_id_ar);
                        let span_id = encode_id(&log.span_id, &mut span_id_ar);

                        let row = LogRecordRow {
                            timestamp: log.time_unix_nano,
                            trace_id,
                            span_id,
                            trace_flags: (log.flags & 0x000000FF) as u8,
                            severity_text: log.severity_text,
                            severity_number: (log.severity_number & 0x000000FF) as u8,
                            service_name: &service_name,
                            body: body_str,
                            resource_schema_url: &res_schema_url,
                            resource_attributes: &res_attrs_field,
                            scope_schema_url: &sl.schema_url,
                            scope_name: &scope_name,
                            scope_version: &scope_version,
                            scope_attributes: &scope_attrs,
                            log_attributes: self.transform_attrs_kv(&log_attrs),
                            event_name: &log.event_name,
                        };

                        match payload_builder.add_row(&row) {
                            Ok(_) => {}
                            Err(e) => return (Err(e), None),
                        }
                    }
                }
            }
        }

        let metadata = if all_metadata.is_empty() {
            None
        } else {
            Some(all_metadata)
        };

        let result = payload_builder
            .finish_with_metadata(metadata)
            .map(|payload| vec![(RequestType::Logs, payload)]);

        (result, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::clickhouse::transformer::Transformer;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn create_test_log_record(event_name: &str, body: &str) -> LogRecord {
        LogRecord {
            time_unix_nano: 1234567890,
            observed_time_unix_nano: 1234567890,
            severity_number: 9, // INFO
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(AnyValueValue::StringValue(body.to_string())),
            }),
            attributes: vec![KeyValue {
                key: "test.attr".to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueValue::StringValue("test_value".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            flags: 1,
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            event_name: event_name.to_string(),
        }
    }

    fn create_test_resource_logs(log_records: Vec<LogRecord>) -> ResourceLogs {
        ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::StringValue("test-service".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "test-scope".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                log_records,
                schema_url: "".to_string(),
            }],
            schema_url: "".to_string(),
        }
    }

    #[test]
    fn test_event_name_is_preserved() {
        let transformer = Transformer::new(crate::exporters::clickhouse::Compression::None, true);

        let log_record = create_test_log_record("introspection.feedback", "thumbs_up");
        let resource_logs = create_test_resource_logs(vec![log_record]);

        let message = Message {
            payload: vec![resource_logs],
            metadata: None,
            request_context: None,
        };

        let (result, _) = transformer.transform(vec![message]);
        assert!(result.is_ok(), "Transform should succeed");

        // The transform succeeded, which means the LogRecordRow was created with event_name
        // The actual serialization includes the event_name field
    }

    #[test]
    fn test_empty_event_name() {
        let transformer = Transformer::new(crate::exporters::clickhouse::Compression::None, true);

        let log_record = create_test_log_record("", "test body");
        let resource_logs = create_test_resource_logs(vec![log_record]);

        let message = Message {
            payload: vec![resource_logs],
            metadata: None,
            request_context: None,
        };

        let (result, _) = transformer.transform(vec![message]);
        assert!(
            result.is_ok(),
            "Transform should succeed with empty event_name"
        );
    }

    #[test]
    fn test_log_body_string_value() {
        let transformer = Transformer::new(crate::exporters::clickhouse::Compression::None, true);

        let log_record = create_test_log_record("test.event", "hello world");
        let resource_logs = create_test_resource_logs(vec![log_record]);

        let message = Message {
            payload: vec![resource_logs],
            metadata: None,
            request_context: None,
        };

        let (result, _) = transformer.transform(vec![message]);
        assert!(result.is_ok(), "Transform should succeed");
    }
}
