// SPDX-License-Identifier: Apache-2.0

use crate::receivers::fluent::message::{EventRecord, EventTimestamp, Message};
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

const LOG_BODY_KEY: &str = "log";

/// Convert a Fluent Message to OTLP ResourceLogs
pub(crate) fn message_to_resource_logs(message: &Message) -> ResourceLogs {
    let (tag, entries) = message.entries();

    // Create log records from entries
    let log_records: Vec<LogRecord> = entries
        .into_iter()
        .map(|(timestamp, record)| convert_event_to_log_record(timestamp, record))
        .collect();

    // Create a single ScopeLogs with the tag as the scope name
    let scope_logs = ScopeLogs {
        scope: Some(InstrumentationScope {
            name: tag.to_string(),
            version: String::new(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }),
        log_records,
        schema_url: String::new(),
    };

    // Create ResourceLogs with a basic resource
    ResourceLogs {
        resource: Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("fluent".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_logs: vec![scope_logs],
        schema_url: String::new(),
    }
}

/// Convert an EventRecord to OTLP LogRecord
fn convert_event_to_log_record(timestamp: &EventTimestamp, record: &EventRecord) -> LogRecord {
    let dt = timestamp.as_datetime();
    let time_unix_nano = dt.timestamp_nanos_opt().unwrap_or(0) as u64;

    // Extract the "log" field as the body, if present
    let body = record
        .get(LOG_BODY_KEY)
        .map(|v| convert_event_value_to_any_value(v.as_value()));

    // Convert all other fields to attributes
    let attributes: Vec<KeyValue> = record
        .iter()
        .filter(|(key, _)| key.as_str() != LOG_BODY_KEY)
        .map(|(key, value)| KeyValue {
            key: key.clone(),
            value: Some(convert_event_value_to_any_value(value.as_value())),
        })
        .collect();

    LogRecord {
        time_unix_nano,
        observed_time_unix_nano: time_unix_nano, // Use same timestamp for observed time
        severity_number: 0,                      // Unspecified
        severity_text: String::new(),
        body,
        attributes,
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![],
        span_id: vec![],
        event_name: String::new(),
    }
}

/// Convert rmpv::Value to OTLP AnyValue
fn convert_event_value_to_any_value(value: &rmpv::Value) -> AnyValue {
    use rmpv::Value;

    let any_value_inner = match value {
        Value::Nil => None,
        Value::Boolean(b) => Some(any_value::Value::BoolValue(*b)),
        Value::Integer(i) => Some(any_value::Value::IntValue(i.as_i64().unwrap_or(0))),
        Value::F32(f) => Some(any_value::Value::DoubleValue(*f as f64)),
        Value::F64(f) => Some(any_value::Value::DoubleValue(*f)),
        Value::String(s) => {
            if let Some(utf8_str) = s.as_str() {
                Some(any_value::Value::StringValue(utf8_str.to_string()))
            } else {
                // Binary string, convert to bytes
                Some(any_value::Value::BytesValue(s.as_bytes().to_vec()))
            }
        }
        Value::Binary(b) => Some(any_value::Value::BytesValue(b.clone())),
        Value::Array(arr) => {
            let values: Vec<AnyValue> = arr
                .iter()
                .map(|v| convert_event_value_to_any_value(v))
                .collect();
            Some(any_value::Value::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue { values },
            ))
        }
        Value::Map(map) => {
            let values: Vec<KeyValue> = map
                .iter()
                .filter_map(|(k, v)| {
                    // Keys must be strings
                    if let Some(key_str) = k.as_str() {
                        Some(KeyValue {
                            key: key_str.to_string(),
                            value: Some(convert_event_value_to_any_value(v)),
                        })
                    } else {
                        None
                    }
                })
                .collect();
            Some(any_value::Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList { values },
            ))
        }
        Value::Ext(_tag, data) => {
            // Extension types are converted to bytes with a special attribute for the tag
            // For now, just convert to bytes
            Some(any_value::Value::BytesValue(data.clone()))
        }
    };

    AnyValue {
        value: any_value_inner,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::fluent::message::EventTimestamp;
    use chrono::{DateTime, Utc};
    use std::collections::BTreeMap;

    #[test]
    fn test_convert_simple_log_record() {
        let timestamp = EventTimestamp::Unix(DateTime::from_timestamp(1234567890, 0).unwrap());
        let mut record = BTreeMap::new();
        record.insert(
            "log".to_string(),
            rmpv::Value::String("Test log message".into()).into(),
        );
        record.insert(
            "container_id".to_string(),
            rmpv::Value::String("abc123".into()).into(),
        );

        let log_record = convert_event_to_log_record(&timestamp, &record);

        assert_eq!(log_record.time_unix_nano, 1234567890000000000);
        assert!(log_record.body.is_some());

        // Check body
        if let Some(AnyValue {
            value: Some(any_value::Value::StringValue(body)),
        }) = log_record.body
        {
            assert_eq!(body, "Test log message");
        } else {
            panic!("Body should be a string");
        }

        // Check attributes
        assert_eq!(log_record.attributes.len(), 1);
        assert_eq!(log_record.attributes[0].key, "container_id");
    }

    #[test]
    fn test_convert_various_value_types() {
        // Test integer
        let int_val = convert_event_value_to_any_value(&rmpv::Value::Integer(42.into()));
        assert!(matches!(
            int_val.value,
            Some(any_value::Value::IntValue(42))
        ));

        // Test boolean
        let bool_val = convert_event_value_to_any_value(&rmpv::Value::Boolean(true));
        assert!(matches!(
            bool_val.value,
            Some(any_value::Value::BoolValue(true))
        ));

        // Test float
        let float_val = convert_event_value_to_any_value(&rmpv::Value::F64(3.14));
        assert!(matches!(
            float_val.value,
            Some(any_value::Value::DoubleValue(_))
        ));

        // Test array
        let array_val = convert_event_value_to_any_value(&rmpv::Value::Array(vec![
            rmpv::Value::Integer(1.into()),
            rmpv::Value::Integer(2.into()),
        ]));
        assert!(matches!(
            array_val.value,
            Some(any_value::Value::ArrayValue(_))
        ));
    }
}
