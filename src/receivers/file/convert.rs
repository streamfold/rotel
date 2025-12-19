// SPDX-License-Identifier: Apache-2.0

//! Conversion from Entry to OTLP ResourceLogs.

use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use serde_json::Value;

use crate::receivers::file::entry::{Entry, Severity};

/// Convert a batch of entries to OTLP ResourceLogs
pub fn convert_entries_to_resource_logs(entries: Vec<Entry>) -> ResourceLogs {
    let log_records: Vec<LogRecord> = entries
        .into_iter()
        .map(convert_entry_to_log_record)
        .collect();

    let scope_logs = ScopeLogs {
        scope: Some(InstrumentationScope {
            name: "rotel.file".to_string(),
            version: String::new(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }),
        log_records,
        schema_url: String::new(),
    };

    ResourceLogs {
        resource: Some(Resource {
            attributes: vec![],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_logs: vec![scope_logs],
        schema_url: String::new(),
    }
}

/// Convert a single Entry to an OTLP LogRecord
fn convert_entry_to_log_record(entry: Entry) -> LogRecord {
    let time_unix_nano = entry.timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;

    // Convert severity
    let severity_number = severity_to_otel_number(entry.severity);
    let severity_text = entry
        .severity_text
        .unwrap_or_else(|| severity_to_text(entry.severity));

    // Convert body (the original log line or message)
    let body = Some(convert_value_to_any_value(entry.body));

    // Convert attributes (parsed fields and metadata)
    let mut attributes: Vec<KeyValue> = entry
        .attributes
        .into_iter()
        .map(|(key, value)| KeyValue {
            key,
            value: Some(convert_value_to_any_value(value)),
        })
        .collect();

    // Add resource attributes as regular attributes (prefixed)
    for (key, value) in entry.resource {
        attributes.push(KeyValue {
            key: format!("resource.{}", key),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value)),
            }),
        });
    }

    LogRecord {
        time_unix_nano,
        observed_time_unix_nano: time_unix_nano,
        severity_number,
        severity_text,
        body,
        attributes,
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![],
        span_id: vec![],
        event_name: String::new(),
    }
}

/// Convert Entry Severity to OTLP severity number
fn severity_to_otel_number(severity: Severity) -> i32 {
    // Map to OTEL severity numbers (1-24)
    match severity {
        Severity::Default => 0,
        Severity::Trace => 1,
        Severity::Trace2 => 2,
        Severity::Trace3 => 3,
        Severity::Trace4 => 4,
        Severity::Debug => 5,
        Severity::Debug2 => 6,
        Severity::Debug3 => 7,
        Severity::Debug4 => 8,
        Severity::Info => 9,
        Severity::Info2 => 10,
        Severity::Info3 => 11,
        Severity::Info4 => 12,
        Severity::Notice => 13,
        Severity::Warning => 13,
        Severity::Warning2 => 14,
        Severity::Warning3 => 15,
        Severity::Warning4 => 16,
        Severity::Error => 17,
        Severity::Error2 => 18,
        Severity::Error3 => 19,
        Severity::Error4 => 20,
        Severity::Critical => 21,
        Severity::Alert => 22,
        Severity::Emergency => 23,
        Severity::Emergency2 => 23,
        Severity::Emergency3 => 23,
        Severity::Emergency4 => 24,
        Severity::Catastrophe => 24,
    }
}

/// Convert severity to text representation
fn severity_to_text(severity: Severity) -> String {
    match severity {
        Severity::Default => "UNSPECIFIED".to_string(),
        Severity::Trace | Severity::Trace2 | Severity::Trace3 | Severity::Trace4 => {
            "TRACE".to_string()
        }
        Severity::Debug | Severity::Debug2 | Severity::Debug3 | Severity::Debug4 => {
            "DEBUG".to_string()
        }
        Severity::Info | Severity::Info2 | Severity::Info3 | Severity::Info4 => "INFO".to_string(),
        Severity::Notice => "NOTICE".to_string(),
        Severity::Warning | Severity::Warning2 | Severity::Warning3 | Severity::Warning4 => {
            "WARN".to_string()
        }
        Severity::Error | Severity::Error2 | Severity::Error3 | Severity::Error4 => {
            "ERROR".to_string()
        }
        Severity::Critical => "CRITICAL".to_string(),
        Severity::Alert => "ALERT".to_string(),
        Severity::Emergency
        | Severity::Emergency2
        | Severity::Emergency3
        | Severity::Emergency4 => "EMERGENCY".to_string(),
        Severity::Catastrophe => "FATAL".to_string(),
    }
}

/// Convert a serde_json::Value to OTLP AnyValue
fn convert_value_to_any_value(value: Value) -> AnyValue {
    let inner = match value {
        Value::Null => None,
        Value::Bool(b) => Some(any_value::Value::BoolValue(b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(any_value::Value::IntValue(i))
            } else if let Some(f) = n.as_f64() {
                Some(any_value::Value::DoubleValue(f))
            } else {
                Some(any_value::Value::StringValue(n.to_string()))
            }
        }
        Value::String(s) => Some(any_value::Value::StringValue(s)),
        Value::Array(arr) => {
            let values: Vec<AnyValue> = arr.into_iter().map(convert_value_to_any_value).collect();
            Some(any_value::Value::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue { values },
            ))
        }
        Value::Object(map) => {
            let values: Vec<KeyValue> = map
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k,
                    value: Some(convert_value_to_any_value(v)),
                })
                .collect();
            Some(any_value::Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList { values },
            ))
        }
    };

    AnyValue { value: inner }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_simple_entry() {
        let mut entry = Entry::with_body("test log message");
        entry.add_attribute_string("env", "test");

        let resource_logs = convert_entries_to_resource_logs(vec![entry]);

        assert_eq!(resource_logs.scope_logs.len(), 1);
        assert_eq!(resource_logs.scope_logs[0].log_records.len(), 1);

        let log_record = &resource_logs.scope_logs[0].log_records[0];
        assert!(log_record.time_unix_nano > 0);

        // Check body is preserved as string
        if let Some(ref body) = log_record.body {
            if let Some(any_value::Value::StringValue(ref s)) = body.value {
                assert_eq!(s, "test log message");
            } else {
                panic!("Expected string body");
            }
        }

        // Check attributes
        assert!(log_record.attributes.iter().any(|a| a.key == "env"));
    }

    #[test]
    fn test_convert_entry_with_parsed_attributes() {
        let mut entry = Entry::with_body("192.168.1.1 - GET /api HTTP/1.1");
        entry.add_attribute("remote_addr", Value::String("192.168.1.1".to_string()));
        entry.add_attribute("method", Value::String("GET".to_string()));
        entry.add_attribute("path", Value::String("/api".to_string()));
        entry.add_attribute("status", Value::Number(200.into()));

        let resource_logs = convert_entries_to_resource_logs(vec![entry]);
        let log_record = &resource_logs.scope_logs[0].log_records[0];

        // Body should be the original log line
        if let Some(ref body) = log_record.body {
            if let Some(any_value::Value::StringValue(ref s)) = body.value {
                assert_eq!(s, "192.168.1.1 - GET /api HTTP/1.1");
            } else {
                panic!("Expected string body");
            }
        }

        // Parsed fields should be in attributes
        assert!(log_record.attributes.iter().any(|a| a.key == "remote_addr"));
        assert!(log_record.attributes.iter().any(|a| a.key == "method"));
        assert!(log_record.attributes.iter().any(|a| a.key == "status"));
    }

    #[test]
    fn test_severity_conversion() {
        let mut entry = Entry::with_body("error message");
        entry.severity = Severity::Error;

        let resource_logs = convert_entries_to_resource_logs(vec![entry]);
        let log_record = &resource_logs.scope_logs[0].log_records[0];

        assert_eq!(log_record.severity_number, 17); // OTEL Error severity
        assert_eq!(log_record.severity_text, "ERROR");
    }
}
