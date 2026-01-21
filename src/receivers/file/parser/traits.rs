// SPDX-License-Identifier: Apache-2.0

//! Parser traits for transforming log lines into OTLP LogRecords.
//!
//! Parsers extract structured fields from raw log text and output them
//! directly as OTLP KeyValue pairs, avoiding intermediate representations.

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

use crate::receivers::file::error::Result;

/// Result of parsing a log line.
///
/// This is a lightweight struct containing just the parsed fields,
/// which will be used to construct a LogRecord directly.
#[derive(Debug, Default)]
pub struct ParsedLog {
    /// Timestamp in nanoseconds since Unix epoch (for time_unix_nano)
    pub timestamp: Option<u64>,
    /// Parsed attributes (key-value pairs extracted from the log)
    pub attributes: Vec<KeyValue>,
    /// Optional severity number (OTLP severity 1-24)
    pub severity_number: Option<i32>,
    /// Optional severity text
    pub severity_text: Option<String>,
}

impl ParsedLog {
    /// Create a new empty ParsedLog
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a ParsedLog with pre-allocated capacity for attributes
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            timestamp: None,
            attributes: Vec::with_capacity(capacity),
            severity_number: None,
            severity_text: None,
        }
    }

    /// Add a string attribute
    #[inline]
    pub fn add_string(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.push(KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
        });
    }

    /// Add an integer attribute
    #[inline]
    pub fn add_int(&mut self, key: impl Into<String>, value: i64) {
        self.attributes.push(KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(value)),
            }),
        });
    }

    /// Add a double attribute
    #[inline]
    pub fn add_double(&mut self, key: impl Into<String>, value: f64) {
        self.attributes.push(KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::DoubleValue(value)),
            }),
        });
    }

    /// Add a boolean attribute
    #[inline]
    pub fn add_bool(&mut self, key: impl Into<String>, value: bool) {
        self.attributes.push(KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::BoolValue(value)),
            }),
        });
    }

    /// Add an AnyValue attribute
    #[inline]
    pub fn add_any(&mut self, key: impl Into<String>, value: AnyValue) {
        self.attributes.push(KeyValue {
            key: key.into(),
            value: Some(value),
        });
    }
}

/// Parser transforms a log line into parsed attributes.
///
/// Parsers output directly to OTLP types (Vec<KeyValue>) without
/// intermediate representations, minimizing memory allocations.
pub trait Parser: Send + Sync {
    /// Parse a log line and extract structured fields.
    ///
    /// Returns a ParsedLog containing the extracted attributes.
    /// The original log line (body) is NOT included - the caller
    /// is responsible for setting it on the LogRecord.
    fn parse(&self, line: &str) -> Result<ParsedLog>;
}

/// Helper to create an AnyValue from common types
pub fn string_value(s: impl Into<String>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(s.into())),
    }
}

/// Helper to create an AnyValue from an i64
pub fn int_value(i: i64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::IntValue(i)),
    }
}

/// Helper to create an AnyValue from an f64
pub fn double_value(f: f64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::DoubleValue(f)),
    }
}

/// Helper to create an AnyValue from a bool
pub fn bool_value(b: bool) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BoolValue(b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsed_log_add_string() {
        let mut log = ParsedLog::new();
        log.add_string("key", "value");

        assert_eq!(log.attributes.len(), 1);
        assert_eq!(log.attributes[0].key, "key");
        match &log.attributes[0].value {
            Some(AnyValue {
                value: Some(any_value::Value::StringValue(s)),
            }) => assert_eq!(s, "value"),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_parsed_log_add_int() {
        let mut log = ParsedLog::new();
        log.add_int("count", 42);

        assert_eq!(log.attributes.len(), 1);
        match &log.attributes[0].value {
            Some(AnyValue {
                value: Some(any_value::Value::IntValue(i)),
            }) => assert_eq!(*i, 42),
            _ => panic!("Expected int value"),
        }
    }

    #[test]
    fn test_parsed_log_with_capacity() {
        let log = ParsedLog::with_capacity(10);
        assert!(log.attributes.capacity() >= 10);
    }
}
