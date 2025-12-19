use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use super::severity::Severity;

/// Entry is a flexible representation of log data associated with a timestamp.
///
/// This follows the OpenTelemetry log data model:
/// - `body`: The main log content (original log line or selected field)
/// - `attributes`: Parsed fields and metadata
/// - `resource`: Resource-level metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// When the log event occurred
    pub timestamp: DateTime<Utc>,

    /// Severity level of the log entry
    #[serde(default)]
    pub severity: Severity,

    /// Human-readable severity text (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity_text: Option<String>,

    /// The log body - typically the original log line or message
    pub body: Value,

    /// Parsed fields and metadata attributes
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, Value>,

    /// Resource metadata (hostname, service name, etc.)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub resource: HashMap<String, String>,
}

impl Entry {
    /// Create a new entry with the current timestamp and empty body
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now(),
            severity: Severity::default(),
            severity_text: None,
            body: Value::Null,
            attributes: HashMap::new(),
            resource: HashMap::new(),
        }
    }

    /// Create a new entry with the given body value
    pub fn with_body(body: impl Into<Value>) -> Self {
        Self {
            timestamp: Utc::now(),
            severity: Severity::default(),
            severity_text: None,
            body: body.into(),
            attributes: HashMap::new(),
            resource: HashMap::new(),
        }
    }

    /// Add an attribute to the entry (for parsed fields and metadata)
    pub fn add_attribute(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.attributes.insert(key.into(), value.into());
    }

    /// Add a string attribute to the entry
    pub fn add_attribute_string(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.attributes.insert(key.into(), Value::String(value.into()));
    }

    /// Add a resource key to the entry
    pub fn add_resource(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.resource.insert(key.into(), value.into());
    }

    /// Get the body as a string if it is one
    pub fn body_string(&self) -> Option<&str> {
        self.body.as_str()
    }

    /// Get the body as an object if it is one
    pub fn body_object(&self) -> Option<&serde_json::Map<String, Value>> {
        self.body.as_object()
    }

    /// Set the body to a string value
    pub fn set_body_string(&mut self, value: impl Into<String>) {
        self.body = Value::String(value.into());
    }

    /// Set the body to a JSON object
    pub fn set_body_object(&mut self, value: serde_json::Map<String, Value>) {
        self.body = Value::Object(value);
    }

    // Legacy compatibility methods - map to new field names

    /// Create a new entry with the given record value (legacy alias for with_body)
    pub fn with_record(record: impl Into<Value>) -> Self {
        Self::with_body(record)
    }

    /// Add a label to the entry (legacy alias for add_attribute_string)
    pub fn add_label(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.add_attribute_string(key, value);
    }

    /// Get the record as a string (legacy alias for body_string)
    pub fn record_string(&self) -> Option<&str> {
        self.body_string()
    }

    /// Get the record field (legacy - returns body)
    pub fn record(&self) -> &Value {
        &self.body
    }
}

impl Default for Entry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_new() {
        let entry = Entry::new();
        assert_eq!(entry.severity, Severity::Default);
        assert!(entry.attributes.is_empty());
        assert!(entry.resource.is_empty());
    }

    #[test]
    fn test_entry_with_body() {
        let entry = Entry::with_body("test message");
        assert_eq!(entry.body_string(), Some("test message"));
    }

    #[test]
    fn test_entry_add_attribute() {
        let mut entry = Entry::new();
        entry.add_attribute_string("env", "production");
        assert_eq!(
            entry.attributes.get("env"),
            Some(&Value::String("production".to_string()))
        );
    }

    #[test]
    fn test_entry_add_attribute_value() {
        let mut entry = Entry::new();
        entry.add_attribute("count", Value::Number(42.into()));
        assert_eq!(entry.attributes.get("count"), Some(&Value::Number(42.into())));
    }

    #[test]
    fn test_entry_serialization() {
        let mut entry = Entry::new();
        entry.set_body_string("test message");
        entry.severity = Severity::Info;
        entry.add_attribute_string("env", "test");

        let json = serde_json::to_string(&entry).unwrap();
        let parsed: Entry = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.body_string(), Some("test message"));
        assert_eq!(parsed.severity, Severity::Info);
        assert_eq!(
            parsed.attributes.get("env"),
            Some(&Value::String("test".to_string()))
        );
    }

    #[test]
    fn test_legacy_with_record() {
        let entry = Entry::with_record("test message");
        assert_eq!(entry.record_string(), Some("test message"));
    }

    #[test]
    fn test_legacy_add_label() {
        let mut entry = Entry::new();
        entry.add_label("env", "production");
        assert_eq!(
            entry.attributes.get("env"),
            Some(&Value::String("production".to_string()))
        );
    }
}
