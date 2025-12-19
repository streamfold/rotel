// SPDX-License-Identifier: Apache-2.0

use serde_json::Value;

use crate::receivers::file::entry::Entry;
use crate::receivers::file::error::{Error, Result};
use super::traits::Parser;

/// A parser that parses JSON strings into structured data.
#[derive(Debug, Clone, Default)]
pub struct JsonParser {
    /// If true, parsing failures return the original entry unchanged.
    /// If false, parsing failures return an error.
    lenient: bool,
}

impl JsonParser {
    /// Create a new JsonParser with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a lenient JsonParser that returns the original entry on parse failure
    /// instead of returning an error.
    pub fn lenient() -> Self {
        Self { lenient: true }
    }

    /// Set whether the parser is lenient.
    pub fn with_lenient(mut self, lenient: bool) -> Self {
        self.lenient = lenient;
        self
    }
}

impl Parser for JsonParser {
    fn parse(&self, mut entry: Entry) -> Result<Entry> {
        // Handle different body types
        let input = match &entry.body {
            Value::String(s) => s.clone(),
            Value::Null => return Ok(entry), // Nothing to parse
            // If already an object, extract fields to attributes
            Value::Object(obj) => {
                // Clone the map to avoid borrow issues
                let fields: Vec<(String, Value)> = obj
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for (key, value) in fields {
                    entry.add_attribute(key, value);
                }
                return Ok(entry);
            }
            Value::Array(_) => return Ok(entry), // Arrays stay as body
            other => {
                if self.lenient {
                    return Ok(entry);
                }
                return Err(Error::Config(format!(
                    "expected string body, got {:?}",
                    other
                )));
            }
        };

        match self.parse_str(&input) {
            Ok(parsed) => {
                // Add parsed fields to attributes (body is preserved)
                if let Value::Object(map) = parsed {
                    for (key, value) in map {
                        entry.add_attribute(key, value);
                    }
                }
                Ok(entry)
            }
            Err(e) => {
                if self.lenient {
                    Ok(entry)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn parse_str(&self, value: &str) -> Result<Value> {
        serde_json::from_str(value).map_err(|e| Error::Config(format!("invalid JSON: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_parser_object() {
        let parser = JsonParser::new();

        let result = parser
            .parse_str(r#"{"name": "test", "count": 42}"#)
            .unwrap();

        assert_eq!(result["name"], "test");
        assert_eq!(result["count"], 42);
    }

    #[test]
    fn test_json_parser_array() {
        let parser = JsonParser::new();

        let result = parser.parse_str(r#"[1, 2, 3]"#).unwrap();

        assert_eq!(result[0], 1);
        assert_eq!(result[1], 2);
        assert_eq!(result[2], 3);
    }

    #[test]
    fn test_json_parser_nested() {
        let parser = JsonParser::new();

        let result = parser
            .parse_str(r#"{"user": {"name": "alice", "age": 30}}"#)
            .unwrap();

        assert_eq!(result["user"]["name"], "alice");
        assert_eq!(result["user"]["age"], 30);
    }

    #[test]
    fn test_json_parser_invalid_json() {
        let parser = JsonParser::new();

        let result = parser.parse_str("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_json_parser_lenient_invalid() {
        let parser = JsonParser::lenient();

        let entry = Entry::with_body("not valid json");
        let parsed = parser.parse(entry).unwrap();

        // Should return original entry unchanged
        assert_eq!(parsed.body_string(), Some("not valid json"));
    }

    #[test]
    fn test_json_parser_with_entry() {
        let parser = JsonParser::new();

        let entry = Entry::with_body(r#"{"message": "hello"}"#);
        let parsed = parser.parse(entry).unwrap();

        // Body should be preserved
        assert_eq!(parsed.body_string(), Some(r#"{"message": "hello"}"#));

        // Parsed fields should be in attributes
        assert_eq!(parsed.attributes["message"], "hello");
    }

    #[test]
    fn test_json_parser_already_object() {
        let parser = JsonParser::new();

        let mut entry = Entry::new();
        entry.body = serde_json::json!({"already": "parsed"});

        let parsed = parser.parse(entry).unwrap();

        // Fields should be extracted to attributes
        assert_eq!(parsed.attributes["already"], "parsed");
    }

    #[test]
    fn test_json_parser_preserves_body_and_metadata() {
        let parser = JsonParser::new();

        let mut entry = Entry::with_body(r#"{"msg": "test"}"#);
        entry.add_attribute_string("source", "app");

        let parsed = parser.parse(entry).unwrap();

        // Body should be preserved
        assert_eq!(parsed.body_string(), Some(r#"{"msg": "test"}"#));

        // Existing attributes should be preserved
        assert_eq!(parsed.attributes.get("source"), Some(&Value::String("app".to_string())));

        // Parsed field should be added to attributes
        assert_eq!(parsed.attributes["msg"], "test");
    }
}
