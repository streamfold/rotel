// SPDX-License-Identifier: Apache-2.0

use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value};
use serde_json::Value;

use super::traits::{ParsedLog, Parser};
use crate::receivers::file::error::{Error, Result};

/// A parser that parses JSON strings into structured data.
#[derive(Debug, Clone, Default)]
pub struct JsonParser {
    /// If true, parsing failures return an empty ParsedLog.
    /// If false, parsing failures return an error.
    lenient: bool,
}

impl JsonParser {
    /// Create a new JsonParser with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a lenient JsonParser that returns an empty ParsedLog on parse failure
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
    fn parse(&self, line: &str) -> Result<ParsedLog> {
        let parsed: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                if self.lenient {
                    return Ok(ParsedLog::new());
                }
                return Err(Error::Config(format!("invalid JSON: {}", e)));
            }
        };

        // Convert JSON object to ParsedLog
        match parsed {
            Value::Object(map) => {
                let mut result = ParsedLog::with_capacity(map.len());
                for (key, value) in map {
                    let any_value = json_to_any_value(value);
                    result.add_any(key, any_value);
                }
                Ok(result)
            }
            _ => {
                if self.lenient {
                    Ok(ParsedLog::new())
                } else {
                    Err(Error::Config(
                        "JSON must be an object at the top level".to_string(),
                    ))
                }
            }
        }
    }
}

/// Convert a serde_json::Value to an OTLP AnyValue
fn json_to_any_value(value: Value) -> AnyValue {
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
            let values: Vec<AnyValue> = arr.into_iter().map(json_to_any_value).collect();
            Some(any_value::Value::ArrayValue(ArrayValue { values }))
        }
        Value::Object(map) => {
            let values: Vec<KeyValue> = map
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k,
                    value: Some(json_to_any_value(v)),
                })
                .collect();
            Some(any_value::Value::KvlistValue(KeyValueList { values }))
        }
    };

    AnyValue { value: inner }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_string_value<'a>(log: &'a ParsedLog, key: &str) -> Option<&'a str> {
        log.attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| match &kv.value {
                Some(av) => match &av.value {
                    Some(any_value::Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                },
                None => None,
            })
    }

    fn get_int_value(log: &ParsedLog, key: &str) -> Option<i64> {
        log.attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| match &kv.value {
                Some(av) => match &av.value {
                    Some(any_value::Value::IntValue(i)) => Some(*i),
                    _ => None,
                },
                None => None,
            })
    }

    fn get_double_value(log: &ParsedLog, key: &str) -> Option<f64> {
        log.attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| match &kv.value {
                Some(av) => match &av.value {
                    Some(any_value::Value::DoubleValue(f)) => Some(*f),
                    _ => None,
                },
                None => None,
            })
    }

    fn get_bool_value(log: &ParsedLog, key: &str) -> Option<bool> {
        log.attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| match &kv.value {
                Some(av) => match &av.value {
                    Some(any_value::Value::BoolValue(b)) => Some(*b),
                    _ => None,
                },
                None => None,
            })
    }

    #[test]
    fn test_json_parser_object() {
        let parser = JsonParser::new();

        let result = parser.parse(r#"{"name": "test", "count": 42}"#).unwrap();

        assert_eq!(get_string_value(&result, "name"), Some("test"));
        assert_eq!(get_int_value(&result, "count"), Some(42));
    }

    #[test]
    fn test_json_parser_types() {
        let parser = JsonParser::new();

        let result = parser
            .parse(r#"{"str": "hello", "int": 42, "float": 3.14, "bool": true}"#)
            .unwrap();

        assert_eq!(get_string_value(&result, "str"), Some("hello"));
        assert_eq!(get_int_value(&result, "int"), Some(42));
        assert_eq!(get_double_value(&result, "float"), Some(3.14));
        assert_eq!(get_bool_value(&result, "bool"), Some(true));
    }

    #[test]
    fn test_json_parser_array_not_object() {
        let parser = JsonParser::new();

        // Arrays at top level should error (non-lenient)
        let result = parser.parse(r#"[1, 2, 3]"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_parser_nested() {
        let parser = JsonParser::new();

        let result = parser
            .parse(r#"{"user": {"name": "alice", "age": 30}}"#)
            .unwrap();

        // Nested object should be a KvlistValue
        let user_attr = result.attributes.iter().find(|kv| kv.key == "user").unwrap();
        match &user_attr.value {
            Some(av) => match &av.value {
                Some(any_value::Value::KvlistValue(kvl)) => {
                    assert_eq!(kvl.values.len(), 2);
                }
                _ => panic!("Expected KvlistValue"),
            },
            None => panic!("Expected value"),
        }
    }

    #[test]
    fn test_json_parser_invalid_json() {
        let parser = JsonParser::new();

        let result = parser.parse("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_json_parser_lenient_invalid() {
        let parser = JsonParser::lenient();

        let result = parser.parse("not valid json").unwrap();

        // Should return empty ParsedLog
        assert!(result.attributes.is_empty());
    }

    #[test]
    fn test_json_parser_with_array_field() {
        let parser = JsonParser::new();

        let result = parser.parse(r#"{"tags": ["a", "b", "c"]}"#).unwrap();

        let tags_attr = result.attributes.iter().find(|kv| kv.key == "tags").unwrap();
        match &tags_attr.value {
            Some(av) => match &av.value {
                Some(any_value::Value::ArrayValue(arr)) => {
                    assert_eq!(arr.values.len(), 3);
                }
                _ => panic!("Expected ArrayValue"),
            },
            None => panic!("Expected value"),
        }
    }
}
