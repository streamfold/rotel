use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

use crate::receivers::file::error::{Error, Result};

const LABELS_PREFIX: &str = "$labels";
const RESOURCE_PREFIX: &str = "$resource";
const RECORD_PREFIX: &str = "$record";

/// Field represents a potential field on an entry.
/// It is used to get, set, and delete values at this field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Field {
    /// Access the record field, with optional nested keys
    Record(Vec<String>),
    /// Access a label by key
    Label(String),
    /// Access a resource by key
    Resource(String),
    /// Access the timestamp
    Timestamp,
    /// Access the severity
    Severity,
}

impl Field {
    /// Create a new record field from keys
    pub fn record(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Field::Record(keys.into_iter().map(|k| k.into()).collect())
    }

    /// Create a root record field
    pub fn root_record() -> Self {
        Field::Record(vec![])
    }

    /// Create a new label field
    pub fn label(key: impl Into<String>) -> Self {
        Field::Label(key.into())
    }

    /// Create a new resource field
    pub fn resource(key: impl Into<String>) -> Self {
        Field::Resource(key.into())
    }

    /// Parse a field from a string in JSON dot notation
    pub fn parse(s: &str) -> Result<Self> {
        let parts = split_field(s)?;

        if parts.is_empty() {
            return Err(Error::Field("empty field".to_string()));
        }

        match parts[0].as_str() {
            LABELS_PREFIX => {
                if parts.len() != 2 {
                    return Err(Error::Field("labels cannot be nested".to_string()));
                }
                Ok(Field::Label(parts[1].clone()))
            }
            RESOURCE_PREFIX => {
                if parts.len() != 2 {
                    return Err(Error::Field("resource fields cannot be nested".to_string()));
                }
                Ok(Field::Resource(parts[1].clone()))
            }
            RECORD_PREFIX | "$" => Ok(Field::Record(parts[1..].to_vec())),
            "timestamp" if parts.len() == 1 => Ok(Field::Timestamp),
            "severity" if parts.len() == 1 => Ok(Field::Severity),
            _ => Ok(Field::Record(parts)),
        }
    }

    /// Check if this is a root record field
    pub fn is_root_record(&self) -> bool {
        matches!(self, Field::Record(keys) if keys.is_empty())
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::Record(keys) => {
                if keys.is_empty() {
                    write!(f, "{}", RECORD_PREFIX)
                } else {
                    // Check if any key contains dots
                    let contains_dots = keys.iter().any(|k| k.contains('.'));
                    if contains_dots {
                        write!(f, "{}", RECORD_PREFIX)?;
                        for key in keys {
                            write!(f, "['{}']", key)?;
                        }
                        Ok(())
                    } else {
                        write!(f, "{}", keys.join("."))
                    }
                }
            }
            Field::Label(key) => write!(f, "{}.{}", LABELS_PREFIX, key),
            Field::Resource(key) => write!(f, "{}.{}", RESOURCE_PREFIX, key),
            Field::Timestamp => write!(f, "timestamp"),
            Field::Severity => write!(f, "severity"),
        }
    }
}

impl Serialize for Field {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Field::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// State machine for parsing field strings
#[derive(Debug, Clone, Copy, PartialEq)]
enum SplitState {
    Begin,
    InBracket,
    InQuote,
    OutQuote,
    OutBracket,
    InUnbracketedToken,
}

/// Split a field string into its component parts
fn split_field(s: &str) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut state = SplitState::Begin;
    let mut quote_char = ' ';
    let mut token_start = 0;

    let chars: Vec<char> = s.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        match state {
            SplitState::Begin => {
                if c == '[' {
                    state = SplitState::InBracket;
                } else {
                    token_start = i;
                    state = SplitState::InUnbracketedToken;
                }
            }
            SplitState::InBracket => {
                if c != '\'' && c != '"' {
                    return Err(Error::Field(
                        "strings in brackets must be surrounded by quotes".to_string(),
                    ));
                }
                state = SplitState::InQuote;
                quote_char = c;
                token_start = i + 1;
            }
            SplitState::InQuote => {
                if c == quote_char {
                    let token: String = chars[token_start..i].iter().collect();
                    fields.push(token);
                    state = SplitState::OutQuote;
                }
            }
            SplitState::OutQuote => {
                if c != ']' {
                    return Err(Error::Field(
                        "found characters between closed quote and closing bracket".to_string(),
                    ));
                }
                state = SplitState::OutBracket;
            }
            SplitState::OutBracket => match c {
                '.' => {
                    state = SplitState::InUnbracketedToken;
                    token_start = i + 1;
                }
                '[' => {
                    state = SplitState::InBracket;
                }
                _ => {
                    return Err(Error::Field(
                        "bracketed access must be followed by a dot or another bracketed access"
                            .to_string(),
                    ));
                }
            },
            SplitState::InUnbracketedToken => {
                if c == '.' {
                    let token: String = chars[token_start..i].iter().collect();
                    fields.push(token);
                    token_start = i + 1;
                } else if c == '[' {
                    let token: String = chars[token_start..i].iter().collect();
                    fields.push(token);
                    state = SplitState::InBracket;
                }
            }
        }
    }

    match state {
        SplitState::InBracket | SplitState::OutQuote => {
            return Err(Error::Field("found unclosed left bracket".to_string()));
        }
        SplitState::InQuote => {
            let msg = if quote_char == '"' {
                "found unclosed double quote"
            } else {
                "found unclosed single quote"
            };
            return Err(Error::Field(msg.to_string()));
        }
        SplitState::InUnbracketedToken => {
            let token: String = chars[token_start..].iter().collect();
            fields.push(token);
        }
        _ => {}
    }

    Ok(fields)
}

/// Helper trait to access nested JSON values
pub trait FieldAccess {
    fn get_field(&self, keys: &[String]) -> Option<&Value>;
    fn get_field_mut(&mut self, keys: &[String]) -> Option<&mut Value>;
    fn set_field(&mut self, keys: &[String], value: Value);
    fn delete_field(&mut self, keys: &[String]) -> Option<Value>;
}

impl FieldAccess for Value {
    fn get_field(&self, keys: &[String]) -> Option<&Value> {
        if keys.is_empty() {
            return Some(self);
        }

        let mut current = self;
        for key in keys {
            current = current.get(key)?;
        }
        Some(current)
    }

    fn get_field_mut(&mut self, keys: &[String]) -> Option<&mut Value> {
        if keys.is_empty() {
            return Some(self);
        }

        let mut current = self;
        for key in keys {
            current = current.get_mut(key)?;
        }
        Some(current)
    }

    fn set_field(&mut self, keys: &[String], value: Value) {
        if keys.is_empty() {
            *self = value;
            return;
        }

        // Ensure we have an object at the root
        if !self.is_object() {
            *self = Value::Object(serde_json::Map::new());
        }

        // First, ensure all intermediate paths exist
        for i in 0..keys.len() - 1 {
            let needs_creation = {
                let mut current = &*self;
                for key in &keys[..i] {
                    match current.get(key) {
                        Some(v) => current = v,
                        None => break,
                    }
                }
                current.get(&keys[i]).is_none()
                    || !current
                        .get(&keys[i])
                        .map(|v| v.is_object())
                        .unwrap_or(false)
            };

            if needs_creation {
                // Navigate to parent and create the object
                let mut current = &mut *self;
                for key in &keys[..i] {
                    current = current.get_mut(key).unwrap();
                }
                if let Some(obj) = current.as_object_mut() {
                    obj.insert(keys[i].clone(), Value::Object(serde_json::Map::new()));
                }
            }
        }

        // Now set the final value
        let mut current = &mut *self;
        for key in &keys[..keys.len() - 1] {
            current = current.get_mut(key).unwrap();
        }
        if let Some(obj) = current.as_object_mut() {
            obj.insert(keys.last().unwrap().clone(), value);
        }
    }

    fn delete_field(&mut self, keys: &[String]) -> Option<Value> {
        if keys.is_empty() {
            let old = std::mem::replace(self, Value::Null);
            return Some(old);
        }

        if keys.len() == 1 {
            return self.as_object_mut()?.remove(&keys[0]);
        }

        // Navigate to parent
        let mut current = self;
        for key in &keys[..keys.len() - 1] {
            current = current.get_mut(key)?;
        }

        current.as_object_mut()?.remove(keys.last()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_parse_simple() {
        let field = Field::parse("foo").unwrap();
        assert_eq!(field, Field::Record(vec!["foo".to_string()]));
    }

    #[test]
    fn test_field_parse_nested() {
        let field = Field::parse("foo.bar.baz").unwrap();
        assert_eq!(
            field,
            Field::Record(vec![
                "foo".to_string(),
                "bar".to_string(),
                "baz".to_string()
            ])
        );
    }

    #[test]
    fn test_field_parse_labels() {
        let field = Field::parse("$labels.env").unwrap();
        assert_eq!(field, Field::Label("env".to_string()));
    }

    #[test]
    fn test_field_parse_resource() {
        let field = Field::parse("$resource.host").unwrap();
        assert_eq!(field, Field::Resource("host".to_string()));
    }

    #[test]
    fn test_field_parse_record_prefix() {
        let field = Field::parse("$record.message").unwrap();
        assert_eq!(field, Field::Record(vec!["message".to_string()]));
    }

    #[test]
    fn test_field_parse_bracket_notation() {
        let field = Field::parse("$record['foo.bar']").unwrap();
        assert_eq!(field, Field::Record(vec!["foo.bar".to_string()]));
    }

    #[test]
    fn test_field_display() {
        let field = Field::record(["foo", "bar"]);
        assert_eq!(field.to_string(), "foo.bar");

        let field = Field::label("env");
        assert_eq!(field.to_string(), "$labels.env");

        let field = Field::root_record();
        assert_eq!(field.to_string(), "$record");
    }

    #[test]
    fn test_value_field_access() {
        let mut value = serde_json::json!({
            "foo": {
                "bar": "baz"
            }
        });

        // Get
        let keys = vec!["foo".to_string(), "bar".to_string()];
        assert_eq!(
            value.get_field(&keys),
            Some(&Value::String("baz".to_string()))
        );

        // Set
        value.set_field(&keys, Value::String("qux".to_string()));
        assert_eq!(
            value.get_field(&keys),
            Some(&Value::String("qux".to_string()))
        );

        // Delete
        let deleted = value.delete_field(&keys);
        assert_eq!(deleted, Some(Value::String("qux".to_string())));
        assert_eq!(value.get_field(&keys), None);
    }
}
