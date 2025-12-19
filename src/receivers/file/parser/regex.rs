// SPDX-License-Identifier: Apache-2.0

use regex::Regex;
use serde_json::{Map, Value};

use super::traits::Parser;
use crate::receivers::file::entry::Entry;
use crate::receivers::file::error::{Error, Result};

/// A parser that extracts fields from a string using a regular expression
/// with named capture groups.
pub struct RegexParser {
    regex: Regex,
    /// Names of the capture groups (excluding the full match)
    group_names: Vec<String>,
}

impl RegexParser {
    /// Create a new RegexParser from a regex pattern string.
    ///
    /// The pattern must contain at least one named capture group using
    /// the `(?P<name>...)` syntax.
    pub fn new(pattern: &str) -> Result<Self> {
        let regex = Regex::new(pattern)
            .map_err(|e| Error::Config(format!("invalid regex pattern: {}", e)))?;

        // Collect named capture groups
        let group_names: Vec<String> = regex
            .capture_names()
            .skip(1) // Skip the full match (index 0)
            .filter_map(|name| name.map(|s| s.to_string()))
            .collect();

        if group_names.is_empty() {
            return Err(Error::Config(
                "regex pattern must contain at least one named capture group (use (?P<name>...) syntax)".to_string()
            ));
        }

        Ok(Self { regex, group_names })
    }

    /// Get the names of the capture groups in this regex
    pub fn group_names(&self) -> &[String] {
        &self.group_names
    }

    /// Get a reference to the underlying regex
    pub fn regex(&self) -> &Regex {
        &self.regex
    }
}

impl Parser for RegexParser {
    fn parse(&self, mut entry: Entry) -> Result<Entry> {
        // Get the body as a string
        let input = match &entry.body {
            Value::String(s) => s.clone(),
            Value::Null => return Ok(entry), // Nothing to parse
            other => {
                return Err(Error::Config(format!(
                    "expected string body, got {:?}",
                    other
                )));
            }
        };

        // Parse and add fields to attributes (body is preserved)
        let parsed = self.parse_str(&input)?;
        if let Value::Object(map) = parsed {
            for (key, value) in map {
                entry.add_attribute(key, value);
            }
        }
        Ok(entry)
    }

    fn parse_str(&self, value: &str) -> Result<Value> {
        let captures = self.regex.captures(value).ok_or_else(|| {
            Error::Config(format!(
                "regex pattern does not match input: {:?}",
                value.chars().take(100).collect::<String>()
            ))
        })?;

        let mut map = Map::new();

        for name in &self.group_names {
            if let Some(m) = captures.name(name) {
                map.insert(name.clone(), Value::String(m.as_str().to_string()));
            }
        }

        Ok(Value::Object(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regex_parser_simple() {
        let parser = RegexParser::new(r"^(?P<key>\w+)=(?P<value>\w+)$").unwrap();

        let result = parser.parse_str("foo=bar").unwrap();

        assert_eq!(result["key"], "foo");
        assert_eq!(result["value"], "bar");
    }

    #[test]
    fn test_regex_parser_no_named_groups() {
        let result = RegexParser::new(r"^(\w+)=(\w+)$");
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_parser_no_match() {
        let parser = RegexParser::new(r"^(?P<key>\w+)=(?P<value>\w+)$").unwrap();

        let result = parser.parse_str("this does not match");
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_parser_with_entry() {
        let parser = RegexParser::new(r"^(?P<method>\w+) (?P<path>\S+)$").unwrap();

        let entry = Entry::with_body("GET /api/users");
        let parsed = parser.parse(entry).unwrap();

        // Body should be preserved
        assert_eq!(parsed.body_string(), Some("GET /api/users"));

        // Parsed fields should be in attributes
        assert_eq!(parsed.attributes["method"], "GET");
        assert_eq!(parsed.attributes["path"], "/api/users");
    }

    #[test]
    fn test_regex_parser_optional_groups() {
        // Test with optional capture group
        let parser = RegexParser::new(r"^(?P<method>\w+)(?: (?P<path>\S+))?$").unwrap();

        // With path
        let result = parser.parse_str("GET /api").unwrap();
        assert_eq!(result["method"], "GET");
        assert_eq!(result["path"], "/api");

        // Without path - only method is captured
        let result = parser.parse_str("OPTIONS").unwrap();
        assert_eq!(result["method"], "OPTIONS");
        assert!(result.get("path").is_none());
    }

    #[test]
    fn test_regex_parser_group_names() {
        let parser = RegexParser::new(r"(?P<a>\w+)-(?P<b>\w+)-(?P<c>\w+)").unwrap();

        assert_eq!(parser.group_names(), &["a", "b", "c"]);
    }

    #[test]
    fn test_regex_parser_preserves_body_and_metadata() {
        let parser = RegexParser::new(r"^(?P<msg>.*)$").unwrap();

        let mut entry = Entry::with_body("hello world");
        entry.add_attribute_string("env", "test");
        entry.add_resource("host", "localhost");

        let parsed = parser.parse(entry).unwrap();

        // Body should be preserved
        assert_eq!(parsed.body_string(), Some("hello world"));

        // Existing attributes should be preserved
        assert_eq!(
            parsed.attributes.get("env"),
            Some(&Value::String("test".to_string()))
        );
        assert_eq!(parsed.resource.get("host"), Some(&"localhost".to_string()));

        // Parsed field should be added to attributes
        assert_eq!(parsed.attributes["msg"], "hello world");
    }
}
