// SPDX-License-Identifier: Apache-2.0

use regex::Regex;

use super::traits::{ParsedLog, Parser};
use crate::receivers::file::error::{Error, Result};

/// A parser that extracts fields from a string using a regular expression
/// with named capture groups.
pub struct RegexParser {
    regex: Regex,
    /// Names of the capture groups (excluding the full match)
    group_names: Vec<String>,
    /// Number of capture groups (for pre-allocation)
    num_groups: usize,
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

        let num_groups = group_names.len();

        Ok(Self {
            regex,
            group_names,
            num_groups,
        })
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
    fn parse(&self, line: &str) -> Result<ParsedLog> {
        let captures = self.regex.captures(line).ok_or_else(|| {
            Error::Config(format!(
                "regex pattern does not match input: {:?}",
                line.chars().take(100).collect::<String>()
            ))
        })?;

        // Pre-allocate with known capacity
        let mut result = ParsedLog::with_capacity(self.num_groups);

        for name in &self.group_names {
            if let Some(m) = captures.name(name) {
                result.add_string(name.clone(), m.as_str().to_string());
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value;

    fn get_string_value<'a>(log: &'a ParsedLog, key: &str) -> Option<&'a str> {
        log.attributes.iter().find(|kv| kv.key == key).and_then(|kv| {
            match &kv.value {
                Some(av) => match &av.value {
                    Some(any_value::Value::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                },
                None => None,
            }
        })
    }

    #[test]
    fn test_regex_parser_simple() {
        let parser = RegexParser::new(r"^(?P<key>\w+)=(?P<value>\w+)$").unwrap();

        let result = parser.parse("foo=bar").unwrap();

        assert_eq!(get_string_value(&result, "key"), Some("foo"));
        assert_eq!(get_string_value(&result, "value"), Some("bar"));
    }

    #[test]
    fn test_regex_parser_no_named_groups() {
        let result = RegexParser::new(r"^(\w+)=(\w+)$");
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_parser_no_match() {
        let parser = RegexParser::new(r"^(?P<key>\w+)=(?P<value>\w+)$").unwrap();

        let result = parser.parse("this does not match");
        assert!(result.is_err());
    }

    #[test]
    fn test_regex_parser_method_path() {
        let parser = RegexParser::new(r"^(?P<method>\w+) (?P<path>\S+)$").unwrap();

        let result = parser.parse("GET /api/users").unwrap();

        assert_eq!(get_string_value(&result, "method"), Some("GET"));
        assert_eq!(get_string_value(&result, "path"), Some("/api/users"));
    }

    #[test]
    fn test_regex_parser_optional_groups() {
        // Test with optional capture group
        let parser = RegexParser::new(r"^(?P<method>\w+)(?: (?P<path>\S+))?$").unwrap();

        // With path
        let result = parser.parse("GET /api").unwrap();
        assert_eq!(get_string_value(&result, "method"), Some("GET"));
        assert_eq!(get_string_value(&result, "path"), Some("/api"));

        // Without path - only method is captured
        let result = parser.parse("OPTIONS").unwrap();
        assert_eq!(get_string_value(&result, "method"), Some("OPTIONS"));
        assert_eq!(get_string_value(&result, "path"), None);
    }

    #[test]
    fn test_regex_parser_group_names() {
        let parser = RegexParser::new(r"(?P<a>\w+)-(?P<b>\w+)-(?P<c>\w+)").unwrap();

        assert_eq!(parser.group_names(), &["a", "b", "c"]);
    }

    #[test]
    fn test_regex_parser_preallocates() {
        let parser = RegexParser::new(r"(?P<a>\w+)-(?P<b>\w+)-(?P<c>\w+)").unwrap();

        let result = parser.parse("x-y-z").unwrap();
        // Should have exactly 3 attributes
        assert_eq!(result.attributes.len(), 3);
    }
}
