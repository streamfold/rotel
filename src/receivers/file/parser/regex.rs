// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, NaiveDateTime};
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
    /// Optional field name that contains a timestamp to parse
    timestamp_field: Option<String>,
    /// Chrono format string for parsing the timestamp field
    timestamp_format: Option<String>,
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
            timestamp_field: None,
            timestamp_format: None,
        })
    }

    /// Configure a field to be parsed as a timestamp.
    ///
    /// When the specified field is encountered during parsing, its value
    /// will be parsed using the provided chrono format string and stored
    /// in `ParsedLog.timestamp` as nanoseconds since Unix epoch.
    ///
    /// # Arguments
    /// * `field` - The name of the capture group containing the timestamp
    /// * `format` - A chrono format string (e.g., "%d/%b/%Y:%H:%M:%S %z")
    pub fn with_timestamp(mut self, field: impl Into<String>, format: impl Into<String>) -> Self {
        self.timestamp_field = Some(field.into());
        self.timestamp_format = Some(format.into());
        self
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

/// Common timestamp formats to try when the primary format fails.
/// These cover common non-standard nginx time_local configurations.
const FALLBACK_TIMESTAMP_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S", // ISO-ish: 2019-02-13 20:58:22
    "%Y-%m-%dT%H:%M:%S", // ISO 8601: 2019-02-13T20:58:22
    "%Y/%m/%d %H:%M:%S", // Slash format: 2019/02/13 20:58:22
    "%d/%b/%Y %H:%M:%S", // Without colon separator: 13/Feb/2019 20:58:22
];

/// Try to parse a timestamp value using common naive datetime formats.
/// Assumes UTC for timestamps without timezone information.
fn try_parse_naive_timestamp(value: &str) -> Option<u64> {
    for format in FALLBACK_TIMESTAMP_FORMATS {
        if let Some(nanos) = NaiveDateTime::parse_from_str(value, format)
            .ok()
            .map(|dt| dt.and_utc())
            .and_then(|dt| dt.timestamp_nanos_opt())
            .map(|n| n as u64)
        {
            return Some(nanos);
        }
    }
    None
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
                let value = m.as_str();

                // Check if this field should be parsed as a timestamp
                if let (Some(ts_field), Some(ts_format)) =
                    (&self.timestamp_field, &self.timestamp_format)
                {
                    if name == ts_field {
                        // Parse timestamp and store in result.timestamp
                        // Try the configured format first (may include timezone)
                        if let Some(nanos) = DateTime::parse_from_str(value, ts_format)
                            .ok()
                            .and_then(|dt| dt.timestamp_nanos_opt())
                            .map(|n| n as u64)
                        {
                            result.timestamp = Some(nanos);
                        } else {
                            // Try as naive datetime (no timezone, assume UTC)
                            // This handles formats like "2019-02-13 20:58:22"
                            result.timestamp = try_parse_naive_timestamp(value);
                        }
                    }
                }

                result.add_string(name.clone(), value.to_string());
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

    // Timestamp parsing tests

    /// Nginx time_local format for testing
    const NGINX_TIME_FORMAT: &str = "%d/%b/%Y:%H:%M:%S %z";

    #[test]
    fn test_regex_parser_with_timestamp_timezone_offsets() {
        let parser = RegexParser::new(r"^\[(?P<ts>[^\]]+)\]$")
            .unwrap()
            .with_timestamp("ts", NGINX_TIME_FORMAT);

        // Test negative timezone offset (-0500 = EST)
        // 10:15:32 -0500 = 15:15:32 UTC (5 hours later than +0000)
        let result_neg = parser.parse("[17/Dec/2025:10:15:32 -0500]").unwrap();
        assert!(result_neg.timestamp.is_some());

        // Test positive timezone offset (+0530 = IST)
        // 10:15:32 +0530 = 04:45:32 UTC (5.5 hours earlier than +0000)
        let result_pos = parser.parse("[17/Dec/2025:10:15:32 +0530]").unwrap();
        assert!(result_pos.timestamp.is_some());

        // UTC reference
        let result_utc = parser.parse("[17/Dec/2025:10:15:32 +0000]").unwrap();
        let timestamp_utc = result_utc.timestamp.unwrap();

        // -0500 should be 5 hours (18000 seconds) later than UTC
        let expected_neg = timestamp_utc + (5 * 60 * 60 * 1_000_000_000);
        assert_eq!(result_neg.timestamp.unwrap(), expected_neg);

        // +0530 should be 5.5 hours (19800 seconds) earlier than UTC
        let expected_pos = timestamp_utc - (5 * 60 * 60 * 1_000_000_000 + 30 * 60 * 1_000_000_000);
        assert_eq!(result_pos.timestamp.unwrap(), expected_pos);

        assert_eq!(
            get_string_value(&result_utc, "ts"),
            Some("17/Dec/2025:10:15:32 +0000")
        );
    }

    #[test]
    fn test_regex_parser_with_timestamp_invalid_format() {
        let parser = RegexParser::new(r"^\[(?P<ts>[^\]]+)\]$")
            .unwrap()
            .with_timestamp("ts", NGINX_TIME_FORMAT);

        // Invalid timestamp format - regex matches but timestamp parsing fails
        let result = parser.parse("[invalid timestamp]").unwrap();
        assert!(
            result.timestamp.is_none(),
            "Invalid timestamp should result in None"
        );

        // Completely unparseable date format (month 99, day 99)
        let result = parser.parse("[99/99/2025 10:15:32]").unwrap();
        assert!(result.timestamp.is_none());

        // Partial/truncated date
        let result = parser.parse("[2025-12]").unwrap();
        assert!(result.timestamp.is_none());
    }

    #[test]
    fn test_regex_parser_with_fallback_timestamp_formats() {
        let parser = RegexParser::new(r"^\[(?P<ts>[^\]]+)\]$")
            .unwrap()
            .with_timestamp("ts", NGINX_TIME_FORMAT);

        // ISO-ish format without timezone - now supported via fallback
        let result = parser.parse("[2019-02-13 20:58:22]").unwrap();
        assert!(
            result.timestamp.is_some(),
            "ISO-ish format should parse via fallback"
        );

        // Slash format - also supported via fallback
        let result = parser.parse("[2025/12/17 10:15:32]").unwrap();
        assert!(
            result.timestamp.is_some(),
            "Slash format should parse via fallback"
        );

        // nginx format without timezone but with colon separator - this should NOT parse
        // because we don't have a fallback for %d/%b/%Y:%H:%M:%S (note the colon after year)
        let result = parser.parse("[17/Dec/2025:10:15:32]").unwrap();
        assert!(
            result.timestamp.is_none(),
            "nginx format without timezone should not parse"
        );
    }

    #[test]
    fn test_regex_parser_without_timestamp_config() {
        // Parser without timestamp configuration should not set timestamp
        let parser = RegexParser::new(r"^\[(?P<ts>[^\]]+)\]$").unwrap();

        let result = parser.parse("[17/Dec/2025:10:15:32 +0000]").unwrap();
        assert!(
            result.timestamp.is_none(),
            "Parser without timestamp config should not set timestamp"
        );
    }

    #[test]
    fn test_regex_parser_timestamp_field_not_matched() {
        let parser = RegexParser::new(r"^(?P<message>.+)$")
            .unwrap()
            .with_timestamp("nonexistent_field", NGINX_TIME_FORMAT);

        let result = parser.parse("Hello world").unwrap();
        assert!(
            result.timestamp.is_none(),
            "Non-matching timestamp field should result in None"
        );
    }

    #[test]
    fn test_regex_parser_optional_timestamp_not_present() {
        let parser = RegexParser::new(r"^(?P<message>\w+)(?: \[(?P<ts>[^\]]+)\])?$")
            .unwrap()
            .with_timestamp("ts", NGINX_TIME_FORMAT);

        let result = parser.parse("Hello").unwrap();

        assert!(result.timestamp.is_none());
        assert_eq!(
            result.attributes.iter().find(|kv| kv.key == "ts").is_none(),
            true
        );

        let result_with_ts = parser.parse("Hello [17/Dec/2025:10:15:32 +0000]").unwrap();
        assert!(result_with_ts.timestamp.is_some());
    }
}
