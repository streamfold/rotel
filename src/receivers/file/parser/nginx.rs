//! Nginx log parsing presets.
//!
//! This module provides pre-configured parsers for common nginx log formats.
//!
//! # Supported Formats
//!
//! ## Access Log (Combined Format)
//!
//! The default nginx access log format:
//! ```text
//! $remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
//! ```
//!
//! Example:
//! ```text
//! 192.168.1.1 - - [17/Dec/2025:10:15:32 +0000] "GET /api/users HTTP/1.1" 200 1234 "https://example.com" "Mozilla/5.0"
//! ```
//!
//! ## Error Log
//!
//! The nginx error log format:
//! ```text
//! YYYY/MM/DD HH:MM:SS [level] PID#TID: *CID message
//! ```
//!
//! Example:
//! ```text
//! 2025/12/17 10:15:32 [error] 1234#5678: *9 open() "/var/www/missing.html" failed (2: No such file or directory)
//! ```
//!
use chrono::{DateTime, NaiveDateTime};
use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};

use super::json::JsonParser;
use super::regex::RegexParser;
use super::traits::{ParsedLog, Parser};
use crate::receivers::file::error::Result;

/// Nginx time_local format string for chrono parsing.
pub const NGINX_TIME_LOCAL_FORMAT: &str = "%d/%b/%Y:%H:%M:%S %z";

/// Regex pattern for nginx combined access log format.
///
/// Captures:
/// - `remote_addr`: Client IP address
/// - `remote_user`: Authenticated user (or `-`)
/// - `time_local`: Timestamp in nginx format
/// - `request`: Full request line (method + path + protocol)
/// - `status`: HTTP status code
/// - `body_bytes_sent`: Response body size
/// - `http_referer`: Referer header
/// - `http_user_agent`: User agent string
pub const NGINX_COMBINED_PATTERN: &str = r#"^(?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<time_local>[^\]]+)\] "(?P<request>[^"]*)" (?P<status>\d+) (?P<body_bytes_sent>\d+|-) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)""#;

/// Regex pattern for nginx error log format.
///
/// Captures:
/// - `time`: Timestamp (YYYY/MM/DD HH:MM:SS)
/// - `level`: Log level (error, warn, notice, info, etc.)
/// - `pid`: Process ID
/// - `tid`: Thread ID
/// - `cid`: Connection ID (optional)
/// - `message`: Error message
pub const NGINX_ERROR_PATTERN: &str = r#"^(?P<time>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) \[(?P<level>\w+)\] (?P<pid>\d+)#(?P<tid>\d+):(?: \*(?P<cid>\d+))? (?P<message>.+)$"#;

/// Create a parser for nginx combined access log format.
pub fn access_parser() -> Result<NginxAccessParser> {
    NginxAccessParser::new()
}

/// Create a parser for nginx error log format.
pub fn error_parser() -> Result<NginxErrorParser> {
    NginxErrorParser::new()
}

/// Create a parser for nginx JSON access logs.
///
/// This is for nginx configured with JSON log format, e.g.:
/// ```nginx
/// log_format json_combined escape=json '{'
///   '"time_local":"$time_local",'
///   '"remote_addr":"$remote_addr",'
///   '"request":"$request",'
///   '"status":$status,'
///   '"body_bytes_sent":$body_bytes_sent'
/// '}';
/// ```
pub fn json_access_parser() -> NginxJsonAccessParser {
    NginxJsonAccessParser::new()
}

/// Create an auto-detecting parser for nginx access logs.
/// Detects JSON vs combined format per-line.
pub fn auto_access_parser() -> Result<NginxAutoAccessParser> {
    NginxAutoAccessParser::new()
}

/// Parser for nginx combined access log format.
///
/// This parser extracts fields from the standard nginx combined log format.
/// It matches the OTel Collector's filelog receiver behavior.
pub struct NginxAccessParser {
    regex: RegexParser,
}

impl NginxAccessParser {
    /// Create a new nginx access log parser.
    pub fn new() -> Result<Self> {
        let regex = RegexParser::new(NGINX_COMBINED_PATTERN)?
            .with_timestamp("time_local", NGINX_TIME_LOCAL_FORMAT);
        Ok(Self { regex })
    }
}

impl Parser for NginxAccessParser {
    fn parse(&self, line: &str) -> Result<ParsedLog> {
        let mut parsed = self.regex.parse(line)?;
        parsed.add_string("source", "nginx");
        Ok(parsed)
    }
}

/// Parser for nginx error log format.
pub struct NginxErrorParser {
    regex: RegexParser,
}

impl NginxErrorParser {
    /// Create a new nginx error log parser.
    pub fn new() -> Result<Self> {
        let regex = RegexParser::new(NGINX_ERROR_PATTERN)?;
        Ok(Self { regex })
    }
}

impl Parser for NginxErrorParser {
    fn parse(&self, line: &str) -> Result<ParsedLog> {
        let mut parsed = self.regex.parse(line)?;
        parsed.add_string("source", "nginx");
        Ok(parsed)
    }
}

/// Common alternative timestamp formats for nginx time_local field.
/// These are tried in order if the standard format fails.
const ALTERNATIVE_TIME_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S",    // ISO-ish without timezone: 2019-02-13 20:58:22
    "%Y-%m-%dT%H:%M:%S",    // ISO 8601 without timezone: 2019-02-13T20:58:22
    "%Y-%m-%dT%H:%M:%S%z",  // ISO 8601 with timezone: 2019-02-13T20:58:22+0000
    "%Y-%m-%d %H:%M:%S %z", // With timezone: 2019-02-13 20:58:22 +0000
    "%d/%b/%Y %H:%M:%S",    // Without colon: 13/Feb/2019 20:58:22
];

/// Parse nginx time_local field from attributes and set timestamp on ParsedLog.
fn parse_time_local_timestamp(parsed: &mut ParsedLog) {
    let time_local = parsed.attributes.iter().find_map(|kv| {
        if kv.key == "time_local" {
            if let Some(AnyValue {
                value: Some(any_value::Value::StringValue(s)),
            }) = &kv.value
            {
                return Some(s.as_str());
            }
        }
        None
    });

    if let Some(ts) = time_local {
        // Try the standard nginx format first (has timezone)
        if let Some(nanos) = DateTime::parse_from_str(ts, NGINX_TIME_LOCAL_FORMAT)
            .ok()
            .and_then(|dt| dt.timestamp_nanos_opt())
            .map(|n| n as u64)
        {
            parsed.timestamp = Some(nanos);
            return;
        }

        // Try alternative formats with timezone
        for format in ALTERNATIVE_TIME_FORMATS {
            // Try with timezone first
            if let Some(nanos) = DateTime::parse_from_str(ts, format)
                .ok()
                .and_then(|dt| dt.timestamp_nanos_opt())
                .map(|n| n as u64)
            {
                parsed.timestamp = Some(nanos);
                return;
            }

            // Try as naive datetime (no timezone) and assume UTC
            if let Some(nanos) = NaiveDateTime::parse_from_str(ts, format)
                .ok()
                .map(|dt| dt.and_utc())
                .and_then(|dt| dt.timestamp_nanos_opt())
                .map(|n| n as u64)
            {
                parsed.timestamp = Some(nanos);
                return;
            }
        }
    }
}

/// Parser for nginx JSON access log format.
pub struct NginxJsonAccessParser {
    json: JsonParser,
}

impl NginxJsonAccessParser {
    pub fn new() -> Self {
        Self {
            json: JsonParser::new(),
        }
    }
}

impl Parser for NginxJsonAccessParser {
    fn parse(&self, line: &str) -> Result<ParsedLog> {
        let mut parsed = self.json.parse(line)?;
        parse_time_local_timestamp(&mut parsed);
        parsed.add_string("source", "nginx");
        Ok(parsed)
    }
}

/// Auto-detecting parser for nginx access logs.
/// Detects JSON vs combined format based on whether the line starts with '{'.
pub struct NginxAutoAccessParser {
    combined: RegexParser,
    json: JsonParser,
}

impl NginxAutoAccessParser {
    pub fn new() -> Result<Self> {
        let combined = RegexParser::new(NGINX_COMBINED_PATTERN)?
            .with_timestamp("time_local", NGINX_TIME_LOCAL_FORMAT);
        let json = JsonParser::new();
        Ok(Self { combined, json })
    }
}

impl Parser for NginxAutoAccessParser {
    fn parse(&self, line: &str) -> Result<ParsedLog> {
        let mut parsed = if line.starts_with('{') {
            let mut p = self.json.parse(line)?;
            parse_time_local_timestamp(&mut p);
            p
        } else {
            self.combined.parse(line)?
        };
        parsed.add_string("source", "nginx");
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::any_value;

    // Real nginx access log samples
    const ACCESS_LOG_SAMPLES: &[&str] = &[
        r#"192.168.1.1 - - [17/Dec/2025:10:15:32 +0000] "GET /api/users HTTP/1.1" 200 1234 "-" "curl/7.68.0""#,
        r#"10.0.0.50 - alice [17/Dec/2025:10:15:33 +0000] "POST /api/login HTTP/1.1" 302 0 "https://example.com/login" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36""#,
        r#"172.16.0.1 - - [17/Dec/2025:10:15:34 +0000] "GET /static/style.css HTTP/2.0" 304 0 "https://example.com/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)""#,
        r#"192.168.1.100 - - [17/Dec/2025:10:15:35 +0000] "DELETE /api/users/123 HTTP/1.1" 204 0 "-" "PostmanRuntime/7.29.0""#,
        r#"10.10.10.10 - admin [17/Dec/2025:10:15:36 +0000] "PUT /api/config HTTP/1.1" 200 89 "-" "python-requests/2.28.1""#,
    ];

    // Real nginx error log samples
    const ERROR_LOG_SAMPLES: &[&str] = &[
        r#"2025/12/17 10:15:32 [error] 1234#5678: *9 open() "/var/www/html/missing.html" failed (2: No such file or directory), client: 192.168.1.1, server: example.com, request: "GET /missing.html HTTP/1.1""#,
        r#"2025/12/17 10:15:33 [warn] 1234#5678: *10 an upstream response is buffered to a temporary file /var/cache/nginx/proxy_temp/1/00/0000000001"#,
        r#"2025/12/17 10:15:34 [notice] 1234#0: signal process started"#,
        r#"2025/12/17 10:15:35 [error] 1234#5678: *11 connect() failed (111: Connection refused) while connecting to upstream, client: 10.0.0.1, server: api.example.com"#,
        r#"2025/12/17 10:15:36 [crit] 1234#5678: *12 SSL_do_handshake() failed (SSL: error:14094412:SSL routines:ssl3_read_bytes:sslv3 alert bad certificate)"#,
    ];

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
    fn test_access_parser_basic() {
        let parser = access_parser().unwrap();

        let result = parser.parse(ACCESS_LOG_SAMPLES[0]).unwrap();

        assert_eq!(
            get_string_value(&result, "remote_addr"),
            Some("192.168.1.1")
        );
        assert_eq!(get_string_value(&result, "remote_user"), Some("-"));
        assert_eq!(
            get_string_value(&result, "time_local"),
            Some("17/Dec/2025:10:15:32 +0000")
        );
        assert_eq!(
            get_string_value(&result, "request"),
            Some("GET /api/users HTTP/1.1")
        );
        assert_eq!(get_string_value(&result, "status"), Some("200"));
        assert_eq!(get_string_value(&result, "body_bytes_sent"), Some("1234"));
        assert_eq!(get_string_value(&result, "http_referer"), Some("-"));
        assert_eq!(
            get_string_value(&result, "http_user_agent"),
            Some("curl/7.68.0")
        );
    }

    #[test]
    fn test_access_parser_with_user() {
        let parser = access_parser().unwrap();

        let result = parser.parse(ACCESS_LOG_SAMPLES[1]).unwrap();

        assert_eq!(get_string_value(&result, "remote_addr"), Some("10.0.0.50"));
        assert_eq!(get_string_value(&result, "remote_user"), Some("alice"));
        assert_eq!(
            get_string_value(&result, "request"),
            Some("POST /api/login HTTP/1.1")
        );
        assert_eq!(get_string_value(&result, "status"), Some("302"));
        assert_eq!(
            get_string_value(&result, "http_referer"),
            Some("https://example.com/login")
        );
    }

    #[test]
    fn test_access_parser_http2() {
        let parser = access_parser().unwrap();

        let result = parser.parse(ACCESS_LOG_SAMPLES[2]).unwrap();

        assert_eq!(
            get_string_value(&result, "request"),
            Some("GET /static/style.css HTTP/2.0")
        );
        assert_eq!(get_string_value(&result, "status"), Some("304"));
    }

    #[test]
    fn test_access_parser_all_methods() {
        let parser = access_parser().unwrap();

        // DELETE
        let result = parser.parse(ACCESS_LOG_SAMPLES[3]).unwrap();
        assert_eq!(
            get_string_value(&result, "request"),
            Some("DELETE /api/users/123 HTTP/1.1")
        );
        assert_eq!(get_string_value(&result, "status"), Some("204"));

        // PUT
        let result = parser.parse(ACCESS_LOG_SAMPLES[4]).unwrap();
        assert_eq!(
            get_string_value(&result, "request"),
            Some("PUT /api/config HTTP/1.1")
        );
        assert_eq!(get_string_value(&result, "remote_user"), Some("admin"));
    }

    #[test]
    fn test_access_parser_all_samples() {
        let parser = access_parser().unwrap();

        for (i, sample) in ACCESS_LOG_SAMPLES.iter().enumerate() {
            let result = parser.parse(sample);
            assert!(
                result.is_ok(),
                "Failed to parse access log sample {}: {:?}",
                i,
                result.err()
            );

            let parsed = result.unwrap();
            assert!(
                get_string_value(&parsed, "remote_addr").is_some(),
                "Missing remote_addr in sample {}",
                i
            );
            assert!(
                get_string_value(&parsed, "status").is_some(),
                "Missing status in sample {}",
                i
            );
        }
    }

    #[test]
    fn test_error_parser_basic() {
        let parser = error_parser().unwrap();

        let result = parser.parse(ERROR_LOG_SAMPLES[0]).unwrap();

        assert_eq!(
            get_string_value(&result, "time"),
            Some("2025/12/17 10:15:32")
        );
        assert_eq!(get_string_value(&result, "level"), Some("error"));
        assert_eq!(get_string_value(&result, "pid"), Some("1234"));
        assert_eq!(get_string_value(&result, "tid"), Some("5678"));
        assert_eq!(get_string_value(&result, "cid"), Some("9"));
        assert!(
            get_string_value(&result, "message")
                .unwrap()
                .contains("No such file or directory")
        );
    }

    #[test]
    fn test_error_parser_warn() {
        let parser = error_parser().unwrap();

        let result = parser.parse(ERROR_LOG_SAMPLES[1]).unwrap();

        assert_eq!(get_string_value(&result, "level"), Some("warn"));
        assert_eq!(get_string_value(&result, "cid"), Some("10"));
    }

    #[test]
    fn test_error_parser_no_cid() {
        let parser = error_parser().unwrap();

        let result = parser.parse(ERROR_LOG_SAMPLES[2]).unwrap();

        assert_eq!(get_string_value(&result, "level"), Some("notice"));
        assert_eq!(get_string_value(&result, "tid"), Some("0"));
        // cid should not be present (optional group)
        assert!(get_string_value(&result, "cid").is_none());
    }

    #[test]
    fn test_error_parser_all_levels() {
        let parser = error_parser().unwrap();

        let levels = ["error", "warn", "notice", "error", "crit"];

        for (i, sample) in ERROR_LOG_SAMPLES.iter().enumerate() {
            let result = parser.parse(sample).unwrap();
            assert_eq!(
                get_string_value(&result, "level"),
                Some(levels[i]),
                "Wrong level for sample {}",
                i
            );
        }
    }

    #[test]
    fn test_error_parser_all_samples() {
        let parser = error_parser().unwrap();

        for (i, sample) in ERROR_LOG_SAMPLES.iter().enumerate() {
            let result = parser.parse(sample);
            assert!(
                result.is_ok(),
                "Failed to parse error log sample {}: {:?}",
                i,
                result.err()
            );

            let parsed = result.unwrap();
            assert!(
                get_string_value(&parsed, "level").is_some(),
                "Missing level in sample {}",
                i
            );
            assert!(
                get_string_value(&parsed, "message").is_some(),
                "Missing message in sample {}",
                i
            );
        }
    }

    #[test]
    fn test_json_access_parser() {
        let parser = json_access_parser();

        let json_log = r#"{"time_local":"17/Dec/2025:10:15:32 +0000","remote_addr":"192.168.1.1","request":"GET /api HTTP/1.1","status":200,"body_bytes_sent":1234}"#;

        let result = parser.parse(json_log).unwrap();

        assert_eq!(
            get_string_value(&result, "remote_addr"),
            Some("192.168.1.1")
        );
        // status and body_bytes_sent are numbers, not strings
        assert!(result.attributes.iter().any(|kv| kv.key == "status"));
        assert!(
            result
                .attributes
                .iter()
                .any(|kv| kv.key == "body_bytes_sent")
        );

        assert_eq!(
            get_string_value(&result, "source"),
            Some("nginx"),
            "NginxJsonAccessParser should add source attribute"
        );

        assert!(
            result.timestamp.is_some(),
            "NginxJsonAccessParser should parse time_local as timestamp"
        );
        let expected_nanos = 1765966532_000_000_000u64;
        assert_eq!(result.timestamp.unwrap(), expected_nanos);
    }

    #[test]
    fn test_json_access_parser_without_time_local() {
        let parser = json_access_parser();

        let json_log =
            r#"{"remote_addr":"192.168.1.1","request":"GET /api HTTP/1.1","status":200}"#;

        let result = parser.parse(json_log).unwrap();

        assert_eq!(
            get_string_value(&result, "remote_addr"),
            Some("192.168.1.1")
        );
        assert_eq!(
            get_string_value(&result, "source"),
            Some("nginx"),
            "Source attribute should still be added"
        );
        assert!(
            result.timestamp.is_none(),
            "Timestamp should be None when time_local is not present"
        );
    }

    // Timestamp parsing tests are in regex.rs where the parsing logic lives.
    // These tests verify NginxAccessParser correctly configures timestamp extraction.

    #[test]
    fn test_access_parser_sets_timestamp() {
        let parser = access_parser().unwrap();

        let result = parser.parse(ACCESS_LOG_SAMPLES[0]).unwrap();

        // Verify timestamp is set
        assert!(
            result.timestamp.is_some(),
            "NginxAccessParser should set timestamp from time_local"
        );

        // Verify it's the expected value (17/Dec/2025:10:15:32 +0000)
        let expected_nanos = 1765966532_000_000_000u64;
        assert_eq!(result.timestamp.unwrap(), expected_nanos);
    }

    #[test]
    fn test_access_parser_all_samples_have_timestamp() {
        let parser = access_parser().unwrap();

        for (i, sample) in ACCESS_LOG_SAMPLES.iter().enumerate() {
            let result = parser.parse(sample).unwrap();
            assert!(
                result.timestamp.is_some(),
                "Sample {} should have timestamp set",
                i
            );
        }
    }

    #[test]
    fn test_error_parser_no_timestamp() {
        let parser = error_parser().unwrap();
        let result = parser.parse(ERROR_LOG_SAMPLES[0]).unwrap();
        assert!(result.timestamp.is_none());
    }

    const JSON_ACCESS_LOG_SAMPLES: &[&str] = &[
        r#"{"time_local":"17/Dec/2025:10:15:32 +0000","remote_addr":"192.168.1.1","request":"GET /api/users HTTP/1.1","status":200,"body_bytes_sent":1234}"#,
        r#"{"time_local":"17/Dec/2025:10:15:33 +0000","remote_addr":"10.0.0.50","remote_user":"alice","request":"POST /api/login HTTP/1.1","status":302}"#,
    ];

    #[test]
    fn test_auto_parser_combined_format() {
        let parser = auto_access_parser().unwrap();
        let result = parser.parse(ACCESS_LOG_SAMPLES[0]).unwrap();

        assert_eq!(
            get_string_value(&result, "remote_addr"),
            Some("192.168.1.1")
        );
        assert_eq!(get_string_value(&result, "status"), Some("200"));
        assert_eq!(get_string_value(&result, "source"), Some("nginx"));
        assert!(result.timestamp.is_some());
    }

    #[test]
    fn test_auto_parser_json_format() {
        let parser = auto_access_parser().unwrap();
        let result = parser.parse(JSON_ACCESS_LOG_SAMPLES[0]).unwrap();

        assert_eq!(
            get_string_value(&result, "remote_addr"),
            Some("192.168.1.1")
        );
        assert_eq!(
            get_string_value(&result, "request"),
            Some("GET /api/users HTTP/1.1")
        );
        assert_eq!(get_string_value(&result, "source"), Some("nginx"));
        assert!(result.timestamp.is_some());
        assert_eq!(result.timestamp.unwrap(), 1765966532_000_000_000u64);
    }

    #[test]
    fn test_auto_parser_mixed_formats() {
        let parser = auto_access_parser().unwrap();

        let combined = parser.parse(ACCESS_LOG_SAMPLES[0]).unwrap();
        assert_eq!(
            get_string_value(&combined, "remote_addr"),
            Some("192.168.1.1")
        );

        let json = parser.parse(JSON_ACCESS_LOG_SAMPLES[0]).unwrap();
        assert_eq!(get_string_value(&json, "remote_addr"), Some("192.168.1.1"));

        let combined2 = parser.parse(ACCESS_LOG_SAMPLES[1]).unwrap();
        assert_eq!(
            get_string_value(&combined2, "remote_addr"),
            Some("10.0.0.50")
        );
    }

    #[test]
    fn test_auto_parser_json_without_time_local() {
        let parser = auto_access_parser().unwrap();
        let json_no_time = r#"{"remote_addr":"192.168.1.1","status":200}"#;
        let result = parser.parse(json_no_time).unwrap();

        assert_eq!(
            get_string_value(&result, "remote_addr"),
            Some("192.168.1.1")
        );
        assert!(result.timestamp.is_none());
    }
}
