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
use crate::receivers::file::entry::Entry;
use crate::receivers::file::error::Result;
use super::json::JsonParser;
use super::regex::RegexParser;
use super::traits::Parser;
use serde_json::Value;

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
pub fn json_access_parser() -> JsonParser {
    JsonParser::new()
}

/// Parser for nginx combined access log format.
///
/// This parser extracts fields from the standard nginx combined log format
/// and also parses the request line into separate method, path, and protocol fields.
pub struct NginxAccessParser {
    regex: RegexParser,
}

impl NginxAccessParser {
    /// Create a new nginx access log parser.
    pub fn new() -> Result<Self> {
        let regex = RegexParser::new(NGINX_COMBINED_PATTERN)?;
        Ok(Self { regex })
    }

    /// Parse the request field into method, path, and protocol.
    fn parse_request(request: &str) -> (Option<&str>, Option<&str>, Option<&str>) {
        let parts: Vec<&str> = request.splitn(3, ' ').collect();
        match parts.len() {
            3 => (Some(parts[0]), Some(parts[1]), Some(parts[2])),
            2 => (Some(parts[0]), Some(parts[1]), None),
            1 if !parts[0].is_empty() => (Some(parts[0]), None, None),
            _ => (None, None, None),
        }
    }
}

impl Parser for NginxAccessParser {
    fn parse(&self, entry: Entry) -> Result<Entry> {
        let mut parsed = self.regex.parse(entry)?;

        // Parse the request field into components (request is now in attributes)
        let request_parts = parsed
            .attributes
            .get("request")
            .and_then(|v| v.as_str())
            .map(|request| {
                let (method, path, protocol) = Self::parse_request(request);
                (
                    method.map(|s| s.to_string()),
                    path.map(|s| s.to_string()),
                    protocol.map(|s| s.to_string()),
                )
            });

        // Add parsed request components to attributes
        if let Some((method, path, protocol)) = request_parts {
            if let Some(m) = method {
                parsed.add_attribute("method", Value::String(m));
            }
            if let Some(p) = path {
                parsed.add_attribute("path", Value::String(p));
            }
            if let Some(proto) = protocol {
                parsed.add_attribute("protocol", Value::String(proto));
            }
        }

        Ok(parsed)
    }

    fn parse_str(&self, value: &str) -> Result<Value> {
        let entry = Entry::with_body(value);
        let parsed = self.parse(entry)?;
        // Return attributes as the parsed value (for backward compat with tests)
        let mut map = serde_json::Map::new();
        for (k, v) in &parsed.attributes {
            map.insert(k.clone(), v.clone());
        }
        Ok(Value::Object(map))
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
    fn parse(&self, entry: Entry) -> Result<Entry> {
        self.regex.parse(entry)
    }

    fn parse_str(&self, value: &str) -> Result<Value> {
        let entry = Entry::with_body(value);
        let parsed = self.parse(entry)?;
        // Return attributes as the parsed value (for backward compat with tests)
        let mut map = serde_json::Map::new();
        for (k, v) in &parsed.attributes {
            map.insert(k.clone(), v.clone());
        }
        Ok(Value::Object(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_access_parser_basic() {
        let parser = access_parser().unwrap();

        let result = parser.parse_str(ACCESS_LOG_SAMPLES[0]).unwrap();

        assert_eq!(result["remote_addr"], "192.168.1.1");
        assert_eq!(result["remote_user"], "-");
        assert_eq!(result["time_local"], "17/Dec/2025:10:15:32 +0000");
        assert_eq!(result["request"], "GET /api/users HTTP/1.1");
        assert_eq!(result["method"], "GET");
        assert_eq!(result["path"], "/api/users");
        assert_eq!(result["protocol"], "HTTP/1.1");
        assert_eq!(result["status"], "200");
        assert_eq!(result["body_bytes_sent"], "1234");
        assert_eq!(result["http_referer"], "-");
        assert_eq!(result["http_user_agent"], "curl/7.68.0");
    }

    #[test]
    fn test_access_parser_with_user() {
        let parser = access_parser().unwrap();

        let result = parser.parse_str(ACCESS_LOG_SAMPLES[1]).unwrap();

        assert_eq!(result["remote_addr"], "10.0.0.50");
        assert_eq!(result["remote_user"], "alice");
        assert_eq!(result["method"], "POST");
        assert_eq!(result["path"], "/api/login");
        assert_eq!(result["status"], "302");
        assert_eq!(result["http_referer"], "https://example.com/login");
    }

    #[test]
    fn test_access_parser_http2() {
        let parser = access_parser().unwrap();

        let result = parser.parse_str(ACCESS_LOG_SAMPLES[2]).unwrap();

        assert_eq!(result["protocol"], "HTTP/2.0");
        assert_eq!(result["status"], "304");
    }

    #[test]
    fn test_access_parser_all_methods() {
        let parser = access_parser().unwrap();

        // DELETE
        let result = parser.parse_str(ACCESS_LOG_SAMPLES[3]).unwrap();
        assert_eq!(result["method"], "DELETE");
        assert_eq!(result["status"], "204");

        // PUT
        let result = parser.parse_str(ACCESS_LOG_SAMPLES[4]).unwrap();
        assert_eq!(result["method"], "PUT");
        assert_eq!(result["remote_user"], "admin");
    }

    #[test]
    fn test_access_parser_all_samples() {
        let parser = access_parser().unwrap();

        for (i, sample) in ACCESS_LOG_SAMPLES.iter().enumerate() {
            let result = parser.parse_str(sample);
            assert!(
                result.is_ok(),
                "Failed to parse access log sample {}: {:?}",
                i,
                result.err()
            );

            let parsed = result.unwrap();
            assert!(
                parsed.get("remote_addr").is_some(),
                "Missing remote_addr in sample {}",
                i
            );
            assert!(
                parsed.get("status").is_some(),
                "Missing status in sample {}",
                i
            );
        }
    }

    #[test]
    fn test_error_parser_basic() {
        let parser = error_parser().unwrap();

        let result = parser.parse_str(ERROR_LOG_SAMPLES[0]).unwrap();

        assert_eq!(result["time"], "2025/12/17 10:15:32");
        assert_eq!(result["level"], "error");
        assert_eq!(result["pid"], "1234");
        assert_eq!(result["tid"], "5678");
        assert_eq!(result["cid"], "9");
        assert!(result["message"]
            .as_str()
            .unwrap()
            .contains("No such file or directory"));
    }

    #[test]
    fn test_error_parser_warn() {
        let parser = error_parser().unwrap();

        let result = parser.parse_str(ERROR_LOG_SAMPLES[1]).unwrap();

        assert_eq!(result["level"], "warn");
        assert_eq!(result["cid"], "10");
    }

    #[test]
    fn test_error_parser_no_cid() {
        let parser = error_parser().unwrap();

        let result = parser.parse_str(ERROR_LOG_SAMPLES[2]).unwrap();

        assert_eq!(result["level"], "notice");
        assert_eq!(result["tid"], "0");
        // cid should not be present (optional group)
        assert!(result.get("cid").is_none() || result["cid"].is_null());
    }

    #[test]
    fn test_error_parser_all_levels() {
        let parser = error_parser().unwrap();

        let levels = ["error", "warn", "notice", "error", "crit"];

        for (i, sample) in ERROR_LOG_SAMPLES.iter().enumerate() {
            let result = parser.parse_str(sample).unwrap();
            assert_eq!(result["level"], levels[i], "Wrong level for sample {}", i);
        }
    }

    #[test]
    fn test_error_parser_all_samples() {
        let parser = error_parser().unwrap();

        for (i, sample) in ERROR_LOG_SAMPLES.iter().enumerate() {
            let result = parser.parse_str(sample);
            assert!(
                result.is_ok(),
                "Failed to parse error log sample {}: {:?}",
                i,
                result.err()
            );

            let parsed = result.unwrap();
            assert!(
                parsed.get("level").is_some(),
                "Missing level in sample {}",
                i
            );
            assert!(
                parsed.get("message").is_some(),
                "Missing message in sample {}",
                i
            );
        }
    }

    #[test]
    fn test_json_access_parser() {
        let parser = json_access_parser();

        let json_log = r#"{"time_local":"17/Dec/2025:10:15:32 +0000","remote_addr":"192.168.1.1","request":"GET /api HTTP/1.1","status":200,"body_bytes_sent":1234}"#;

        let result = parser.parse_str(json_log).unwrap();

        assert_eq!(result["remote_addr"], "192.168.1.1");
        assert_eq!(result["status"], 200);
        assert_eq!(result["body_bytes_sent"], 1234);
    }

    #[test]
    fn test_access_parser_with_entry() {
        let parser = access_parser().unwrap();

        let mut entry = Entry::with_body(ACCESS_LOG_SAMPLES[0]);
        entry.add_attribute_string("source", "nginx");
        entry.add_attribute_string("file_name", "access.log");

        let parsed = parser.parse(entry).unwrap();

        // Body should be preserved
        assert_eq!(parsed.body_string(), Some(ACCESS_LOG_SAMPLES[0]));

        // Check existing attributes preserved
        assert_eq!(parsed.attributes.get("source"), Some(&Value::String("nginx".to_string())));
        assert_eq!(parsed.attributes.get("file_name"), Some(&Value::String("access.log".to_string())));

        // Check parsed fields in attributes
        assert_eq!(parsed.attributes["remote_addr"], "192.168.1.1");
        assert_eq!(parsed.attributes["method"], "GET");
    }

    #[test]
    fn test_parse_request_edge_cases() {
        // Normal case
        let (m, p, proto) = NginxAccessParser::parse_request("GET /path HTTP/1.1");
        assert_eq!(m, Some("GET"));
        assert_eq!(p, Some("/path"));
        assert_eq!(proto, Some("HTTP/1.1"));

        // No protocol
        let (m, p, proto) = NginxAccessParser::parse_request("GET /path");
        assert_eq!(m, Some("GET"));
        assert_eq!(p, Some("/path"));
        assert_eq!(proto, None);

        // Just method
        let (m, p, proto) = NginxAccessParser::parse_request("OPTIONS");
        assert_eq!(m, Some("OPTIONS"));
        assert_eq!(p, None);
        assert_eq!(proto, None);

        // Empty
        let (m, p, proto) = NginxAccessParser::parse_request("");
        assert_eq!(m, None);
        assert_eq!(p, None);
        assert_eq!(proto, None);

        // Path with spaces (malformed but should handle gracefully)
        let (m, p, proto) = NginxAccessParser::parse_request("GET /path with spaces HTTP/1.1");
        assert_eq!(m, Some("GET"));
        assert_eq!(p, Some("/path"));
        assert_eq!(proto, Some("with spaces HTTP/1.1")); // Takes rest as protocol
    }
}
