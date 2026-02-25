// SPDX-License-Identifier: Apache-2.0

//! Parser for Linux kernel message format
//!
//! The kmsg device produces messages in the format:
//! `priority,sequence,timestamp[,flags];message\n`
//!
//! Where:
//! - priority: syslog priority (0-7 for level, higher bits for facility)
//! - sequence: kernel message sequence number
//! - timestamp: microseconds since boot
//! - flags: optional, comma-separated flags (may include 'c' for continuation)
//! - message: the actual log message
//!
//! Example: `6,1234,567890123456;eth0: link up`
//! Example with flags: `6,1234,567890123456,c;continuation message`

use crate::receivers::kmsg::error::{KmsgReceiverError, Result};

/// Kernel log priority levels (syslog severity)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    Emergency = 0, // System is unusable
    Alert = 1,     // Action must be taken immediately
    Critical = 2,  // Critical conditions
    Error = 3,     // Error conditions
    Warning = 4,   // Warning conditions
    Notice = 5,    // Normal but significant condition
    Info = 6,      // Informational
    Debug = 7,     // Debug-level messages
}

/// Syslog facility codes
/// See: https://datatracker.ietf.org/doc/html/rfc5424#section-6.2.1
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Facility {
    Kern = 0,      // Kernel messages
    User = 1,      // User-level messages
    Mail = 2,      // Mail system
    Daemon = 3,    // System daemons
    Auth = 4,      // Security/authorization messages
    Syslog = 5,    // Syslogd internal messages
    Lpr = 6,       // Line printer subsystem
    News = 7,      // Network news subsystem
    Uucp = 8,      // UUCP subsystem
    Cron = 9,      // Clock daemon
    Authpriv = 10, // Security/authorization (private)
    Ftp = 11,      // FTP daemon
    Ntp = 12,      // NTP subsystem
    Audit = 13,    // Log audit
    Alert = 14,    // Log alert
    Clock = 15,    // Clock daemon (note 2)
    Local0 = 16,   // Local use 0
    Local1 = 17,   // Local use 1
    Local2 = 18,   // Local use 2
    Local3 = 19,   // Local use 3
    Local4 = 20,   // Local use 4
    Local5 = 21,   // Local use 5
    Local6 = 22,   // Local use 6
    Local7 = 23,   // Local use 7
}

impl Facility {
    /// Extract facility from raw priority byte (bits 3-7)
    pub fn from_u8(value: u8) -> Self {
        match value >> 3 {
            0 => Facility::Kern,
            1 => Facility::User,
            2 => Facility::Mail,
            3 => Facility::Daemon,
            4 => Facility::Auth,
            5 => Facility::Syslog,
            6 => Facility::Lpr,
            7 => Facility::News,
            8 => Facility::Uucp,
            9 => Facility::Cron,
            10 => Facility::Authpriv,
            11 => Facility::Ftp,
            12 => Facility::Ntp,
            13 => Facility::Audit,
            14 => Facility::Alert,
            15 => Facility::Clock,
            16 => Facility::Local0,
            17 => Facility::Local1,
            18 => Facility::Local2,
            19 => Facility::Local3,
            20 => Facility::Local4,
            21 => Facility::Local5,
            22 => Facility::Local6,
            _ => Facility::Local7,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Facility::Kern => "kern",
            Facility::User => "user",
            Facility::Mail => "mail",
            Facility::Daemon => "daemon",
            Facility::Auth => "auth",
            Facility::Syslog => "syslog",
            Facility::Lpr => "lpr",
            Facility::News => "news",
            Facility::Uucp => "uucp",
            Facility::Cron => "cron",
            Facility::Authpriv => "authpriv",
            Facility::Ftp => "ftp",
            Facility::Ntp => "ntp",
            Facility::Audit => "audit",
            Facility::Alert => "alert",
            Facility::Clock => "clock",
            Facility::Local0 => "local0",
            Facility::Local1 => "local1",
            Facility::Local2 => "local2",
            Facility::Local3 => "local3",
            Facility::Local4 => "local4",
            Facility::Local5 => "local5",
            Facility::Local6 => "local6",
            Facility::Local7 => "local7",
        }
    }
}

impl Priority {
    pub fn from_u8(value: u8) -> Self {
        // Mask to get just the priority level (0-7)
        match value & 0x07 {
            0 => Priority::Emergency,
            1 => Priority::Alert,
            2 => Priority::Critical,
            3 => Priority::Error,
            4 => Priority::Warning,
            5 => Priority::Notice,
            6 => Priority::Info,
            // 7 is the only remaining possibility after & 0x07
            _ => Priority::Debug,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Priority::Emergency => "EMERGENCY",
            Priority::Alert => "ALERT",
            Priority::Critical => "CRITICAL",
            Priority::Error => "ERROR",
            Priority::Warning => "WARNING",
            Priority::Notice => "NOTICE",
            Priority::Info => "INFO",
            Priority::Debug => "DEBUG",
        }
    }

    /// Convert to OpenTelemetry severity number
    /// See: https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber
    pub fn to_otel_severity_number(&self) -> i32 {
        match self {
            Priority::Emergency => 21, // FATAL
            Priority::Alert => 21,     // FATAL
            Priority::Critical => 21,  // FATAL
            Priority::Error => 17,     // ERROR
            Priority::Warning => 13,   // WARN
            Priority::Notice => 10,    // INFO2
            Priority::Info => 9,       // INFO
            Priority::Debug => 5,      // DEBUG
        }
    }
}

/// A parsed kernel message record
#[derive(Debug, Clone)]
pub struct KmsgRecord {
    /// The raw priority value from kmsg (contains both facility and level)
    pub priority_raw: u8,
    /// The parsed priority level (0-7)
    pub priority: Priority,
    /// The parsed facility (extracted from priority byte bits 3-7)
    pub facility: Facility,
    /// Kernel message sequence number
    pub sequence: u64,
    /// Raw kernel timestamp in microseconds since boot (monotonic, unaffected by NTP)
    pub timestamp_us: u64,
    /// The log message content
    pub message: String,
    /// Whether this is a continuation of a previous message
    pub is_continuation: bool,
}

/// Get the system boot time as nanoseconds since Unix epoch
pub fn get_boot_time_ns() -> Result<u64> {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Read uptime from /proc/uptime (first value is seconds since boot)
    let uptime_str = fs::read_to_string("/proc/uptime").map_err(|e| {
        KmsgReceiverError::BootTimeError(format!("Failed to read /proc/uptime: {}", e))
    })?;

    let uptime_secs: f64 = uptime_str
        .split_whitespace()
        .next()
        .ok_or_else(|| KmsgReceiverError::BootTimeError("Empty /proc/uptime".to_string()))?
        .parse()
        .map_err(|e| KmsgReceiverError::BootTimeError(format!("Failed to parse uptime: {}", e)))?;

    let uptime_ns = (uptime_secs * 1_000_000_000.0) as u64;

    // Get current time
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| KmsgReceiverError::BootTimeError(format!("System time error: {}", e)))?;

    // Boot time = current time - uptime
    // Use saturating_sub to handle edge cases where uptime might be slightly off
    let boot_time_ns = (now.as_nanos() as u64).saturating_sub(uptime_ns);

    Ok(boot_time_ns)
}

/// Parse a kmsg line into a KmsgRecord
///
/// Format: `priority,sequence,timestamp[,flags];message`
///
/// The timestamp is stored as raw microseconds since boot (monotonic).
/// Conversion to absolute wall-clock time is deferred to send time,
/// ensuring correct timestamps even if NTP adjusts the clock after boot.
pub fn parse_kmsg_line(line: &str) -> Result<KmsgRecord> {
    // Split into header and message parts
    let (header, message) = line.split_once(';').ok_or_else(|| {
        KmsgReceiverError::ParseError(format!("Missing ';' separator in kmsg line: {}", line))
    })?;

    // Parse header parts: priority,sequence,timestamp[,flags]
    let parts: Vec<&str> = header.split(',').collect();
    if parts.len() < 3 {
        return Err(KmsgReceiverError::ParseError(format!(
            "Invalid kmsg header format, expected at least 3 parts: {}",
            header
        )));
    }

    // Parse priority (may include facility in higher bits)
    let priority_raw: u8 = parts[0].parse().map_err(|e| {
        KmsgReceiverError::ParseError(format!("Failed to parse priority '{}': {}", parts[0], e))
    })?;

    // Parse sequence number
    let sequence: u64 = parts[1].parse().map_err(|e| {
        KmsgReceiverError::ParseError(format!("Failed to parse sequence '{}': {}", parts[1], e))
    })?;

    // Parse timestamp (microseconds since boot) - stored raw, converted at send time
    let timestamp_us: u64 = parts[2].parse().map_err(|e| {
        KmsgReceiverError::ParseError(format!("Failed to parse timestamp '{}': {}", parts[2], e))
    })?;

    // Check for continuation flag (if present in parts[3+])
    let is_continuation = parts.len() > 3 && parts[3..].contains(&"c");

    // Extract priority level (bits 0-2) and facility (bits 3-7)
    let priority = Priority::from_u8(priority_raw);
    let facility = Facility::from_u8(priority_raw);

    Ok(KmsgRecord {
        priority_raw,
        priority,
        facility,
        sequence,
        timestamp_us,
        message: message.to_string(),
        is_continuation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_message() {
        let line = "6,1234,567890;eth0: link up";

        let record = parse_kmsg_line(line).unwrap();

        assert_eq!(record.priority_raw, 6);
        assert_eq!(record.priority, Priority::Info);
        assert_eq!(record.facility, Facility::Kern); // facility 0 (kernel)
        assert_eq!(record.sequence, 1234);
        assert_eq!(record.timestamp_us, 567890); // Raw microseconds since boot
        assert_eq!(record.message, "eth0: link up");
        assert!(!record.is_continuation);
    }

    #[test]
    fn test_parse_message_with_continuation_flag() {
        let line = "6,1234,567890,c;continuation message";

        let record = parse_kmsg_line(line).unwrap();

        assert!(record.is_continuation);
        assert_eq!(record.message, "continuation message");
    }

    #[test]
    fn test_parse_message_with_multiple_flags() {
        let line = "4,5678,123456,-,c;warning with flags";

        let record = parse_kmsg_line(line).unwrap();

        assert!(record.is_continuation);
        assert_eq!(record.priority, Priority::Warning);
    }

    #[test]
    fn test_parse_emergency_priority() {
        let line = "0,100,1000;EMERGENCY MESSAGE";

        let record = parse_kmsg_line(line).unwrap();

        assert_eq!(record.priority, Priority::Emergency);
        assert_eq!(record.priority.as_str(), "EMERGENCY");
    }

    #[test]
    fn test_parse_invalid_missing_semicolon() {
        let line = "6,1234,567890 missing semicolon";

        let result = parse_kmsg_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_too_few_parts() {
        let line = "6,1234;message";

        let result = parse_kmsg_line(line);
        assert!(result.is_err());
    }

    #[test]
    fn test_priority_to_otel_severity() {
        assert_eq!(Priority::Emergency.to_otel_severity_number(), 21);
        assert_eq!(Priority::Error.to_otel_severity_number(), 17);
        assert_eq!(Priority::Warning.to_otel_severity_number(), 13);
        assert_eq!(Priority::Info.to_otel_severity_number(), 9);
        assert_eq!(Priority::Debug.to_otel_severity_number(), 5);
    }

    #[test]
    fn test_priority_from_u8_with_facility() {
        // Priority 6 with facility 1 (user) = 8 + 6 = 14
        // Should still extract priority 6
        assert_eq!(Priority::from_u8(14), Priority::Info);

        // Priority 3 with facility 0 (kernel) = 0 + 3 = 3
        assert_eq!(Priority::from_u8(3), Priority::Error);
    }

    #[test]
    fn test_facility_extraction() {
        // Facility 0 (kernel) with priority 6 = 0*8 + 6 = 6
        assert_eq!(Facility::from_u8(6), Facility::Kern);
        assert_eq!(Facility::from_u8(6).as_str(), "kern");

        // Facility 1 (user) with priority 6 = 1*8 + 6 = 14
        assert_eq!(Facility::from_u8(14), Facility::User);
        assert_eq!(Facility::from_u8(14).as_str(), "user");

        // Facility 3 (daemon) with priority 4 = 3*8 + 4 = 28
        assert_eq!(Facility::from_u8(28), Facility::Daemon);
        assert_eq!(Facility::from_u8(28).as_str(), "daemon");

        // Facility 4 (auth) with priority 3 = 4*8 + 3 = 35
        assert_eq!(Facility::from_u8(35), Facility::Auth);

        // Facility 16 (local0) with priority 6 = 16*8 + 6 = 134
        assert_eq!(Facility::from_u8(134), Facility::Local0);
    }

    #[test]
    fn test_parse_message_with_facility() {
        // Priority 14 = facility 1 (user) + level 6 (info)
        let line = "14,1234,567890;user space message";

        let record = parse_kmsg_line(line).unwrap();

        assert_eq!(record.priority_raw, 14);
        assert_eq!(record.priority, Priority::Info);
        assert_eq!(record.facility, Facility::User);
    }
}
