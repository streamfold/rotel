// SPDX-License-Identifier: Apache-2.0

//! Convert kmsg records to OTLP log records
//!
//! Timestamp conversion from kernel monotonic time (microseconds since boot) to
//! absolute wall-clock time is performed here at send time, not at parse time.
//! This ensures correct timestamps even if NTP adjusts the system clock after boot.

use crate::receivers::kmsg::parser::{KmsgRecord, get_boot_time_ns};
use gethostname::gethostname;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

// Log record attributes
const KMSG_PRIORITY_KEY: &str = "kmsg.priority";
const KMSG_PRIORITY_NAME_KEY: &str = "kmsg.priority_name";
const KMSG_FACILITY_KEY: &str = "kmsg.facility";
const KMSG_FACILITY_NAME_KEY: &str = "kmsg.facility_name";
const KMSG_SEQUENCE_KEY: &str = "kmsg.sequence";
const KMSG_CONTINUATION_KEY: &str = "kmsg.continuation";

// Resource attributes
const LOG_SOURCE_KEY: &str = "log.source";
const LOG_SOURCE_VALUE: &str = "kmsg";
const HOST_NAME_KEY: &str = "host.name";
const OS_TYPE_KEY: &str = "os.type";
const OS_TYPE_VALUE: &str = "linux";
const SERVICE_NAME_KEY: &str = "service.name";
const SERVICE_NAME_VALUE: &str = "kernel";

/// Convert a batch of KmsgRecords to OTLP ResourceLogs
///
/// Boot time is calculated once per batch to convert kernel timestamps
/// (microseconds since boot) to absolute wall-clock timestamps.
pub fn convert_to_otlp_logs(records: Vec<KmsgRecord>) -> ResourceLogs {
    // Calculate boot time once per batch for timestamp conversion.
    // If this fails, we'll fall back to using observed_time for event timestamps.
    let boot_time_ns = match get_boot_time_ns() {
        Ok(t) => Some(t),
        Err(e) => {
            warn!(
                "Failed to get boot time for timestamp conversion: {}. Using observed time as fallback.",
                e
            );
            None
        }
    };

    let log_records: Vec<LogRecord> = records
        .into_iter()
        .map(|r| convert_ksmg_record_to_otlp_log_record(r, boot_time_ns))
        .collect();

    let scope_logs = ScopeLogs {
        scope: Some(InstrumentationScope {
            name: "kmsg".to_string(),
            version: String::new(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }),
        log_records,
        schema_url: String::new(),
    };

    // Build resource attributes
    let mut resource_attributes = vec![
        KeyValue {
            key: LOG_SOURCE_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(LOG_SOURCE_VALUE.to_string())),
            }),
        },
        KeyValue {
            key: OS_TYPE_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(OS_TYPE_VALUE.to_string())),
            }),
        },
        KeyValue {
            key: SERVICE_NAME_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    SERVICE_NAME_VALUE.to_string(),
                )),
            }),
        },
    ];

    // Add hostname if available
    if let Ok(hostname) = gethostname().into_string() {
        resource_attributes.push(KeyValue {
            key: HOST_NAME_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(hostname)),
            }),
        });
    }

    ResourceLogs {
        resource: Some(Resource {
            attributes: resource_attributes,
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_logs: vec![scope_logs],
        schema_url: String::new(),
    }
}

/// Convert a single KmsgRecord to an OTLP LogRecord
///
/// `boot_time_ns` is used to convert the kernel's monotonic timestamp to absolute time.
/// If `None`, observed_time is used as a fallback for the event timestamp.
fn convert_ksmg_record_to_otlp_log_record(
    record: KmsgRecord,
    boot_time_ns: Option<u64>,
) -> LogRecord {
    // Get current time as observed_time (when sending the log)
    let observed_time_unix_nano = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    // Convert kernel timestamp (µs since boot) to absolute time (ns since epoch)
    // If boot_time is unavailable, fall back to observed_time
    let time_unix_nano = match boot_time_ns {
        Some(boot_time) => boot_time.saturating_add(record.timestamp_us.saturating_mul(1000)),
        None => observed_time_unix_nano,
    };

    let mut attributes = vec![
        KeyValue {
            key: KMSG_PRIORITY_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(
                    (record.priority_raw & 0x07) as i64,
                )),
            }),
        },
        KeyValue {
            key: KMSG_PRIORITY_NAME_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    record.priority.as_str().to_string(),
                )),
            }),
        },
        KeyValue {
            key: KMSG_FACILITY_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(
                    (record.priority_raw >> 3) as i64,
                )),
            }),
        },
        KeyValue {
            key: KMSG_FACILITY_NAME_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    record.facility.as_str().to_string(),
                )),
            }),
        },
        KeyValue {
            key: KMSG_SEQUENCE_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(record.sequence as i64)),
            }),
        },
    ];

    // Add continuation flag if this is a continuation message
    if record.is_continuation {
        attributes.push(KeyValue {
            key: KMSG_CONTINUATION_KEY.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::BoolValue(true)),
            }),
        });
    }

    LogRecord {
        time_unix_nano,
        observed_time_unix_nano,
        severity_number: record.priority.to_otel_severity_number(),
        severity_text: record.priority.as_str().to_string(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(record.message)),
        }),
        attributes,
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![],
        span_id: vec![],
        event_name: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::kmsg::parser::{Facility, Priority};

    // Helper to create a test record with raw kernel timestamp (µs since boot)
    fn make_test_record(
        priority_raw: u8,
        priority: Priority,
        facility: Facility,
        sequence: u64,
        timestamp_us: u64,
        message: &str,
        is_continuation: bool,
    ) -> KmsgRecord {
        KmsgRecord {
            priority_raw,
            priority,
            facility,
            sequence,
            timestamp_us,
            message: message.to_string(),
            is_continuation,
        }
    }

    #[test]
    fn test_convert_single_record_with_boot_time() {
        let boot_time_ns: u64 = 1000000000000; // 1000 seconds since epoch in ns
        let timestamp_us: u64 = 5000000; // 5 seconds since boot in µs

        let record = make_test_record(
            6,
            Priority::Info,
            Facility::Kern,
            1234,
            timestamp_us,
            "Test message",
            false,
        );

        let log_record = convert_record_to_log_record(record, Some(boot_time_ns));

        // Expected: boot_time + (timestamp_us * 1000)
        let expected_time_ns = boot_time_ns + (timestamp_us * 1000);
        assert_eq!(log_record.time_unix_nano, expected_time_ns);
        assert!(
            log_record.observed_time_unix_nano > 0,
            "observed_time should be set to current time"
        );
        assert_eq!(log_record.severity_number, 9); // INFO
        assert_eq!(log_record.severity_text, "INFO");

        // Check body
        if let Some(AnyValue {
            value: Some(any_value::Value::StringValue(body)),
        }) = &log_record.body
        {
            assert_eq!(body, "Test message");
        } else {
            panic!("Expected string body");
        }
    }

    #[test]
    fn test_convert_record_without_boot_time_uses_observed_time() {
        let record = make_test_record(
            6,
            Priority::Info,
            Facility::Kern,
            1234,
            5000000, // This should be ignored when boot_time is None
            "Test message",
            false,
        );

        let log_record = convert_record_to_log_record(record, None);

        // When boot_time is None, time_unix_nano should equal observed_time_unix_nano
        assert_eq!(
            log_record.time_unix_nano,
            log_record.observed_time_unix_nano
        );
    }

    #[test]
    fn test_convert_continuation_message() {
        let record = make_test_record(
            4,
            Priority::Warning,
            Facility::Kern,
            5678,
            2000000,
            "Continuation",
            true,
        );

        let log_record = convert_record_to_log_record(record, Some(1000000000000));

        // Check that continuation attribute is present
        let continuation_attr = log_record
            .attributes
            .iter()
            .find(|kv| kv.key == KMSG_CONTINUATION_KEY);

        assert!(continuation_attr.is_some());
        if let Some(KeyValue {
            value:
                Some(AnyValue {
                    value: Some(any_value::Value::BoolValue(true)),
                }),
            ..
        }) = continuation_attr
        {
            // OK
        } else {
            panic!("Expected boolean true for continuation");
        }
    }

    #[test]
    fn test_convert_multiple_records_same_boot_time() {
        // When converting a batch, all records use the same boot_time
        let boot_time_ns: u64 = 1000000000000;
        let records = vec![
            make_test_record(
                3,
                Priority::Error,
                Facility::Kern,
                100,
                1000000,
                "Error 1",
                false,
            ),
            make_test_record(
                6,
                Priority::Info,
                Facility::Kern,
                101,
                2000000,
                "Info 1",
                false,
            ),
        ];

        let resource_logs = convert_to_otlp_logs(records);

        assert_eq!(resource_logs.scope_logs[0].log_records.len(), 2);
        assert_eq!(
            resource_logs.scope_logs[0].log_records[0].severity_number,
            17
        ); // ERROR
        assert_eq!(
            resource_logs.scope_logs[0].log_records[1].severity_number,
            9
        ); // INFO

        // Timestamps should be different (different timestamp_us values)
        let time1 = resource_logs.scope_logs[0].log_records[0].time_unix_nano;
        let time2 = resource_logs.scope_logs[0].log_records[1].time_unix_nano;
        assert!(time2 > time1, "Second record should have later timestamp");
    }

    #[test]
    fn test_convert_record_with_facility() {
        // Priority 14 = facility 1 (user) + level 6 (info)
        let record = make_test_record(
            14,
            Priority::Info,
            Facility::User,
            1234,
            1000000,
            "User message",
            false,
        );

        let log_record = convert_record_to_log_record(record, Some(1000000000000));

        // Check facility attribute
        let facility_attr = log_record
            .attributes
            .iter()
            .find(|kv| kv.key == KMSG_FACILITY_KEY);

        assert!(facility_attr.is_some());
        if let Some(KeyValue {
            value:
                Some(AnyValue {
                    value: Some(any_value::Value::IntValue(val)),
                }),
            ..
        }) = facility_attr
        {
            assert_eq!(*val, 1); // User facility
        } else {
            panic!("Expected integer facility");
        }

        // Check facility name attribute
        let facility_name_attr = log_record
            .attributes
            .iter()
            .find(|kv| kv.key == KMSG_FACILITY_NAME_KEY);

        assert!(facility_name_attr.is_some());
        if let Some(KeyValue {
            value:
                Some(AnyValue {
                    value: Some(any_value::Value::StringValue(val)),
                }),
            ..
        }) = facility_name_attr
        {
            assert_eq!(val, "user");
        } else {
            panic!("Expected string facility name");
        }
    }
}
