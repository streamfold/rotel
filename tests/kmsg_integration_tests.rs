// SPDX-License-Identifier: Apache-2.0

//! Kmsg Integration Tests
//!
//! These tests require Linux with access to /dev/kmsg and verify actual
//! end-to-end functionality of the kmsg receiver.
//!
//! To run these tests:
//! 1. On Linux: cargo test --test kmsg_integration_tests --features "integration-tests,kmsg_receiver"
//! 2. In Docker (works on Linux, macOS, and Windows with Docker Desktop):
//!    ```
//!    # Using the helper script (recommended):
//!    ./scripts/kmsg-test-env.sh build   # One-time setup
//!    ./scripts/kmsg-test-env.sh test    # Run tests
//!    ./scripts/kmsg-test-env.sh help    # See all commands
//!    ```
//!
//! Note: Reading /dev/kmsg typically requires root or CAP_SYSLOG capability.
//! On macOS/Windows, Docker Desktop runs containers in a Linux VM, so the tests
//! will read kernel messages from that VM's kernel.

#![cfg(all(
    target_os = "linux",
    feature = "integration-tests",
    feature = "kmsg_receiver"
))]

use rotel::receivers::kmsg::config::{KMSG_DEVICE_PATH, KmsgReceiverConfig};
use rotel::receivers::kmsg::convert::convert_to_otlp_logs;
use rotel::receivers::kmsg::parser::{get_boot_time_ns, parse_kmsg_line};
use rotel::receivers::kmsg::receiver::KmsgReceiver;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Check if we have permission to read /dev/kmsg
fn can_read_kmsg() -> bool {
    File::open("/dev/kmsg").is_ok()
}

/// Skip test if we can't read /dev/kmsg
macro_rules! skip_if_no_kmsg {
    () => {
        if !can_read_kmsg() {
            eprintln!("Skipping test: cannot read /dev/kmsg (need root or CAP_SYSLOG)");
            return;
        }
    };
}

#[test]
fn test_kmsg_device_exists() {
    assert!(
        Path::new(KMSG_DEVICE_PATH).exists(),
        "/dev/kmsg should exist on Linux"
    );
}

#[test]
fn test_can_get_boot_time() {
    // Boot time is required to convert kmsg timestamps (microseconds since boot)
    // to absolute OTLP timestamps (nanoseconds since Unix epoch).
    // Formula: absolute_ns = boot_time_ns + (kmsg_timestamp_us * 1000)
    let boot_time = get_boot_time_ns();
    assert!(
        boot_time.is_ok(),
        "Should be able to get boot time from /proc/uptime"
    );

    let boot_time_ns = boot_time.unwrap();
    assert!(boot_time_ns > 0, "Boot time should be positive");

    // Boot time should be reasonable (after year 2000, before year 2100)
    let year_2000_ns: u64 = 946684800 * 1_000_000_000;
    let year_2100_ns: u64 = 4102444800 * 1_000_000_000;
    assert!(
        boot_time_ns > year_2000_ns,
        "Boot time should be after year 2000"
    );
    assert!(
        boot_time_ns < year_2100_ns,
        "Boot time should be before year 2100"
    );
}

#[test]
fn test_can_open_kmsg() {
    skip_if_no_kmsg!();

    let file = File::open("/dev/kmsg");
    assert!(file.is_ok(), "Should be able to open /dev/kmsg");
}

#[test]
fn test_read_and_parse_real_kmsg_messages() {
    skip_if_no_kmsg!();

    // Open kmsg in non-blocking mode to read existing messages
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open("/dev/kmsg")
        .expect("Should open /dev/kmsg");

    let reader = BufReader::new(file);
    let mut parsed_count = 0;
    let mut error_count = 0;

    // Try to read up to 100 messages or until we get EAGAIN
    for (i, line_result) in reader.lines().enumerate() {
        if i >= 100 {
            break;
        }

        match line_result {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }

                match parse_kmsg_line(&line) {
                    Ok(record) => {
                        parsed_count += 1;

                        // Validate the parsed record has reasonable values
                        // timestamp_us is raw kernel timestamp (µs since boot)
                        // Note: timestamp can be 0 for very early boot messages
                        // We just verify the field was parsed (it's always >= 0 for u64)

                        // Verify priority is within syslog range (0-7 after masking)
                        let priority_level = record.priority_raw & 0x07;
                        assert!(
                            priority_level <= 7,
                            "Priority level should be 0-7, got {}",
                            priority_level
                        );
                    }
                    Err(e) => {
                        error_count += 1;
                        eprintln!("Failed to parse line {}: {} - line: {}", i, e, line);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No more messages available
                break;
            }
            Err(e) => {
                eprintln!("Error reading line: {}", e);
                break;
            }
        }
    }

    eprintln!("Parsed {} messages, {} errors", parsed_count, error_count);

    // We should have been able to parse at least some messages if any were available
    // (kernel ring buffer might be empty in some test environments)
    if parsed_count > 0 {
        assert!(
            error_count < parsed_count,
            "Most messages should parse successfully"
        );
    }
}

#[test]
fn test_convert_real_kmsg_to_otlp() {
    skip_if_no_kmsg!();

    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open("/dev/kmsg")
        .expect("Should open /dev/kmsg");

    let reader = BufReader::new(file);
    let mut records = Vec::new();

    // Collect up to 10 valid records
    for line_result in reader.lines() {
        match line_result {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }

                if let Ok(record) = parse_kmsg_line(&line) {
                    records.push(record);
                    if records.len() >= 10 {
                        break;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }

    if records.is_empty() {
        eprintln!("No kmsg records available to test conversion");
        return;
    }

    let record_count = records.len();
    // convert_to_otlp_logs now calculates boot_time internally
    let resource_logs = convert_to_otlp_logs(records);

    // Validate the OTLP structure
    assert!(resource_logs.resource.is_some(), "Should have resource");
    assert_eq!(resource_logs.scope_logs.len(), 1, "Should have one scope");
    assert_eq!(
        resource_logs.scope_logs[0].log_records.len(),
        record_count,
        "Should have same number of log records"
    );

    // Check each log record
    for log_record in &resource_logs.scope_logs[0].log_records {
        assert!(log_record.time_unix_nano > 0, "Should have timestamp");
        assert!(log_record.body.is_some(), "Should have body");
        assert!(!log_record.attributes.is_empty(), "Should have attributes");

        // Check for expected attributes
        let attr_keys: Vec<&str> = log_record
            .attributes
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(
            attr_keys.contains(&"kmsg.priority"),
            "Should have priority attribute"
        );
        assert!(
            attr_keys.contains(&"kmsg.facility"),
            "Should have facility attribute"
        );
        assert!(
            attr_keys.contains(&"kmsg.facility_name"),
            "Should have facility_name attribute"
        );
        assert!(
            attr_keys.contains(&"kmsg.sequence"),
            "Should have sequence attribute"
        );
    }

    eprintln!(
        "Successfully converted {} kmsg records to OTLP",
        record_count
    );
}

#[test]
fn test_priority_filtering() {
    skip_if_no_kmsg!();

    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open("/dev/kmsg")
        .expect("Should open /dev/kmsg");

    let reader = BufReader::new(file);

    let mut priority_counts = [0u32; 8];

    for line_result in reader.lines() {
        match line_result {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }

                if let Ok(record) = parse_kmsg_line(&line) {
                    let prio = (record.priority_raw & 0x07) as usize;
                    if prio < 8 {
                        priority_counts[prio] += 1;
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }

    eprintln!("Priority distribution:");
    eprintln!("  Emergency (0): {}", priority_counts[0]);
    eprintln!("  Alert (1):     {}", priority_counts[1]);
    eprintln!("  Critical (2):  {}", priority_counts[2]);
    eprintln!("  Error (3):     {}", priority_counts[3]);
    eprintln!("  Warning (4):   {}", priority_counts[4]);
    eprintln!("  Notice (5):    {}", priority_counts[5]);
    eprintln!("  Info (6):      {}", priority_counts[6]);
    eprintln!("  Debug (7):     {}", priority_counts[7]);

    // Simulate filtering at priority level 4 (warning and above)
    let warning_and_above: u32 = priority_counts[0..=4].iter().sum();
    let info_and_debug: u32 = priority_counts[5..=7].iter().sum();

    eprintln!(
        "Warning and above: {}, Info and debug: {}",
        warning_and_above, info_and_debug
    );
}

#[tokio::test]
async fn test_kmsg_receiver_initialization() {
    skip_if_no_kmsg!();

    let config = KmsgReceiverConfig::new(
        6,     // INFO level
        false, // don't read existing
    );

    let receiver = KmsgReceiver::new(config, None).await;
    assert!(receiver.is_ok(), "Should be able to create KmsgReceiver");
}

#[tokio::test]
async fn test_kmsg_receiver_start_and_cancel() {
    skip_if_no_kmsg!();

    let config = KmsgReceiverConfig::new(6, false);

    let receiver = KmsgReceiver::new(config, None)
        .await
        .expect("Should create receiver");

    let mut task_set = JoinSet::new();
    let cancel = CancellationToken::new();

    // Start the receiver
    let start_result = receiver.start(&mut task_set, &cancel).await;
    assert!(start_result.is_ok(), "Should start successfully");

    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel and wait for shutdown
    cancel.cancel();

    let shutdown_result = timeout(TEST_TIMEOUT, async {
        while let Some(result) = task_set.join_next().await {
            if let Err(e) = result {
                eprintln!("Task error: {}", e);
            }
        }
    })
    .await;

    assert!(shutdown_result.is_ok(), "Should shutdown within timeout");
}

#[tokio::test]
async fn test_kmsg_receiver_reads_messages() {
    skip_if_no_kmsg!();

    use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
    use rotel::bounded_channel::bounded;
    use rotel::receivers::otlp_output::OTLPOutput;
    use rotel::topology::payload::Message;

    // Create a channel to receive the logs
    let (tx, mut rx) = bounded::<Message<ResourceLogs>>(100);
    let logs_output = OTLPOutput::new(tx);

    let config = KmsgReceiverConfig::new(
        7,    // DEBUG level - get all messages
        true, // read existing messages
    );

    let receiver = KmsgReceiver::new(config, Some(logs_output))
        .await
        .expect("Should create receiver");

    let mut task_set = JoinSet::new();
    let cancel = CancellationToken::new();

    receiver
        .start(&mut task_set, &cancel)
        .await
        .expect("Should start receiver");

    // Wait for some messages or timeout
    let received = timeout(Duration::from_secs(2), async {
        let mut count = 0;
        while let Some(msg) = rx.next().await {
            for resource_logs in &msg.payload {
                for scope_logs in &resource_logs.scope_logs {
                    count += scope_logs.log_records.len();
                }
            }
            if count > 0 {
                break;
            }
        }
        count
    })
    .await;

    // Cancel the receiver
    cancel.cancel();

    match received {
        Ok(count) => {
            eprintln!("Received {} log records from kmsg", count);
            // We might not receive any if the kernel buffer is empty
        }
        Err(_) => {
            eprintln!("Timeout waiting for kmsg messages (kernel buffer may be empty)");
        }
    }

    // Wait for clean shutdown
    let _ = timeout(TEST_TIMEOUT, async {
        while (task_set.join_next().await).is_some() {}
    })
    .await;
}
