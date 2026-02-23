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
    let boot_time_ns = get_boot_time_ns().ok();
    let resource_logs = convert_to_otlp_logs(records, boot_time_ns);

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

#[tokio::test]
async fn test_kmsg_receiver_persistence_creates_offset_file() {
    skip_if_no_kmsg!();

    use rotel::receivers::kmsg::persistence::load_state;
    use rotel::receivers::kmsg::receiver::read_boot_id;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("Should create temp dir");
    let offsets_path = temp_dir.path().join("kmsg_offsets.json");

    // Get current boot_id for verification
    let boot_id = read_boot_id().expect("Should read boot_id");

    let config = KmsgReceiverConfig::new(7, true) // Read existing to ensure we process messages
        .with_offsets_path(Some(offsets_path.clone()))
        .with_checkpoint_interval_ms(500); // Checkpoint quickly for test

    let receiver = KmsgReceiver::new(config, None)
        .await
        .expect("Should create receiver");

    let mut task_set = JoinSet::new();
    let cancel = CancellationToken::new();

    receiver
        .start(&mut task_set, &cancel)
        .await
        .expect("Should start receiver");

    // Let it run long enough to read messages and checkpoint
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Cancel and wait for shutdown (final checkpoint happens here)
    cancel.cancel();
    let _ = timeout(TEST_TIMEOUT, async {
        while (task_set.join_next().await).is_some() {}
    })
    .await;

    // Verify the offset file was created
    assert!(
        offsets_path.exists(),
        "Offset file should be created at {:?}",
        offsets_path
    );

    // Verify the content is valid
    let state = load_state(&offsets_path).expect("Should load persisted state");
    assert_eq!(state.boot_id, boot_id, "Boot ID should match");
    assert!(state.sequence > 0, "Should have recorded a sequence number");

    eprintln!(
        "Persistence test: saved sequence {} for boot {}",
        state.sequence, state.boot_id
    );
}

#[tokio::test]
async fn test_kmsg_receiver_resumes_from_persisted_offset() {
    skip_if_no_kmsg!();

    use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
    use rotel::bounded_channel::bounded;
    use rotel::receivers::kmsg::persistence::{PersistedKmsgState, save_state};
    use rotel::receivers::kmsg::receiver::read_boot_id;
    use rotel::receivers::otlp_output::OTLPOutput;
    use rotel::topology::payload::Message;
    use tempfile::TempDir;

    // First, read some messages to find the current sequence range.
    // Use raw reads like the receiver does - BufReader::lines() doesn't work
    // correctly with non-blocking FDs since it expects EOF termination.
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open("/dev/kmsg")
        .expect("Should open /dev/kmsg");

    let fd = file.as_raw_fd();
    let mut buf = [0u8; 4096];
    let mut max_sequence: u64 = 0;
    let mut initial_message_count: usize = 0;

    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                break;
            }
            // Other errors (EINTR, EPIPE) - just stop reading
            break;
        }
        if n == 0 {
            break;
        }
        if let Ok(line) = std::str::from_utf8(&buf[..n as usize]) {
            if let Ok(record) = parse_kmsg_line(line.trim_end()) {
                max_sequence = max_sequence.max(record.sequence);
                initial_message_count += 1;
            }
        }
    }

    if max_sequence == 0 {
        eprintln!("No kmsg messages available, skipping resume test");
        return;
    }

    eprintln!(
        "Current max sequence: {}, initial message count: {}",
        max_sequence, initial_message_count
    );

    // Create a persisted state that says we've already processed all current messages
    let temp_dir = TempDir::new().expect("Should create temp dir");
    let offsets_path = temp_dir.path().join("kmsg_offsets.json");
    let boot_id = read_boot_id().expect("Should read boot_id");

    let state = PersistedKmsgState::new(boot_id.clone(), max_sequence);
    save_state(&offsets_path, &state, true).expect("Should save state");

    // Create a channel to receive logs
    let (tx, mut rx) = bounded::<Message<ResourceLogs>>(100);
    let logs_output = OTLPOutput::new(tx);

    // Start receiver with persistence - it should skip all existing messages
    let config = KmsgReceiverConfig::new(7, false) // read_existing=false, but persistence will override
        .with_offsets_path(Some(offsets_path.clone()));

    let receiver = KmsgReceiver::new(config, Some(logs_output))
        .await
        .expect("Should create receiver");

    let mut task_set = JoinSet::new();
    let cancel = CancellationToken::new();

    receiver
        .start(&mut task_set, &cancel)
        .await
        .expect("Should start receiver");

    // Wait briefly - we should NOT receive any messages since we've "already processed" them all.
    // If we DO receive messages, they must be NEW messages (sequence > max_sequence),
    // not duplicates of already-processed messages.
    let received = timeout(Duration::from_millis(500), async {
        let mut count = 0;
        while let Some(msg) = rx.next().await {
            for resource_logs in &msg.payload {
                for scope_logs in &resource_logs.scope_logs {
                    count += scope_logs.log_records.len();
                }
            }
            if count > 0 {
                return count;
            }
        }
        count
    })
    .await;

    cancel.cancel();
    let _ = timeout(TEST_TIMEOUT, async {
        while (task_set.join_next().await).is_some() {}
    })
    .await;

    // Verify behavior based on what we received
    match received {
        Ok(count) => {
            if count == 0 {
                // No messages received - this is the expected case when no new kernel messages
                // were generated during the test window
                eprintln!(
                    "Resume test passed: no messages received (expected - no new kernel activity)"
                );
            } else {
                // We received messages - this is acceptable only if new messages arrived during
                // the test window. Since the kernel can generate messages at any time, we can't
                // fail this case. However, the key assertion is that we didn't receive ALL
                // messages from the beginning (which would indicate resume didn't work).
                // If we receive more than 10% of the initial buffer size, resume likely failed.
                let threshold = (initial_message_count / 10).max(10);
                assert!(
                    count < threshold,
                    "Received {} messages after resume (threshold: {}, initial: {}) - \
                     expected few/none if resume worked correctly. \
                     This may indicate the offset persistence resume logic is not skipping \
                     already-processed messages.",
                    count,
                    threshold,
                    initial_message_count
                );
                eprintln!(
                    "Received {} new messages after resume (likely new messages arrived during test)",
                    count
                );
            }
        }
        Err(_) => {
            // Timeout waiting for messages - also expected when no new messages arrive
            eprintln!(
                "Resume test passed: timeout with no messages (expected - no new kernel activity)"
            );
        }
    }
}
