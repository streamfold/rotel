// SPDX-License-Identifier: Apache-2.0

//! Kmsg receiver implementation
//!
//! Reads kernel log messages from /dev/kmsg and sends them to the OTLP pipeline.

use crate::receivers::get_meter;
use crate::receivers::kmsg::config::{KMSG_DEVICE_PATH, KmsgReceiverConfig};
use crate::receivers::kmsg::convert::convert_to_otlp_logs;
use crate::receivers::kmsg::error::{KmsgReceiverError, Result};
use crate::receivers::kmsg::parser::{KmsgRecord, parse_kmsg_line};
use crate::receivers::kmsg::persistence::{self, PersistedKmsgState};
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use flume::r#async::SendFut;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::select;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tracing::{debug, error, info, warn};

/// Path to the Linux boot ID file
const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// Read the current Linux boot ID from `/proc/sys/kernel/random/boot_id`.
///
/// Returns the boot ID as a trimmed string, or an error if the file cannot be read.
#[cfg(target_os = "linux")]
pub fn read_boot_id() -> std::io::Result<String> {
    let boot_id = std::fs::read_to_string(BOOT_ID_PATH)?;
    Ok(boot_id.trim().to_string())
}

/// Stub for non-Linux platforms. Always returns an error.
#[cfg(not(target_os = "linux"))]
pub fn read_boot_id() -> std::io::Result<String> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "boot_id is only available on Linux",
    ))
}

pub struct KmsgReceiver {
    config: KmsgReceiverConfig,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
}

impl KmsgReceiver {
    pub async fn new(
        config: KmsgReceiverConfig,
        logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    ) -> Result<Self> {
        // Validate configuration
        config
            .validate()
            .map_err(KmsgReceiverError::ConfigurationError)?;

        if !std::path::Path::new(KMSG_DEVICE_PATH).exists() {
            return Err(KmsgReceiverError::ConfigurationError(format!(
                "kmsg device not found at: {}",
                KMSG_DEVICE_PATH
            )));
        }

        info!(
            device_path = KMSG_DEVICE_PATH,
            priority_level = config.priority_level,
            read_existing = config.read_existing,
            batch_size = config.batch_size,
            batch_timeout_ms = config.batch_timeout_ms,
            offsets_path = config.offsets_path.as_ref().map(|p| p.display().to_string()).as_deref().unwrap_or("disabled"),
            checkpoint_interval_ms = config.checkpoint_interval_ms,
            "Kmsg receiver initialized"
        );

        Ok(Self {
            config,
            logs_output,
        })
    }

    pub async fn start(
        self,
        task_set: &mut JoinSet<std::result::Result<(), BoxError>>,
        receivers_cancel: &CancellationToken,
    ) -> std::result::Result<(), BoxError> {
        let cancel = receivers_cancel.clone();
        let config = self.config.clone();
        let logs_output = self.logs_output.clone();

        info!(device_path = KMSG_DEVICE_PATH, "Kmsg receiver starting");

        task_set.spawn(async move {
            let result = run_kmsg_reader(config, logs_output, cancel).await;
            if let Err(ref e) = result {
                error!("Kmsg receiver error: {}", e);
            }
            result
        });

        Ok(())
    }
}

/// Struct to hold batch state and handle sending
struct BatchSender<'a> {
    records: Vec<KmsgRecord>,
    logs_output: &'a Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    pending_send: Option<SendFut<'a, payload::Message<ResourceLogs>>>,
    pending_send_count: u64,
    batch_size: usize,
}

impl<'a> BatchSender<'a> {
    fn new(
        logs_output: &'a Option<OTLPOutput<payload::Message<ResourceLogs>>>,
        batch_size: usize,
    ) -> Self {
        Self {
            records: Vec::with_capacity(batch_size),
            logs_output,
            pending_send: None,
            pending_send_count: 0,
            batch_size,
        }
    }

    fn push(&mut self, record: KmsgRecord) {
        self.records.push(record);
    }

    fn has_pending_send(&self) -> bool {
        self.pending_send.is_some()
    }

    fn pending_send_mut(&mut self) -> &mut Option<SendFut<'a, payload::Message<ResourceLogs>>> {
        &mut self.pending_send
    }

    fn pending_count(&self) -> u64 {
        self.pending_send_count
    }

    /// Clear pending send state after awaiting completes.
    /// Note: The future itself is already cleared by `conditional_wait` using `.take()`.
    fn clear_pending(&mut self) {
        self.pending_send = None; // Redundant but defensive
        self.pending_send_count = 0;
    }

    /// Try to send the current batch if conditions are met.
    /// Sends when batch is full or when `force` is true (e.g., timer tick or shutdown).
    /// Returns true if a send was initiated.
    fn try_send_batch(&mut self, force: bool) -> bool {
        if self.records.is_empty() || self.pending_send.is_some() {
            return false;
        }

        let should_send = force || self.records.len() >= self.batch_size;

        if !should_send {
            return false;
        }

        if let Some(logs_output) = self.logs_output {
            let records = std::mem::take(&mut self.records);
            let count = records.len() as u64;
            let resource_logs = convert_to_otlp_logs(records);
            let payload_msg = payload::Message::new(None, vec![resource_logs], None);
            self.pending_send = Some(logs_output.send_async(payload_msg));
            self.pending_send_count = count;
            true
        } else {
            // No output configured, just discard
            self.records.clear();
            false
        }
    }

    /// Wait for any in-flight pending send to complete (for shutdown)
    /// Returns (accepted, refused) counts for the pending send
    async fn complete_pending_send(&mut self) -> (u64, u64) {
        if let Some(pending) = self.pending_send.take() {
            let count = self.pending_send_count;
            self.pending_send_count = 0;
            return match pending.await {
                Ok(_) => (count, 0),
                Err(e) => {
                    warn!("Failed to complete pending send during shutdown: {}", e);
                    (0, count)
                }
            };
        }
        (0, 0)
    }

    /// Send remaining records synchronously (for shutdown)
    async fn flush(&mut self) -> (u64, u64) {
        let mut accepted = 0u64;
        let mut refused = 0u64;

        if !self.records.is_empty() {
            if let Some(logs_output) = self.logs_output {
                let records = std::mem::take(&mut self.records);
                let count = records.len() as u64;
                let resource_logs = convert_to_otlp_logs(records);
                let payload_msg = payload::Message::new(None, vec![resource_logs], None);
                match logs_output.send(payload_msg).await {
                    Ok(_) => accepted = count,
                    Err(e) => {
                        refused = count;
                        warn!("Failed to send final batch: {}", e);
                    }
                }
            }
        }

        (accepted, refused)
    }
}

/// Buffer size for reading from /dev/kmsg.
/// Kernel messages are limited by LOG_LINE_MAX (~1KB text) plus metadata.
/// 4KB provides ample headroom for any realistic message.
const READ_BUF_SIZE: usize = 4096;

/// Metrics counters for the kmsg receiver
struct ReceiverMetrics {
    accepted: Counter<u64>,
    refused: Counter<u64>,
    filtered: Counter<u64>,
    tags: [KeyValue; 1],
}

impl ReceiverMetrics {
    fn new() -> Self {
        Self {
            accepted: get_meter()
                .u64_counter("rotel_receiver_accepted_log_records")
                .with_description(
                    "Number of log records successfully ingested and pushed into the pipeline.",
                )
                .with_unit("log_records")
                .build(),
            refused: get_meter()
                .u64_counter("rotel_receiver_refused_log_records")
                .with_description(
                    "Number of log records that could not be pushed into the pipeline.",
                )
                .with_unit("log_records")
                .build(),
            filtered: get_meter()
                .u64_counter("rotel_receiver_filtered_log_records")
                .with_description("Number of log records filtered out by priority level.")
                .with_unit("log_records")
                .build(),
            tags: [KeyValue::new("receiver", "kmsg")],
        }
    }

    fn add_accepted(&self, count: u64) {
        if count > 0 {
            self.accepted.add(count, &self.tags);
        }
    }

    fn add_refused(&self, count: u64) {
        if count > 0 {
            self.refused.add(count, &self.tags);
        }
    }

    fn add_filtered(&self, count: u64) {
        if count > 0 {
            self.filtered.add(count, &self.tags);
        }
    }
}

/// Open /dev/kmsg and optionally seek to end
fn open_kmsg(read_existing: bool) -> std::result::Result<std::fs::File, BoxError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(KMSG_DEVICE_PATH)
        .map_err(|e| format!("Failed to open {}: {}", KMSG_DEVICE_PATH, e))?;

    if !read_existing {
        let fd = file.as_raw_fd();
        let result = unsafe { libc::lseek(fd, 0, libc::SEEK_END) };
        if result == -1 {
            let err = std::io::Error::last_os_error();
            return Err(format!("Failed to seek to end of kmsg: {}", err).into());
        }
        debug!("Seeked to end of kmsg ring buffer");
    }

    Ok(file)
}

/// Result of attempting to read from kmsg
enum ReadResult {
    /// Successfully read and parsed a record
    Record(KmsgRecord),
    /// Priority-filtered-out record (carries sequence for offset tracking)
    Filtered(u64),
    /// No more data available (EWOULDBLOCK)
    WouldBlock,
    /// Read was interrupted (EINTR), should retry
    Interrupted,
    /// Ring buffer overflow, seeked to end
    Overflow,
    /// Unexpected EOF
    Eof,
    /// Fatal error, should stop
    Error(std::io::Error),
    /// Parse or UTF-8 error, logged and skipped.
    /// Carries an optional sequence number when it could be extracted before the failure.
    Skipped(Option<u64>),
}

/// Read and parse a single kmsg record
fn read_one_record(fd: i32, buf: &mut [u8], priority_level: u8) -> ReadResult {
    let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };

    if n < 0 {
        let err = std::io::Error::last_os_error();
        return match err.kind() {
            std::io::ErrorKind::WouldBlock => ReadResult::WouldBlock,
            std::io::ErrorKind::Interrupted => ReadResult::Interrupted,
            std::io::ErrorKind::BrokenPipe => {
                warn!(
                    "Kernel ring buffer overflow detected (EPIPE). \
                     Some messages may have been lost. Seeking to end of buffer."
                );
                let result = unsafe { libc::lseek(fd, 0, libc::SEEK_END) };
                if result == -1 {
                    let seek_err = std::io::Error::last_os_error();
                    error!("Failed to seek to end after EPIPE: {}", seek_err);
                } else {
                    debug!("Recovered from ring buffer overflow, seeked to end");
                }
                ReadResult::Overflow
            }
            _ => ReadResult::Error(err),
        };
    }

    if n == 0 {
        return ReadResult::Eof;
    }

    let data = &buf[..n as usize];
    let line = match std::str::from_utf8(data) {
        Ok(s) => s.trim_end(),
        Err(e) => {
            warn!("Invalid UTF-8 in kmsg: {}", e);
            return ReadResult::Skipped(None);
        }
    };

    if line.is_empty() {
        return ReadResult::Skipped(None);
    }

    match parse_kmsg_line(line) {
        Ok(record) => {
            if (record.priority_raw & 0x07) <= priority_level {
                ReadResult::Record(record)
            } else {
                ReadResult::Filtered(record.sequence)
            }
        }
        Err(e) => {
            warn!("Failed to parse kmsg line: {}", e);
            // Best-effort sequence extraction: the sequence is the second
            // comma-separated field before the ';' separator.
            let seq = line
                .split_once(';')
                .and_then(|(header, _)| header.split(',').nth(1))
                .and_then(|s| s.parse::<u64>().ok());
            ReadResult::Skipped(seq)
        }
    }
}

/// Main receiver loop using event-based I/O with AsyncFd
async fn run_kmsg_reader(
    config: KmsgReceiverConfig,
    logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    cancel: CancellationToken,
) -> std::result::Result<(), BoxError> {
    let metrics = ReceiverMetrics::new();

    // Read boot_id once for use in both resume detection and checkpointing.
    // Only needed when persistence is enabled (offsets_path is Some).
    let current_boot_id = if config.offsets_path.is_some() {
        match read_boot_id() {
            Ok(id) => Some(id),
            Err(e) => {
                warn!(
                    "Failed to read boot_id: {}. Offset persistence is disabled for this session.",
                    e
                );
                None
            }
        }
    } else {
        debug!("Offset persistence is disabled");
        None
    };

    // Check for persisted state to determine startup behavior
    let resume_sequence = current_boot_id.as_deref().and_then(|boot_id| {
        config
            .offsets_path
            .as_deref()
            .and_then(|path| persistence::determine_start_sequence(path, boot_id))
    });
    let mut skip_until_sequence = resume_sequence;

    let effective_read_existing = if let Some(seq) = resume_sequence {
        // Override read_existing to true so we can catch up from persisted offset.
        // We'll read from the beginning of the ring buffer and skip already-processed
        // messages until we reach the persisted sequence number.
        info!(
            sequence = seq,
            "Resuming from persisted kmsg offset; reading from ring buffer start to catch up"
        );
        true
    } else {
        config.read_existing
    };

    let file = open_kmsg(effective_read_existing)?;
    let async_fd = AsyncFd::with_interest(file, Interest::READABLE)
        .map_err(|e| format!("Failed to create AsyncFd for kmsg: {}", e))?;

    debug!("Kmsg reader started with event-based I/O");

    let mut batch_sender = BatchSender::new(&logs_output, config.batch_size);
    let mut batch_timer =
        tokio::time::interval(tokio::time::Duration::from_millis(config.batch_timeout_ms));
    batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    batch_timer.tick().await; // Skip the first immediate tick

    // Only create a checkpoint timer when persistence is enabled (boot_id available)
    let mut checkpoint_timer = if current_boot_id.is_some() {
        let mut timer = tokio::time::interval(tokio::time::Duration::from_millis(
            config.checkpoint_interval_ms,
        ));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        timer.tick().await; // Skip the first immediate tick
        Some(timer)
    } else {
        None
    };

    let mut filtered_count: u64 = 0;
    let mut skip_count: u64 = 0;
    let mut max_sequence: Option<u64> = None;
    let mut last_checkpointed_sequence: Option<u64> = None;
    let mut checkpoint_failures: u32 = 0;
    let mut read_buf = [0u8; READ_BUF_SIZE];

    loop {
        select! {
            biased;

            // Handle pending send completion
            Some(send_result) = conditional_wait(batch_sender.pending_send_mut()),
                if batch_sender.has_pending_send() =>
            {
                let count = batch_sender.pending_count();
                batch_sender.clear_pending();
                match send_result {
                    Ok(_) => metrics.add_accepted(count),
                    Err(e) => {
                        metrics.add_refused(count);
                        error!("Failed to send logs to output channel: {}", e);
                        break;
                    }
                }
            }

            // Batch timer tick - flush pending records and report filtered metrics
            _ = batch_timer.tick() => {
                batch_sender.try_send_batch(true);
                metrics.add_filtered(filtered_count);
                filtered_count = 0;
            }

            // Checkpoint timer tick - persist current offset to disk.
            // When checkpoint_timer is None (persistence disabled), pending() never
            // resolves, effectively disabling this select branch.
            _ = async {
                if let Some(timer) = checkpoint_timer.as_mut() {
                    timer.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                (last_checkpointed_sequence, checkpoint_failures) = maybe_checkpoint(
                    &current_boot_id,
                    &config.offsets_path,
                    max_sequence,
                    last_checkpointed_sequence,
                    checkpoint_failures,
                    false,
                );
            }

            // Wait for data to be available
            ready_result = async_fd.readable() => {
                let Ok(mut guard) = ready_result else {
                    error!("AsyncFd readable error: {}", ready_result.unwrap_err());
                    break;
                };

                let fd = guard.get_inner().as_raw_fd();
                loop {
                    match read_one_record(fd, &mut read_buf, config.priority_level) {
                        ReadResult::Record(record) => {
                            // Always track max sequence, even for skipped records,
                            // so we don't lose the offset if stopped before new messages arrive
                            update_max_sequence(&mut max_sequence, record.sequence);

                            // Skip records we've already processed (resuming from checkpoint)
                            if let Some(skip_seq) = skip_until_sequence {
                                if record.sequence <= skip_seq {
                                    skip_count += 1;
                                    // Log progress and yield periodically to avoid starving other tasks
                                    if skip_count % 1000 == 0 {
                                        debug!(
                                            skipped = skip_count,
                                            current_sequence = record.sequence,
                                            target_sequence = skip_seq,
                                            "Skipping previously processed messages"
                                        );
                                        tokio::task::yield_now().await;
                                    }
                                    continue;
                                }
                                // Caught up, stop skipping
                                skip_until_sequence = None;
                                info!(
                                    sequence = record.sequence,
                                    skipped = skip_count,
                                    "Caught up to persisted offset, processing new messages"
                                );
                            }

                            batch_sender.push(record);
                            batch_sender.try_send_batch(false);
                        }
                        ReadResult::Filtered(seq) => {
                            update_max_sequence(&mut max_sequence, seq);
                            if skip_until_sequence.is_some() {
                                // During resume, count filtered records as skipped
                                // (they were already counted as filtered in the previous run)
                                skip_count += 1;
                            } else {
                                filtered_count += 1;
                            }
                        }
                        ReadResult::Interrupted => continue,
                        ReadResult::Skipped(seq) => {
                            if let Some(s) = seq {
                                update_max_sequence(&mut max_sequence, s);
                            }
                            continue;
                        }
                        ReadResult::WouldBlock | ReadResult::Eof => {
                            guard.clear_ready();
                            break;
                        }
                        ReadResult::Overflow => {
                            // Ring buffer overflowed — messages were lost and we
                            // seeked to the end. If we were still in the resume-skip
                            // phase, the target sequence was likely evicted, so stop
                            // skipping to avoid silently dropping all future records.
                            if skip_until_sequence.take().is_some() {
                                warn!(
                                    "Ring buffer overflow during resume; \
                                     some messages may be re-processed or lost"
                                );
                            }
                            // Clear stale offset to avoid checkpointing a sequence that
                            // may have been evicted. Continue reading to restore tracking
                            // from the next available message (or WouldBlock if none).
                            max_sequence = None;
                            continue;
                        }
                        ReadResult::Error(e) => {
                            error!("Error reading from kmsg: {}", e);
                            guard.clear_ready();
                            break;
                        }
                    }
                }
            }

            _ = cancel.cancelled() => {
                info!("Kmsg receiver cancelled");
                break;
            }
        }
    }

    // Shutdown: complete pending send and flush remaining records.
    // We reach here on cancellation, channel error, or read error. In all cases,
    // we checkpoint below to preserve progress and avoid reprocessing on restart.
    let (pending_accepted, pending_refused) = batch_sender.complete_pending_send().await;
    metrics.add_accepted(pending_accepted);
    metrics.add_refused(pending_refused);

    let (accepted, refused) = batch_sender.flush().await;
    metrics.add_accepted(accepted);
    metrics.add_refused(refused);

    metrics.add_filtered(filtered_count);

    // Final checkpoint before stopping (sync_dir=true for durability)
    let _ = maybe_checkpoint(
        &current_boot_id,
        &config.offsets_path,
        max_sequence,
        last_checkpointed_sequence,
        checkpoint_failures,
        true,
    );

    info!("Kmsg receiver stopped");
    Ok(())
}

/// Track the highest sequence number seen so far.
fn update_max_sequence(current: &mut Option<u64>, new_val: u64) {
    *current = Some(current.map_or(new_val, |s| s.max(new_val)));
}

/// Number of consecutive checkpoint failures before escalating to error level logging.
const CHECKPOINT_FAILURE_ESCALATION_THRESHOLD: u32 = 3;

/// Try to checkpoint the offset if persistence is enabled and the sequence has advanced.
///
/// Returns a tuple of (updated last_checkpointed_sequence, updated consecutive_failures).
#[must_use]
fn maybe_checkpoint(
    boot_id: &Option<String>,
    offsets_path: &Option<std::path::PathBuf>,
    max_sequence: Option<u64>,
    last_checkpointed: Option<u64>,
    consecutive_failures: u32,
    sync_dir: bool,
) -> (Option<u64>, u32) {
    let (Some(boot_id), Some(path)) = (boot_id.as_deref(), offsets_path.as_ref()) else {
        return (last_checkpointed, 0);
    };

    let Some(seq) = max_sequence else {
        return (last_checkpointed, consecutive_failures);
    };

    if Some(seq) == last_checkpointed {
        return (last_checkpointed, consecutive_failures);
    }

    let state = PersistedKmsgState::new(boot_id.to_owned(), seq);
    if let Err(e) = persistence::save_state(path, &state, sync_dir) {
        let new_failures = consecutive_failures.saturating_add(1);
        if new_failures >= CHECKPOINT_FAILURE_ESCALATION_THRESHOLD {
            error!(
                consecutive_failures = new_failures,
                "Checkpoint failure (persisted): {}. Offsets may be lost on restart.",
                e
            );
        } else {
            warn!(
                consecutive_failures = new_failures,
                "Failed to checkpoint kmsg offset: {}",
                e
            );
        }
        return (last_checkpointed, new_failures);
    }

    debug!(sequence = seq, "Checkpointed kmsg offset");
    (max_sequence, 0) // Reset failure count on success
}

/// Helper for conditional future waiting.
/// Takes the future out of the Option (leaving None) before awaiting,
/// ensuring the Option is automatically cleared after the await completes.
async fn conditional_wait<F>(fut_opt: &mut Option<F>) -> Option<F::Output>
where
    F: std::future::Future + Unpin,
{
    match fut_opt.take() {
        None => None,
        Some(fut) => Some(fut.await),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_sender_operations() {
        use crate::receivers::kmsg::parser::{Facility, Priority};

        let logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>> = None;
        let mut batch_sender = BatchSender::new(&logs_output, 100);

        assert!(!batch_sender.has_pending_send());

        let record = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 1,
            timestamp_us: 1000000, // 1 second since boot in µs
            message: "test".to_string(),
            is_continuation: false,
        };

        batch_sender.push(record.clone());
        assert!(!batch_sender.has_pending_send()); // No output, so no pending send

        // With no logs_output, try_send_batch should just clear records
        let sent = batch_sender.try_send_batch(true);
        assert!(!sent); // Returns false when no output configured
        assert!(batch_sender.records.is_empty()); // Records should be cleared
    }

    #[test]
    fn test_update_max_sequence_from_none() {
        let mut max_seq: Option<u64> = None;
        update_max_sequence(&mut max_seq, 100);
        assert_eq!(max_seq, Some(100));
    }

    #[test]
    fn test_update_max_sequence_increases() {
        let mut max_seq: Option<u64> = Some(50);
        update_max_sequence(&mut max_seq, 100);
        assert_eq!(max_seq, Some(100));
    }

    #[test]
    fn test_update_max_sequence_does_not_decrease() {
        let mut max_seq: Option<u64> = Some(100);
        update_max_sequence(&mut max_seq, 50);
        assert_eq!(max_seq, Some(100));
    }

    #[test]
    fn test_maybe_checkpoint_skips_when_persistence_disabled() {
        // With None for boot_id or path, should return last_checkpointed unchanged
        let (seq, failures) = maybe_checkpoint(&None, &None, Some(100), None, 0, false);
        assert_eq!(seq, None);
        assert_eq!(failures, 0);

        let (seq, failures) = maybe_checkpoint(
            &Some("boot-id".to_string()),
            &None,
            Some(100),
            Some(50),
            0,
            false,
        );
        assert_eq!(seq, Some(50));
        assert_eq!(failures, 0);
    }

    #[test]
    fn test_maybe_checkpoint_skips_when_no_sequence() {
        let (seq, failures) = maybe_checkpoint(
            &Some("test-boot-id".to_string()),
            &Some(std::path::PathBuf::from("/nonexistent/path")),
            None,
            None,
            0,
            false,
        );
        assert_eq!(seq, None);
        assert_eq!(failures, 0);
    }

    #[test]
    fn test_maybe_checkpoint_skips_when_unchanged() {
        let (seq, failures) = maybe_checkpoint(
            &Some("test-boot-id".to_string()),
            &Some(std::path::PathBuf::from("/nonexistent/path")),
            Some(100),
            Some(100),
            0,
            false,
        );
        assert_eq!(seq, Some(100));
        assert_eq!(failures, 0);
    }

    #[test]
    fn test_maybe_checkpoint_writes_when_advanced() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("offsets.json");

        let (seq, failures) = maybe_checkpoint(
            &Some("test-boot-id".to_string()),
            &Some(path.clone()),
            Some(100),
            None,
            0,
            true,
        );
        assert_eq!(seq, Some(100));
        assert_eq!(failures, 0); // Success resets failure count
        assert!(path.exists());

        // Verify the content
        let state = persistence::load_state(&path).unwrap();
        assert_eq!(state.boot_id, "test-boot-id");
        assert_eq!(state.sequence, 100);
    }

    #[test]
    fn test_maybe_checkpoint_successive_checkpoints() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("offsets.json");
        let boot_id = Some("test-boot-id".to_string());
        let offsets_path = Some(path.clone());

        // First checkpoint without sync_dir
        let (last, failures) = maybe_checkpoint(&boot_id, &offsets_path, Some(100), None, 0, false);
        assert_eq!(last, Some(100));
        assert_eq!(failures, 0);

        // Second checkpoint with sync_dir (simulating shutdown)
        let (last, failures) =
            maybe_checkpoint(&boot_id, &offsets_path, Some(200), last, failures, true);
        assert_eq!(last, Some(200));
        assert_eq!(failures, 0);

        let state = persistence::load_state(&path).unwrap();
        assert_eq!(state.sequence, 200);
    }

    #[test]
    fn test_maybe_checkpoint_tracks_consecutive_failures() {
        // Create a file, then try to use it as a directory - this will reliably fail
        let dir = tempfile::TempDir::new().unwrap();
        let blocker_file = dir.path().join("blocker");
        std::fs::write(&blocker_file, "I am a file, not a directory").unwrap();

        // Try to write to a path where the parent "directory" is actually a file
        let invalid_path = blocker_file.join("offsets.json");

        // First failure
        let (seq, failures) = maybe_checkpoint(
            &Some("test-boot-id".to_string()),
            &Some(invalid_path.clone()),
            Some(100),
            None,
            0,
            false,
        );
        assert_eq!(seq, None); // Unchanged due to failure
        assert_eq!(failures, 1);

        // Second failure
        let (seq, failures) = maybe_checkpoint(
            &Some("test-boot-id".to_string()),
            &Some(invalid_path.clone()),
            Some(100),
            seq,
            failures,
            false,
        );
        assert_eq!(seq, None);
        assert_eq!(failures, 2);

        // Third failure - should hit escalation threshold
        let (seq, failures) = maybe_checkpoint(
            &Some("test-boot-id".to_string()),
            &Some(invalid_path),
            Some(100),
            seq,
            failures,
            false,
        );
        assert_eq!(seq, None);
        assert_eq!(failures, 3);
    }

    #[test]
    fn test_maybe_checkpoint_resets_failures_on_success() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("offsets.json");

        // Simulate having had previous failures, then success
        let (seq, failures) = maybe_checkpoint(
            &Some("test-boot-id".to_string()),
            &Some(path),
            Some(100),
            None,
            5, // Had 5 previous failures
            false,
        );
        assert_eq!(seq, Some(100));
        assert_eq!(failures, 0); // Reset to 0 on success
    }
}
