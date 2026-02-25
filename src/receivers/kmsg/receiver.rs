// SPDX-License-Identifier: Apache-2.0

//! Kmsg receiver implementation
//!
//! Reads kernel log messages from /dev/kmsg and sends them to the OTLP pipeline.
//! Implements at-least-once delivery by tracking message acknowledgements from
//! exporters and only persisting offsets after messages are successfully exported.

use crate::bounded_channel::{BoundedReceiver, BoundedSender, bounded};
use crate::receivers::get_meter;
use crate::receivers::kmsg::config::{KMSG_DEVICE_PATH, KmsgReceiverConfig};
use crate::receivers::kmsg::convert::convert_to_otlp_logs;
use crate::receivers::kmsg::error::{KmsgReceiverError, Result};
use crate::receivers::kmsg::offset_committer::{
    KmsgOffsetCommitter, OffsetCommitterConfig, SharedOffsetTracker,
};
use crate::receivers::kmsg::offset_tracker::KmsgOffsetTracker;
use crate::receivers::kmsg::parser::{KmsgRecord, get_boot_time_ns, parse_kmsg_line};
use crate::receivers::kmsg::persistence;
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload::{self, KmsgAcknowledgement, KmsgMetadata, MessageMetadata};
use flume::r#async::SendFut;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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
            offsets_path = config
                .offsets_path
                .as_ref()
                .map(|p| p.display().to_string())
                .as_deref()
                .unwrap_or("disabled"),
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

/// Struct to hold batch state and handle sending with at-least-once delivery
struct BatchSender<'a> {
    records: Vec<KmsgRecord>,
    /// Sequence numbers of records in the current batch (for offset tracking)
    sequences: Vec<u64>,
    logs_output: &'a Option<OTLPOutput<payload::Message<ResourceLogs>>>,
    pending_send: Option<SendFut<'a, payload::Message<ResourceLogs>>>,
    pending_send_count: u64,
    batch_size: usize,
    /// Cached boot time (ns since epoch) for timestamp conversion.
    /// Computed once at receiver start to avoid syscall overhead later.
    boot_time_ns: Option<u64>,
    /// Channel to send acknowledgements back to the offset committer
    ack_tx: Option<BoundedSender<KmsgAcknowledgement>>,
    /// Shared offset tracker for at-least-once delivery
    offset_tracker: Option<SharedOffsetTracker>,
}

impl<'a> BatchSender<'a> {
    fn new(
        logs_output: &'a Option<OTLPOutput<payload::Message<ResourceLogs>>>,
        batch_size: usize,
        ack_tx: Option<BoundedSender<KmsgAcknowledgement>>,
        offset_tracker: Option<SharedOffsetTracker>,
    ) -> Self {
        // Compute boot time once at the start for timestamp conversion.
        // If this fails, convert_to_otlp_logs will fall back to observed_time.
        let boot_time_ns = match get_boot_time_ns() {
            Ok(t) => Some(t),
            Err(e) => {
                warn!(
                    "Failed to get boot time for timestamp conversion: {}. \
                     Using observed time as fallback.",
                    e
                );
                None
            }
        };

        Self {
            records: Vec::with_capacity(batch_size),
            sequences: Vec::with_capacity(batch_size),
            logs_output,
            pending_send: None,
            pending_send_count: 0,
            batch_size,
            boot_time_ns,
            ack_tx,
            offset_tracker,
        }
    }

    fn push(&mut self, record: KmsgRecord) {
        self.sequences.push(record.sequence);
        self.records.push(record);
    }

    /// Clear buffered records without sending.
    /// Used on ring buffer overflow when buffered records reference lost messages.
    fn clear_buffer(&mut self) {
        self.records.clear();
        self.sequences.clear();
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
    /// Must be called after `poll_pending` returns to remove the completed future.
    fn clear_pending(&mut self) {
        self.pending_send = None;
        self.pending_send_count = 0;
    }

    /// Create a payload message from the current batch of records.
    /// Tracks sequences in the offset tracker before creating the message.
    /// Returns the message and the record count, or None if the buffer is empty.
    fn create_payload_message(&mut self) -> Option<(payload::Message<ResourceLogs>, u64)> {
        if self.records.is_empty() {
            return None;
        }

        let records = std::mem::take(&mut self.records);
        let sequences = std::mem::take(&mut self.sequences);
        let count = records.len() as u64;

        // Track sequences in offset tracker before sending (at-least-once)
        if let Some(tracker) = &self.offset_tracker {
            let mut tracker = tracker
                .lock()
                .expect("offset tracker mutex poisoned - this is a bug");
            tracker.track_batch(&sequences);
        }

        let resource_logs = convert_to_otlp_logs(records, self.boot_time_ns);

        // Create a message with metadata for acknowledgement tracking
        let metadata = self.ack_tx.as_ref().map(|ack_tx| {
            let kmsg_metadata = KmsgMetadata::new(sequences, Some(ack_tx.clone()));
            MessageMetadata::kmsg(kmsg_metadata)
        });

        let payload_msg = payload::Message::new(metadata, vec![resource_logs], None);
        Some((payload_msg, count))
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
            if let Some((payload_msg, count)) = self.create_payload_message() {
                self.pending_send = Some(logs_output.send_async(payload_msg));
                self.pending_send_count = count;
                return true;
            }
        } else {
            // No output configured, just discard
            self.records.clear();
            self.sequences.clear();
        }
        false
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
        if let Some(logs_output) = self.logs_output {
            if let Some((payload_msg, count)) = self.create_payload_message() {
                return match logs_output.send(payload_msg).await {
                    Ok(_) => (count, 0),
                    Err(e) => {
                        warn!("Failed to send final batch: {}", e);
                        (0, count)
                    }
                };
            }
        }
        (0, 0)
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

    // Set up at-least-once delivery infrastructure when persistence is enabled
    let (ack_tx, offset_tracker, committer_handle) =
        if let (Some(boot_id), Some(offsets_path)) = (&current_boot_id, &config.offsets_path) {
            let (tx, rx): (
                BoundedSender<KmsgAcknowledgement>,
                BoundedReceiver<KmsgAcknowledgement>,
            ) = bounded(1024);
            let tracker: SharedOffsetTracker = Arc::new(Mutex::new(KmsgOffsetTracker::new(false)));

            // Spawn offset committer task
            let committer_config = OffsetCommitterConfig {
                checkpoint_interval: Duration::from_millis(config.checkpoint_interval_ms),
                drain_timeout: Duration::from_secs(2),
                max_checkpoint_failure_duration: Duration::from_secs(60),
            };

            let mut committer = KmsgOffsetCommitter::new(
                rx,
                tracker.clone(),
                offsets_path.clone(),
                boot_id.clone(),
                committer_config,
            );

            let committer_cancel = cancel.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = committer.run(committer_cancel).await {
                    error!("Kmsg offset committer error: {}", e);
                }
            });

            (Some(tx), Some(tracker), Some(handle))
        } else {
            (None, None, None)
        };

    // Keep a reference to offset_tracker for use outside BatchSender
    // (overflow clearing, filtered record acknowledgement)
    let offset_tracker_ref = offset_tracker.clone();

    let mut batch_sender =
        BatchSender::new(&logs_output, config.batch_size, ack_tx, offset_tracker);
    let mut batch_timer =
        tokio::time::interval(tokio::time::Duration::from_millis(config.batch_timeout_ms));
    batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    batch_timer.tick().await; // Skip the first immediate tick

    let mut filtered_count: u64 = 0;
    let mut skip_count: u64 = 0;
    let mut read_first_failure: Option<Instant> = None;
    let mut fatal_read_error = false;
    let mut read_buf = [0u8; READ_BUF_SIZE];

    loop {
        if fatal_read_error {
            break;
        }
        select! {
            biased;

            // Handle pending send completion
            send_result = poll_pending(batch_sender.pending_send_mut()),
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
                            // Successful read - reset failure tracking
                            if read_first_failure.take().is_some() {
                                debug!("Read succeeded after previous failures");
                            }

                            // Skip records we've already processed (resuming from checkpoint)
                            // The persisted sequence is the one we want to resume FROM,
                            // so we skip sequences strictly less than it.
                            if let Some(resume_from) = skip_until_sequence {
                                if record.sequence < resume_from {
                                    skip_count += 1;
                                    // Log progress and yield periodically to avoid starving other tasks
                                    if skip_count % 1000 == 0 {
                                        debug!(
                                            skipped = skip_count,
                                            current_sequence = record.sequence,
                                            target_sequence = resume_from,
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
                            // Successful read - reset failure tracking
                            if read_first_failure.take().is_some() {
                                debug!("Read succeeded after previous failures");
                            }

                            // For filtered records during resume, skip if before resume point
                            if let Some(resume_from) = skip_until_sequence {
                                if seq < resume_from {
                                    skip_count += 1;
                                    continue;
                                }
                                // Caught up via filtered record - clear skip state
                                skip_until_sequence = None;
                                info!(
                                    sequence = seq,
                                    skipped = skip_count,
                                    "Caught up to persisted offset (via filtered record), processing new messages"
                                );
                            }

                            // Track a filtered record as acknowledged (it won't go through a pipeline)
                            // This keeps hwm accurate and ensures checkpoint reflects all seen messages
                            if let Some(tracker) = &offset_tracker_ref {
                                let mut tracker = tracker
                                    .lock()
                                    .expect("offset tracker mutex poisoned - this is a bug");
                                tracker.acknowledge(seq);
                            }

                            filtered_count += 1;
                        }
                        ReadResult::Interrupted => continue,
                        ReadResult::Skipped(seq) => {
                            // Parse error - handle resume skip and acknowledge if we have a sequence
                            if let Some(s) = seq {
                                // During resume, skip if before resume point
                                if let Some(resume_from) = skip_until_sequence {
                                    if s < resume_from {
                                        skip_count += 1;
                                        continue;
                                    }
                                    // Caught up via skipped record - clear skip state
                                    skip_until_sequence = None;
                                    info!(
                                        sequence = s,
                                        skipped = skip_count,
                                        "Caught up to persisted offset (via malformed record), processing new messages"
                                    );
                                }

                                // Acknowledge to keep hwm accurate
                                if let Some(tracker) = &offset_tracker_ref {
                                    let mut tracker = tracker
                                        .lock()
                                        .expect("offset tracker mutex poisoned - this is a bug");
                                    tracker.acknowledge(s);
                                }
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
                            // Clear buffered records - they reference lost messages
                            batch_sender.clear_buffer();
                            // Clear offset tracker state - pending sequences may reference
                            // evicted messages, and hwm may point to a lost sequence.
                            // Note: In-flight batches (already sent to pipeline) may still
                            // ack back after this clear, updating hwm. This is acceptable
                            // for at-least-once: those messages were exported successfully,
                            // and we'll resume from the new hwm or next tracked sequence.
                            if let Some(tracker) = &offset_tracker_ref {
                                let mut tracker = tracker
                                    .lock()
                                    .expect("offset tracker mutex poisoned - this is a bug");
                                tracker.clear();
                                debug!("Cleared offset tracker state due to ring buffer overflow");
                            }
                            continue;
                        }
                        ReadResult::Error(e) => {
                            guard.clear_ready();

                            // Check for fatal errors that should exit immediately
                            match e.kind() {
                                std::io::ErrorKind::PermissionDenied => {
                                    error!(
                                        "Permission denied reading from {}. \
                                         Ensure the process has CAP_SYSLOG capability or is running as root.",
                                        KMSG_DEVICE_PATH
                                    );
                                    fatal_read_error = true;
                                    break;
                                }
                                std::io::ErrorKind::NotFound => {
                                    error!(
                                        "Device {} not found. \
                                         This may occur in containers or chroot environments without /dev/kmsg access.",
                                        KMSG_DEVICE_PATH
                                    );
                                    fatal_read_error = true;
                                    break;
                                }
                                _ => {
                                    // Track when failures started
                                    let first_failure = *read_first_failure.get_or_insert_with(Instant::now);
                                    let failure_duration = first_failure.elapsed();

                                    if failure_duration >= config.max_read_error_duration {
                                        error!(
                                            "Read errors persisted for {:?}, exiting: {}",
                                            failure_duration, e
                                        );
                                        fatal_read_error = true;
                                        break;
                                    }

                                    warn!(
                                        "Error reading from kmsg (failures started {:?} ago): {}",
                                        failure_duration, e
                                    );
                                    break;
                                }
                            }
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
    // The offset committer will handle final checkpointing after draining acks.
    let (pending_accepted, pending_refused) = batch_sender.complete_pending_send().await;
    metrics.add_accepted(pending_accepted);
    metrics.add_refused(pending_refused);

    let (accepted, refused) = batch_sender.flush().await;
    metrics.add_accepted(accepted);
    metrics.add_refused(refused);

    metrics.add_filtered(filtered_count);

    // Drop the batch_sender to close the ack channel, signaling the committer to drain
    drop(batch_sender);

    // Wait for the offset committer to finish draining acks and checkpointing
    if let Some(handle) = committer_handle {
        debug!("Waiting for offset committer to complete");
        if let Err(e) = handle.await {
            warn!("Offset committer task failed: {}", e);
        }
    }

    info!("Kmsg receiver stopped");
    Ok(())
}

/// Poll an Option<Future> in place without taking ownership.
///
/// The future stays in the Option; the caller must set it to None in the handler
/// after completion. This ensures the future isn't dropped if another select!
/// branch wins the race (e.g., cancellation), allowing shutdown code to
/// complete the pending operation.
async fn poll_pending<F>(fut_opt: &mut Option<F>) -> F::Output
where
    F: std::future::Future + Unpin,
{
    use std::pin::Pin;
    let fut = fut_opt.as_mut().expect("guarded by has_pending_send()");
    Pin::new(fut).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_sender_operations() {
        use crate::receivers::kmsg::parser::{Facility, Priority};

        let logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>> = None;
        let mut batch_sender = BatchSender::new(&logs_output, 100, None, None);

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
        assert!(batch_sender.sequences.is_empty()); // Sequences should also be cleared
    }

    #[test]
    fn test_batch_sender_tracks_sequences() {
        use crate::receivers::kmsg::parser::{Facility, Priority};

        let logs_output: Option<OTLPOutput<payload::Message<ResourceLogs>>> = None;
        let mut batch_sender = BatchSender::new(&logs_output, 100, None, None);

        let record1 = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 100,
            timestamp_us: 1000000,
            message: "test1".to_string(),
            is_continuation: false,
        };

        let record2 = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 101,
            timestamp_us: 2000000,
            message: "test2".to_string(),
            is_continuation: false,
        };

        batch_sender.push(record1);
        batch_sender.push(record2);

        assert_eq!(batch_sender.sequences.len(), 2);
        assert_eq!(batch_sender.sequences[0], 100);
        assert_eq!(batch_sender.sequences[1], 101);
    }

    #[tokio::test]
    async fn test_pending_send_not_dropped_on_shutdown() {
        use crate::receivers::kmsg::parser::{Facility, Priority};
        use crate::receivers::otlp_output::OTLPOutput;

        // Create a channel for the output
        let (tx, rx) = bounded::<payload::Message<ResourceLogs>>(10);
        let logs_output = Some(OTLPOutput::new(tx));

        let mut batch_sender = BatchSender::new(&logs_output, 2, None, None); // Small batch size

        // Create test records
        let record = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 1,
            timestamp_us: 1000000,
            message: "test message".to_string(),
            is_continuation: false,
        };

        // Push enough records to trigger a batch send
        batch_sender.push(record.clone());
        batch_sender.push(KmsgRecord {
            sequence: 2,
            ..record.clone()
        });

        // This should initiate a send (batch size reached)
        let sent = batch_sender.try_send_batch(false);
        assert!(sent, "Batch should have been sent");
        assert!(
            batch_sender.has_pending_send(),
            "Should have a pending send"
        );
        assert_eq!(batch_sender.pending_count(), 2);

        // Simulate shutdown: complete the pending send
        let (accepted, refused) = batch_sender.complete_pending_send().await;
        assert_eq!(accepted, 2, "Both records should be accepted");
        assert_eq!(refused, 0, "No records should be refused");

        // Verify the records actually arrived at the receiver
        let received = rx.try_recv();
        assert!(received.is_some(), "Should have received the batch");
    }

    #[tokio::test]
    async fn test_poll_pending_keeps_future_in_option_until_cleared() {
        use crate::receivers::kmsg::parser::{Facility, Priority};
        use crate::receivers::otlp_output::OTLPOutput;

        let (tx, rx) = bounded::<payload::Message<ResourceLogs>>(10);
        let logs_output = Some(OTLPOutput::new(tx));

        let mut batch_sender = BatchSender::new(&logs_output, 1, None, None);

        let record = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 1,
            timestamp_us: 1000000,
            message: "test".to_string(),
            is_continuation: false,
        };

        batch_sender.push(record);
        batch_sender.try_send_batch(false);

        assert!(batch_sender.has_pending_send());

        // Poll the pending send to completion
        let result = poll_pending(batch_sender.pending_send_mut()).await;
        assert!(result.is_ok());

        // poll_pending does not remove the future from the Option.
        // We must call clear_pending() to remove it.
        assert!(
            batch_sender.pending_send.is_some(),
            "Future should remain in Option until clear_pending is called"
        );

        batch_sender.clear_pending();
        assert!(!batch_sender.has_pending_send());

        // Verify record arrived
        assert!(rx.try_recv().is_some());
    }

    #[tokio::test]
    async fn test_batch_sender_tracks_sequences_in_offset_tracker() {
        use crate::receivers::kmsg::parser::{Facility, Priority};
        use crate::receivers::otlp_output::OTLPOutput;

        let (tx, _rx) = bounded::<payload::Message<ResourceLogs>>(10);
        let logs_output = Some(OTLPOutput::new(tx));

        // Create offset tracker
        let offset_tracker: SharedOffsetTracker =
            Arc::new(Mutex::new(KmsgOffsetTracker::new(false)));

        let mut batch_sender =
            BatchSender::new(&logs_output, 2, None, Some(offset_tracker.clone()));

        let record1 = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 100,
            timestamp_us: 1000000,
            message: "test1".to_string(),
            is_continuation: false,
        };

        let record2 = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 101,
            timestamp_us: 2000000,
            message: "test2".to_string(),
            is_continuation: false,
        };

        batch_sender.push(record1);
        batch_sender.push(record2);

        // Send the batch
        batch_sender.try_send_batch(true);

        // Verify sequences are tracked in offset tracker
        {
            let tracker = offset_tracker.lock().expect("test mutex");
            assert_eq!(tracker.pending_count(), 2);
            assert_eq!(tracker.lowest_pending(), Some(100));
        }
    }

    /// Test that KmsgMetadata is correctly attached to messages with sequences and ack channel
    #[tokio::test]
    async fn test_batch_sender_attaches_kmsg_metadata_to_messages() {
        use crate::receivers::kmsg::parser::{Facility, Priority};
        use crate::receivers::otlp_output::OTLPOutput;
        use crate::topology::payload::KmsgAcknowledgement;

        let (tx, mut rx) = bounded::<payload::Message<ResourceLogs>>(10);
        let logs_output = Some(OTLPOutput::new(tx));

        // Create ack channel
        let (ack_tx, _ack_rx) = bounded::<KmsgAcknowledgement>(10);

        let mut batch_sender = BatchSender::new(&logs_output, 2, Some(ack_tx), None);

        let record1 = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 100,
            timestamp_us: 1000000,
            message: "test1".to_string(),
            is_continuation: false,
        };

        let record2 = KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 101,
            timestamp_us: 2000000,
            message: "test2".to_string(),
            is_continuation: false,
        };

        batch_sender.push(record1);
        batch_sender.push(record2);

        // Send the batch
        batch_sender.try_send_batch(true);

        // Complete the send
        let (accepted, _) = batch_sender.complete_pending_send().await;
        assert_eq!(accepted, 2);

        // Receive the message and verify metadata
        let msg = rx.try_recv().expect("Should receive message");

        // Verify metadata is present
        assert!(msg.metadata.is_some(), "Message should have metadata");

        let metadata = msg.metadata.unwrap();

        // Verify it's Kmsg metadata with correct sequences
        if let payload::MessageMetadataInner::Kmsg(kmsg_meta) = metadata.inner() {
            assert_eq!(kmsg_meta.sequences, vec![100, 101]);
            assert!(
                kmsg_meta.ack_chan.is_some(),
                "Should have ack channel attached"
            );
        } else {
            panic!("Expected Kmsg metadata variant");
        }
    }

    /// Test the full acknowledgement flow: send message → receive → ack → verify tracker updated
    #[tokio::test]
    async fn test_full_ack_flow_updates_offset_tracker() {
        use crate::receivers::kmsg::parser::{Facility, Priority};
        use crate::receivers::otlp_output::OTLPOutput;
        use crate::topology::payload::{Ack, KmsgAcknowledgement};

        let (tx, mut rx) = bounded::<payload::Message<ResourceLogs>>(10);
        let logs_output = Some(OTLPOutput::new(tx));

        // Create ack channel and offset tracker
        let (ack_tx, mut ack_rx) = bounded::<KmsgAcknowledgement>(10);
        let offset_tracker: SharedOffsetTracker =
            Arc::new(Mutex::new(KmsgOffsetTracker::new(false)));

        let mut batch_sender =
            BatchSender::new(&logs_output, 2, Some(ack_tx), Some(offset_tracker.clone()));

        // Push records
        batch_sender.push(KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 100,
            timestamp_us: 1000000,
            message: "test1".to_string(),
            is_continuation: false,
        });
        batch_sender.push(KmsgRecord {
            priority_raw: 6,
            priority: Priority::Info,
            facility: Facility::Kern,
            sequence: 101,
            timestamp_us: 2000000,
            message: "test2".to_string(),
            is_continuation: false,
        });

        // Send the batch
        batch_sender.try_send_batch(true);
        let (accepted, _) = batch_sender.complete_pending_send().await;
        assert_eq!(accepted, 2);

        // Verify sequences are pending in tracker
        {
            let tracker = offset_tracker.lock().expect("test mutex");
            assert_eq!(tracker.pending_count(), 2);
            assert_eq!(tracker.get_persistable_sequence(), Some(100)); // lowest pending
        }

        // Receive message and call ack (simulating exporter behavior)
        let msg = rx.try_recv().expect("Should receive message");
        if let Some(ref metadata) = msg.metadata {
            metadata.ack().await.expect("Ack should succeed");
        }

        // Receive the ack on the ack channel (simulating offset committer)
        let ack = ack_rx.try_recv().expect("Should receive ack");
        match ack {
            KmsgAcknowledgement::Ack(kmsg_ack) => {
                assert_eq!(kmsg_ack.sequences, vec![100, 101]);

                // Process ack in tracker (what committer would do)
                let mut tracker = offset_tracker.lock().expect("test mutex");
                tracker.acknowledge_batch(&kmsg_ack.sequences);
            }
            _ => panic!("Expected Ack"),
        }

        // Verify tracker is now fully acknowledged
        {
            let tracker = offset_tracker.lock().expect("test mutex");
            assert_eq!(tracker.pending_count(), 0);
            assert_eq!(tracker.high_water_mark(), Some(101));
            assert_eq!(tracker.get_persistable_sequence(), Some(102)); // hwm + 1
        }
    }
}
