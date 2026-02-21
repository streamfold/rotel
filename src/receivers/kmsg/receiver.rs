// SPDX-License-Identifier: Apache-2.0

//! Kmsg receiver implementation
//!
//! Reads kernel log messages from /dev/kmsg and sends them to the OTLP pipeline.

use crate::receivers::get_meter;
use crate::receivers::kmsg::config::{KMSG_DEVICE_PATH, KmsgReceiverConfig};
use crate::receivers::kmsg::convert::convert_to_otlp_logs;
use crate::receivers::kmsg::error::{KmsgReceiverError, Result};
use crate::receivers::kmsg::parser::{KmsgRecord, parse_kmsg_line};
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
    /// Record was filtered out by priority
    Filtered,
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
    /// Parse or UTF-8 error, logged and skipped
    Skipped,
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
            return ReadResult::Skipped;
        }
    };

    if line.is_empty() {
        return ReadResult::Skipped;
    }

    match parse_kmsg_line(line) {
        Ok(record) => {
            if (record.priority_raw & 0x07) <= priority_level {
                ReadResult::Record(record)
            } else {
                ReadResult::Filtered
            }
        }
        Err(e) => {
            warn!("Failed to parse kmsg line: {}", e);
            ReadResult::Skipped
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
    let file = open_kmsg(config.read_existing)?;
    let async_fd = AsyncFd::with_interest(file, Interest::READABLE)
        .map_err(|e| format!("Failed to create AsyncFd for kmsg: {}", e))?;

    debug!("Kmsg reader started with event-based I/O");

    let mut batch_sender = BatchSender::new(&logs_output, config.batch_size);
    let mut batch_timer =
        tokio::time::interval(tokio::time::Duration::from_millis(config.batch_timeout_ms));
    batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    batch_timer.tick().await; // Skip the first immediate tick

    let mut filtered_count: u64 = 0;
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
                            batch_sender.push(record);
                            batch_sender.try_send_batch(false);
                        }
                        ReadResult::Filtered => filtered_count += 1,
                        ReadResult::Interrupted => continue,
                        ReadResult::Skipped => continue,
                        ReadResult::WouldBlock | ReadResult::Overflow | ReadResult::Eof => {
                            guard.clear_ready();
                            break;
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

    // Shutdown: complete pending send and flush remaining records
    let (pending_accepted, pending_refused) = batch_sender.complete_pending_send().await;
    metrics.add_accepted(pending_accepted);
    metrics.add_refused(pending_refused);

    let (accepted, refused) = batch_sender.flush().await;
    metrics.add_accepted(accepted);
    metrics.add_refused(refused);

    metrics.add_filtered(filtered_count);

    info!("Kmsg receiver stopped");
    Ok(())
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
}
