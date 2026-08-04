// SPDX-License-Identifier: Apache-2.0

//! Node metrics receiver implementation
//!
//! Periodically collects system metrics and sends them to the pipeline.

use crate::receivers::get_meter;
use crate::receivers::node_metrics::collector::{CollectedMetric, SystemCollector};
use crate::receivers::node_metrics::config::NodeMetricsConfig;
use crate::receivers::node_metrics::convert::{convert_to_otlp_metrics, count_data_points};
use crate::receivers::otlp_output::OTLPOutput;
use crate::topology::payload;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use std::sync::Arc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tower::BoxError;
use tracing::{debug, error, info, warn};

/// Node metrics receiver that periodically scrapes system metrics
pub struct NodeMetricsReceiver {
    config: NodeMetricsConfig,
    metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
}

impl NodeMetricsReceiver {
    /// Create a new node metrics receiver
    ///
    /// The configuration is validated here, as the sibling receivers do, so a config built
    /// programmatically rather than from the CLI cannot bypass the checks.
    pub fn new(
        mut config: NodeMetricsConfig,
        metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
    ) -> Result<Self, BoxError> {
        config.normalize_and_validate()?;
        Ok(Self {
            config,
            metrics_output,
        })
    }

    /// Start the receiver, spawning a task that periodically collects metrics
    pub fn start(
        self,
        task_set: &mut JoinSet<Result<(), BoxError>>,
        receivers_cancel: &CancellationToken,
    ) -> Result<(), BoxError> {
        let cancel = receivers_cancel.clone();
        let config = self.config;
        let metrics_output = self.metrics_output;

        info!(
            scrape_interval = ?config.scrape_interval,
            collectors = ?config.collectors,
            "Node metrics receiver starting"
        );

        #[cfg(not(target_os = "linux"))]
        warn!(
            "Node metrics receiver is designed for Linux. \
             Most collectors read from /proc and /sys which may not be available on this platform."
        );

        task_set.spawn(async move {
            let result = run_scrape_loop(config, metrics_output, cancel).await;
            if let Err(ref e) = result {
                error!("Node metrics receiver error: {}", e);
            }
            result
        });

        Ok(())
    }
}

/// How long a final batch may take to hand off once cancellation has been requested
const SHUTDOWN_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Lower bound on how long a scrape may take before it is considered wedged
///
/// The timeout is the scrape interval or this, whichever is longer: a short interval should
/// not turn a merely slow host into a receiver that never reports at all.
const MIN_SCRAPE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// How long to wait for the pipeline to accept a batch before dropping it
///
/// An upper bound, further capped by the scrape interval at each use site, so a batch is
/// never held past the next tick and a long interval does not buy a long stall.
const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Metrics counters for the receiver
struct ReceiverMetrics {
    accepted: Counter<u64>,
    refused: Counter<u64>,
    scrape_failures: Counter<u64>,
    empty_scrapes: Counter<u64>,
    tags: [KeyValue; 1],
}

impl ReceiverMetrics {
    fn new() -> Self {
        Self {
            // Use the same accepted/refused vocabulary as the other receivers, so node
            // metrics show up on the existing internal-telemetry dashboards.
            // The description is part of the instrument identity, so it must match the
            // other receivers declaring these names or the SDK reports a duplicate
            // metric stream and exports two conflicting series.
            accepted: get_meter()
                .u64_counter("rotel_receiver_accepted_metric_points")
                .with_description(
                    "Number of metric points successfully ingested and pushed into the pipeline.",
                )
                .with_unit("metric_points")
                .build(),
            refused: get_meter()
                .u64_counter("rotel_receiver_refused_metric_points")
                .with_description(
                    "Number of metric points that could not be pushed into the pipeline.",
                )
                .with_unit("metric_points")
                .build(),
            scrape_failures: get_meter()
                .u64_counter("rotel_receiver_scrape_failures")
                .with_description("Number of failed scrape attempts.")
                .with_unit("scrapes")
                .build(),
            empty_scrapes: get_meter()
                .u64_counter("rotel_receiver_empty_scrapes")
                .with_description(
                    "Number of scrapes that collected no metrics at all, which usually means a \
                     misconfigured procfs/sysfs path or an over-broad metric filter.",
                )
                .with_unit("scrapes")
                .build(),
            tags: [KeyValue::new("receiver", "node_metrics")],
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

    fn add_scrape_failure(&self) {
        self.scrape_failures.add(1, &self.tags);
    }

    fn add_empty_scrape(&self) {
        self.empty_scrapes.add(1, &self.tags);
    }
}

/// Decide whether a previously timed-out scrape is still occupying a blocking thread.
///
/// Returns `true` when the earlier scrape has not finished, in which case this cycle must
/// be skipped: tokio cannot abort a blocking task, so starting another one would leak a
/// thread from the pool shared with the exporters every interval. A finished task is
/// reaped (surfacing a panic) and cleared, allowing scraping to resume.
async fn scrape_still_running(outstanding: &mut Option<JoinHandle<Vec<CollectedMetric>>>) -> bool {
    let Some(handle) = outstanding else {
        return false;
    };

    if !handle.is_finished() {
        return true;
    }

    if let Err(e) = handle.await {
        warn!("Previously timed-out scrape task failed: {}", e);
    }
    *outstanding = None;
    false
}

/// Main scrape loop
async fn run_scrape_loop(
    config: NodeMetricsConfig,
    metrics_output: Option<OTLPOutput<payload::Message<ResourceMetrics>>>,
    cancel: CancellationToken,
) -> Result<(), BoxError> {
    let Some(output) = metrics_output else {
        // Returning here would complete this receiver task, which the agent treats as an
        // unexpected early exit and responds to by shutting everything down. Idle until
        // cancellation instead.
        warn!(
            "No metrics output configured; node metrics receiver will not scrape. \
             Configure an exporter that accepts metrics."
        );
        cancel.cancelled().await;
        return Ok(());
    };

    let receiver_metrics = ReceiverMetrics::new();
    let config = Arc::new(config);

    // Constructing the collector reads /proc/stat, so keep it off the async worker too.
    let collector = {
        let config = Arc::clone(&config);
        Arc::new(
            tokio::task::spawn_blocking(move || {
                SystemCollector::new(&config.procfs_path, &config.sysfs_path)
            })
            .await
            .map_err(|e| format!("failed to initialize node metrics collector: {}", e))?,
        )
    };

    debug!(
        "Node metrics scrape loop started, interval: {:?}, boot_time: {}",
        config.scrape_interval,
        collector.boot_time()
    );

    // Use interval_at to start immediately, then at regular intervals.
    // Skip (not Delay or Burst) keeps ticks aligned to the original schedule after a slow
    // scrape, instead of shifting the whole series forward or firing catch-up ticks.
    let start = tokio::time::Instant::now();
    let mut interval = tokio::time::interval_at(start, config.scrape_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // A scrape that timed out keeps running: tokio cannot abort a blocking task once it
    // has started. Hold on to it so the loop does not start a fresh scrape (and leak
    // another blocking thread) every interval while the previous one is wedged — that
    // would eventually exhaust the blocking pool shared with the exporters.
    let mut outstanding: Option<JoinHandle<Vec<CollectedMetric>>> = None;

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                info!("Node metrics receiver cancelled");
                break;
            }

            _ = interval.tick() => {}
        }

        if scrape_still_running(&mut outstanding).await {
            warn!(
                "Previous scrape is still running or queued, skipping this cycle. \
                 If this persists, a filesystem or sysfs read is likely unresponsive; \
                 a filesystem can be excluded with \
                 --node-metrics-receiver-filesystem-mount-exclude."
            );
            receiver_metrics.add_scrape_failure();
            continue;
        }

        debug!("Scraping node metrics");

        // Collect metrics on a blocking thread to avoid stalling the async runtime
        // (statfs and /proc reads can block). The timeout bounds the loop, not the
        // blocking task itself, and cancellation is honoured while waiting.
        // Never shorter than MIN_SCRAPE_TIMEOUT: with the 1s interval the CLI permits, a
        // scrape that legitimately takes longer would time out on every cycle and the
        // receiver would emit nothing at all rather than merely lagging.
        let scrape_timeout = config.scrape_interval.max(MIN_SCRAPE_TIMEOUT);
        let mut handle = {
            let collector = Arc::clone(&collector);
            let config = Arc::clone(&config);
            tokio::task::spawn_blocking(move || collector.collect(&config))
        };

        let collected = tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                info!("Node metrics receiver cancelled during scrape");
                break;
            }

            res = tokio::time::timeout(scrape_timeout, &mut handle) => match res {
                Ok(Ok(collected)) => collected,
                Ok(Err(e)) => {
                    // Do not return Err: the agent propagates a receiver error *before*
                    // cancelling receivers and flushing exporters, so one panicking
                    // collector would discard unrelated pipelines' buffered telemetry.
                    // Treat it like the abandoned-task case and keep scraping.
                    warn!("Node metrics scrape task failed: {}", e);
                    receiver_metrics.add_scrape_failure();
                    continue;
                }
                Err(_) => {
                    warn!("Scrape timed out after {:?}, skipping this cycle", scrape_timeout);
                    receiver_metrics.add_scrape_failure();
                    // Keep the handle so the next tick can tell whether the blocking task
                    // is still wedged, instead of spawning another one on top of it.
                    outstanding = Some(handle);
                    continue;
                }
            }
        };

        let metric_count = collected.len();

        if metric_count == 0 {
            // Not debug-only: an empty scrape every interval means a wrong procfs/sysfs
            // path or an over-broad exclude filter, which is otherwise invisible.
            // Counted as well as logged: a permanently empty scrape is otherwise
            // indistinguishable from a receiver that was never enabled.
            warn!(
                "No metrics collected; check the configured procfs/sysfs paths and metric filters"
            );
            receiver_metrics.add_empty_scrape();
            continue;
        }

        // Convert to OTLP format. Counters carry the *latched* boot time, so that a wall
        // clock step does not move their start time and read as a counter reset.
        let resource_metrics = convert_to_otlp_metrics(
            collected,
            collector.counter_start_time(),
            &config.service_name,
        );

        // Count the points actually exported, not the ones collected: the conversion
        // drops duplicate label sets.
        let point_count = count_data_points(&resource_metrics);

        // Send to an output channel
        let message = payload::Message::new(None, vec![resource_metrics], None);

        // Hold the send future so cancellation can give the in-flight send a bounded
        // window to finish rather than discarding the batch outright.
        let mut send_fut = std::pin::pin!(output.send_async(message));

        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                // The pipeline is still draining at this point, so let the send finish if
                // it can. The window stays well inside the agent's receiver drain budget.
                match tokio::time::timeout(SHUTDOWN_SEND_TIMEOUT, send_fut.as_mut()).await {
                    Ok(Ok(())) => {
                        debug!("Sent {} metric points during shutdown", point_count);
                        receiver_metrics.add_accepted(point_count as u64);
                    }
                    Ok(Err(e)) => {
                        info!(
                            "Metrics pipeline already closed during shutdown, dropping {} \
                             metric points: {}",
                            point_count, e
                        );
                        receiver_metrics.add_refused(point_count as u64);
                    }
                    Err(_) => {
                        info!(
                            "Metrics pipeline did not accept {} metric points within {:?} of \
                             shutdown, dropping them",
                            point_count, SHUTDOWN_SEND_TIMEOUT
                        );
                        receiver_metrics.add_refused(point_count as u64);
                    }
                }
                break;
            }

            // Bound the send: a full channel (slow or retrying exporter) would otherwise
            // park here indefinitely, silently stopping all scraping.
            res = tokio::time::timeout(
                SEND_TIMEOUT.min(config.scrape_interval),
                send_fut.as_mut(),
            ) => match res {
                Ok(Ok(())) => {
                    debug!("Sent {} metric points to pipeline", point_count);
                    receiver_metrics.add_accepted(point_count as u64);
                }
                Ok(Err(e)) => {
                    // The only send error is a disconnected pipeline. During shutdown that
                    // is expected, so do not report it as a receiver failure; otherwise
                    // retrying forever would hide it from the agent.
                    receiver_metrics.add_refused(point_count as u64);
                    if cancel.is_cancelled() {
                        info!("Metrics pipeline closed during shutdown: {}", e);
                        break;
                    }
                    error!("Failed to send metrics to pipeline: {}", e);
                    return Err(format!("node metrics pipeline disconnected: {}", e).into());
                }
                Err(_) => {
                    // Drop this batch rather than stalling: the next scrape re-reads the
                    // current values, so a dropped batch is recoverable.
                    warn!(
                        "Metrics pipeline did not accept {} metric points within {:?}, \
                         abandoning this batch (it may still be delivered). The metrics \
                         exporter is likely backed up.",
                        point_count,
                        SEND_TIMEOUT.min(config.scrape_interval)
                    );
                    receiver_metrics.add_refused(point_count as u64);
                }
            }
        }
    }

    info!("Node metrics receiver stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel::bounded;
    use std::time::Duration;
    use tracing_test::traced_test;

    use crate::receivers::node_metrics::config::Collector;

    /// Count captured log lines containing `needle`.
    ///
    /// `logs_assert` is the only accessor `#[traced_test]` provides, so the count is
    /// captured out of the closure.
    macro_rules! logs_with {
        ($needle:expr) => {{
            let count = std::cell::Cell::new(0usize);
            logs_assert(|lines: &[&str]| {
                count.set(lines.iter().filter(|line| line.contains($needle)).count());
                Ok(())
            });
            count.get()
        }};
    }

    /// Drive the loop until it has logged the start of a scrape, then give it a short grace
    /// period to reach the send. Waiting on the log rather than on a fixed duration keeps
    /// the test deterministic on a loaded machine, where scheduling a blocking task can
    /// take far longer than a hard-coded sleep allows.
    macro_rules! drive_until_scraping {
        ($loop_fut:expr) => {{
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while !logs_contain("Scraping node metrics") {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the scrape loop never started a scrape"
                );
                tokio::select! {
                    res = $loop_fut.as_mut() => panic!("scrape loop exited early: {:?}", res),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
            // The scrape itself only reads the clock and calls uname, so this is ample.
            tokio::select! {
                res = $loop_fut.as_mut() => panic!("scrape loop exited early: {:?}", res),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }};
    }

    /// A config that scrapes only cheap, always-available collectors
    fn test_config() -> NodeMetricsConfig {
        NodeMetricsConfig {
            scrape_interval: Duration::from_secs(1),
            collectors: vec![Collector::Uname, Collector::Time],
            ..Default::default()
        }
    }

    /// [`test_config`] with a different scrape interval
    ///
    /// The interval also bounds the send (`SEND_TIMEOUT.min(scrape_interval)`), so it is
    /// what makes the backpressure tests finish quickly.
    fn test_config_with_interval(scrape_interval: Duration) -> NodeMetricsConfig {
        NodeMetricsConfig {
            scrape_interval,
            ..test_config()
        }
    }

    /// A placeholder batch, used to occupy channel capacity
    fn placeholder_message() -> payload::Message<ResourceMetrics> {
        payload::Message::new(None, vec![], None)
    }

    #[tokio::test]
    async fn test_scrape_loop_cancellation() {
        let (tx, _rx) = bounded::<payload::Message<ResourceMetrics>>(10);
        let output = OTLPOutput::new(tx);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move {
            run_scrape_loop(test_config(), Some(output), cancel_clone).await
        });

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(100)).await;

        cancel.cancel();

        // Unwrap both layers: the join result and the loop's own Result, so a panicking
        // or failing loop cannot pass this test.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("scrape loop did not stop after cancellation")
            .expect("scrape loop task panicked")
            .expect("scrape loop returned an error");
    }

    /// With no metrics pipeline, the loop must idle until cancelled rather than returning.
    /// Returning early is what the agent interprets as an unexpected receiver exit, which
    /// makes it shut the whole process down.
    #[tokio::test]
    async fn test_no_metrics_output_idles_until_cancelled() {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let mut handle =
            tokio::spawn(async move { run_scrape_loop(test_config(), None, cancel_clone).await });

        // It must still be running after a moment
        tokio::select! {
            res = &mut handle => panic!("scrape loop exited before cancellation: {:?}", res),
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }

        cancel.cancel();

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("scrape loop did not stop after cancellation")
            .expect("scrape loop task panicked")
            .expect("scrape loop returned an error");
    }

    /// A disconnected pipeline must surface as an error instead of being retried forever.
    #[tokio::test]
    async fn test_disconnected_pipeline_returns_error() {
        let (tx, rx) = bounded::<payload::Message<ResourceMetrics>>(1);
        let output = OTLPOutput::new(tx);
        // Drop the receiving end so the first send fails
        drop(rx);

        let cancel = CancellationToken::new();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            run_scrape_loop(test_config(), Some(output), cancel),
        )
        .await
        .expect("scrape loop did not return after the pipeline disconnected");

        let err = result.expect_err("expected an error when the pipeline is disconnected");
        assert!(
            err.to_string().contains("pipeline disconnected"),
            "expected a disconnect error, got: {}",
            err
        );
    }

    /// The latch that stops a wedged scrape from leaking a blocking thread every
    /// interval. Driving it directly avoids needing an unresponsive filesystem.
    #[tokio::test]
    async fn test_wedged_scrape_skips_cycles_until_it_finishes() {
        // Nothing outstanding: scraping proceeds.
        let mut outstanding: Option<JoinHandle<Vec<CollectedMetric>>> = None;
        assert!(!scrape_still_running(&mut outstanding).await);

        // A task that never finishes stands in for a hung procfs read.
        let (release, wedged) = tokio::sync::oneshot::channel::<()>();
        let mut outstanding = Some(tokio::spawn(async move {
            let _ = wedged.await;
            Vec::new()
        }));

        // Every cycle while it runs must be skipped, and the handle retained.
        for _ in 0..3 {
            assert!(
                scrape_still_running(&mut outstanding).await,
                "a running scrape must cause the cycle to be skipped"
            );
            assert!(outstanding.is_some(), "the handle must be retained");
        }

        // Once it completes, the latch clears and scraping resumes.
        release.send(()).unwrap();
        let mut cleared = false;
        for _ in 0..100 {
            if !scrape_still_running(&mut outstanding).await {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(cleared, "the latch never cleared after the scrape finished");
        assert!(outstanding.is_none(), "the finished handle must be reaped");
    }

    /// A panic in an abandoned scrape must be surfaced, not discarded with the handle.
    #[traced_test]
    #[tokio::test]
    async fn test_wedged_scrape_panic_is_reaped() {
        let handle = tokio::spawn(async { panic!("boom") });
        // Let it finish so the latch takes the reaping path
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while !handle.is_finished() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        let mut outstanding = Some(handle);
        assert!(!scrape_still_running(&mut outstanding).await);
        assert!(outstanding.is_none());

        // Reaping is the only place the panic can be observed: the handle is dropped
        // immediately afterwards, so a silent reap would lose it entirely.
        assert!(
            logs_contain("Previously timed-out scrape task failed"),
            "the reaped panic must be logged"
        );
    }

    /// A full pipeline must cost one batch, not the scrape loop.
    ///
    /// The send is bounded so that a backed-up exporter cannot park the loop forever:
    /// the batch is dropped and the next scrape re-reads the current values.
    #[traced_test]
    #[tokio::test]
    async fn test_full_pipeline_abandons_the_batch_and_keeps_scraping() {
        // Capacity 1, pre-filled and never drained, so every send the loop attempts
        // parks. Holding `_rx` keeps the channel connected: a dropped receiver would
        // take the disconnect path instead.
        let (tx, mut rx) = bounded::<payload::Message<ResourceMetrics>>(1);
        tx.send(placeholder_message()).await.unwrap();
        let output = OTLPOutput::new(tx.clone());

        // The send is bounded by min(SEND_TIMEOUT, scrape_interval), so the interval is
        // also how long the first batch waits before being abandoned.
        let interval = Duration::from_millis(300);
        let cancel = CancellationToken::new();
        let mut loop_fut = std::pin::pin!(run_scrape_loop(
            test_config_with_interval(interval),
            Some(output),
            cancel.clone()
        ));

        // Drive until two batches have been abandoned, rather than timing a fixed window:
        // the assertion below is about the count, so wait for the count.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let abandoned = logs_with!("abandoning this batch");
            if abandoned >= 2 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "only {} batches were abandoned before the deadline",
                abandoned
            );
            tokio::select! {
                res = loop_fut.as_mut() => {
                    panic!("scrape loop exited while the pipeline was full: {:?}", res)
                }
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }

        // Warned, not silently dropped: this is the only signal that the exporter is
        // backed up badly enough to be losing data.
        assert!(
            logs_contain("abandoning this batch"),
            "an abandoned batch must be reported"
        );

        // More than one batch was abandoned, so the loop went on scraping instead of
        // parking on the full channel for good after the first.
        logs_assert(|lines: &[&str]| {
            let abandoned = lines
                .iter()
                .filter(|line| line.contains("abandoning this batch"))
                .count();
            if abandoned >= 2 {
                Ok(())
            } else {
                Err(format!(
                    "expected at least 2 abandoned batches, saw {}",
                    abandoned
                ))
            }
        });

        // The batch really was dropped rather than queued behind the placeholder.
        // Draining shows the abandoned batches were never queued: only the placeholder is
        // there. (Asserting `tx.len() == 1` would be vacuous — capacity is 1.)
        let queued = rx.next().await.expect("channel closed");
        assert!(
            queued.payload.is_empty(),
            "only the placeholder should have been queued, got {} payload(s)",
            queued.payload.len()
        );

        // The loop is still alive and still returns cleanly.
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), loop_fut)
            .await
            .expect("scrape loop did not stop after cancellation")
            .expect("scrape loop returned an error");
    }

    /// Cancellation gives an in-flight send a bounded window to complete.
    ///
    /// The pipeline is still draining when receivers are cancelled, so a batch that can
    /// still be handed off should be, rather than discarded outright.
    #[traced_test]
    #[tokio::test]
    async fn test_shutdown_grace_window_lets_an_inflight_send_finish() {
        // Pre-fill the single slot so the loop's first send parks, and use an interval
        // long enough that it is certainly still parked when cancellation arrives.
        let (tx, mut rx) = bounded::<payload::Message<ResourceMetrics>>(1);
        tx.send(placeholder_message()).await.unwrap();
        let output = OTLPOutput::new(tx.clone());

        let cancel = CancellationToken::new();
        let mut loop_fut = std::pin::pin!(run_scrape_loop(
            test_config_with_interval(Duration::from_secs(30)),
            Some(output),
            cancel.clone()
        ));

        // Let the first scrape finish and park in the send.
        drive_until_scraping!(loop_fut);

        cancel.cancel();

        // Freeing the slot lets the parked send complete inside SHUTDOWN_SEND_TIMEOUT.
        let placeholder = rx.next().await.expect("channel closed");
        assert!(
            placeholder.payload.is_empty(),
            "expected the pre-filled placeholder to be drained first"
        );

        // The loop must return well inside the agent's 3 second receiver drain budget.
        let started = tokio::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(3), loop_fut)
            .await
            .expect("scrape loop did not stop within the receiver drain budget")
            .expect("scrape loop returned an error");
        let elapsed = started.elapsed();

        // The batch was handed off during the grace window, not dropped.
        assert!(
            logs_contain("metric points during shutdown"),
            "the final batch should have been sent during shutdown"
        );
        let batch = tokio::time::timeout(Duration::from_secs(1), rx.next())
            .await
            .expect("timed out waiting for the final batch")
            .expect("channel closed without the final batch");
        assert_eq!(batch.payload.len(), 1);
        assert!(
            !batch.payload[0].scope_metrics[0].metrics.is_empty(),
            "the final batch must carry the scraped metrics"
        );

        // Completing early is the point: the window is a bound, not a delay.
        assert!(
            elapsed < SHUTDOWN_SEND_TIMEOUT,
            "shutdown took {:?}, which means the send did not complete in the window",
            elapsed
        );
    }

    /// A scrape whose metrics are all filtered out must not send an empty batch.
    #[traced_test]
    #[tokio::test]
    async fn test_fully_filtered_scrape_sends_nothing() {
        let config = NodeMetricsConfig {
            scrape_interval: Duration::from_secs(1),
            collectors: vec![Collector::Uname, Collector::Time],
            ..Default::default()
        }
        .with_exclude_filter(".*")
        .expect("valid regex");

        let (tx, mut rx) = bounded::<payload::Message<ResourceMetrics>>(10);
        let output = OTLPOutput::new(tx);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let handle =
            tokio::spawn(async move { run_scrape_loop(config, Some(output), cancel_clone).await });

        // Nothing should arrive on the channel
        let got = tokio::time::timeout(Duration::from_millis(500), rx.next()).await;
        assert!(
            got.is_err(),
            "expected no message for a fully filtered scrape"
        );

        // ...and the absence of a message must be because the loop filtered everything
        // out, not because it died on its first iteration.
        let mut handle = handle;
        tokio::select! {
            res = &mut handle => panic!("scrape loop exited unexpectedly: {:?}", res),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }

        // An empty scrape means a wrong procfs/sysfs path or an over-broad filter, and
        // is otherwise invisible: nothing arrives on the channel either way.
        assert!(
            logs_contain("No metrics collected"),
            "an empty scrape must be reported"
        );

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("scrape loop did not stop after cancellation")
            .expect("scrape loop task panicked")
            .expect("scrape loop returned an error");
    }

    /// Integration test: run a full scrape→convert→send cycle and verify
    /// that ResourceMetrics actually arrive on the output channel.
    /// Uses only the Uname collector which works on all unix platforms
    /// (libc::uname + SystemTime::now).
    #[tokio::test]
    async fn test_scrape_produces_metrics_on_channel() {
        let config = NodeMetricsConfig {
            scrape_interval: Duration::from_secs(1),
            collectors: vec![Collector::Uname, Collector::Time],
            ..Default::default()
        };

        let (tx, mut rx) = bounded::<payload::Message<ResourceMetrics>>(10);
        let output = OTLPOutput::new(tx);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let handle =
            tokio::spawn(async move { run_scrape_loop(config, Some(output), cancel_clone).await });

        // Wait for the first scrape to produce a message
        let msg = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("timed out waiting for metrics")
            .expect("channel closed without receiving metrics");

        cancel.cancel();

        // Unwrap both layers, like the sibling tests: a loop that produced one good
        // batch and then failed must not pass.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("scrape loop did not stop after cancellation")
            .expect("scrape loop task panicked")
            .expect("scrape loop returned an error");

        // Verify the message contains ResourceMetrics with actual data
        assert_eq!(msg.payload.len(), 1);
        let resource_metrics = &msg.payload[0];

        // Should have a resource with service.name
        let resource = resource_metrics
            .resource
            .as_ref()
            .expect("missing resource");
        assert!(
            resource
                .attributes
                .iter()
                .any(|kv| kv.key == "service.name")
        );

        // Should have node_uname_info (from Uname) and node_time_seconds (from Time)
        let scope_metrics = &resource_metrics.scope_metrics;
        assert_eq!(scope_metrics.len(), 1);
        let metric_names: Vec<&str> = scope_metrics[0]
            .metrics
            .iter()
            .map(|m| m.name.as_str())
            .collect();
        for expected in ["node_time_seconds", "node_uname_info"] {
            assert!(
                metric_names.contains(&expected),
                "expected {} in {:?}",
                expected,
                metric_names
            );
        }

        // host.name must be on the resource so multi-host series stay distinguishable
        assert!(resource.attributes.iter().any(|kv| kv.key == "host.name"));
    }
}
