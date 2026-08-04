// SPDX-License-Identifier: Apache-2.0

//! System metrics collector reading directly from /proc and /sys
//!
//! This collector reads Linux kernel interfaces, emitting raw counter values
//! compatible with Prometheus node_exporter conventions.

// Notice: portions of this collector are derived from Prometheus node_exporter
// (https://github.com/prometheus/node_exporter), licensed under the Apache License 2.0.
/* Copyright © The Prometheus Authors */
//
// Derived material, by submodule:
//   procfs.rs   metric names and HELP strings; the default filesystem-type and
//               mount-point exclusion lists
//   sysfs.rs    metric names and HELP strings; the hwmon chip-naming approach from
//               that project's `collector/hwmon.go`
//   textfile.rs the textfile collector's behaviour and its `node_textfile_*` semantics
//   util.rs     the virtual-device and partition exclusion heuristics
//
// Where behaviour deliberately differs from node_exporter, the relevant function says so.

mod procfs;
mod sysfs;
mod textfile;
mod util;

pub(crate) use util::matches_path_prefix;

#[cfg(test)]
mod test_support;

use crate::receivers::node_metrics::config::{Collector, NodeMetricsConfig};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
// Use portable-atomic on platforms without native 64-bit atomics (e.g. 32-bit ARM)
#[cfg(not(target_has_atomic = "64"))]
use portable_atomic::{AtomicU64, Ordering};
#[cfg(target_has_atomic = "64")]
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

/// Collected metric value with optional labels
///
/// Metric names, units and descriptions are allocated afresh on every scrape.
#[derive(Debug, Clone)]
pub struct CollectedMetric {
    pub name: String,
    pub value: f64,
    pub labels: Vec<(String, String)>,
    pub metric_type: MetricType,
    pub unit: Option<String>,
    pub description: Option<String>,
}

/// Type of metric (determines how it's represented in OTLP)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetricType {
    /// Gauge - current value that can go up or down
    Gauge,
    /// Counter - monotonically increasing value
    Counter,
}

/// System metrics collector reading from /proc and /sys
pub struct SystemCollector {
    /// Clock ticks per second (for CPU time conversion)
    clk_tck: f64,
    /// Memory page size in bytes (for sockstat mem-to-bytes conversion)
    page_size: f64,
    /// System boot time in seconds since epoch, or 0 while it is unknown.
    ///
    /// Re-read on every scrape that reads `/proc/stat`, because the kernel's `btime`
    /// moves whenever the wall clock is stepped — a device with no RTC boots near the
    /// epoch and then jumps when NTP syncs, and a value latched before that would be
    /// wrong forever.
    boot_time: AtomicU64,
    /// Boot time used as `start_time_unix_nano` for cumulative counters, or 0 while
    /// unknown.
    ///
    /// Unlike `boot_time` this is latched at the first non-zero reading: the counters
    /// themselves do not reset when the clock is stepped, and moving their start time
    /// would be reported downstream as a counter reset.
    ///
    /// One consequence: if the clock is later stepped *backwards* past this value, counters
    /// briefly carry a start time ahead of their own timestamp, which a strict backend may
    /// reject. It resolves itself on the next forward correction, and the alternative —
    /// re-latching — would signal a reset on every clock adjustment.
    counter_start_time: AtomicU64,
    /// Path to procfs mount point
    procfs_path: String,
    /// Path to sysfs mount point
    sysfs_path: String,
}

impl SystemCollector {
    /// Create a new system collector with configurable procfs and sysfs paths
    pub fn new(procfs_path: &str, sysfs_path: &str) -> Self {
        // Get clock ticks per second for CPU time conversion.
        // sysconf returns -1 on error; fall back to the POSIX-common default of 100.
        let raw_clk = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        let clk_tck = if raw_clk > 0 { raw_clk as f64 } else { 100.0 };
        // Get memory page size for sockstat mem-to-bytes conversion.
        // sysconf returns -1 on error; fall back to the common default of 4096.
        let raw_page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page_size = if raw_page > 0 {
            raw_page as f64
        } else {
            4096.0
        };

        // Get boot time from /proc/stat
        let boot_time = Self::read_boot_time(procfs_path).unwrap_or_else(|| {
            warn!(
                "Could not read boot time from {}/stat; \
                 counter start_time will default to epoch (0) until it can be read",
                procfs_path
            );
            0
        });

        Self {
            clk_tck,
            page_size,
            boot_time: AtomicU64::new(boot_time),
            counter_start_time: AtomicU64::new(boot_time),
            procfs_path: procfs_path.to_string(),
            sysfs_path: sysfs_path.to_string(),
        }
    }

    /// Read boot time from /proc/stat
    fn read_boot_time(procfs_path: &str) -> Option<u64> {
        let path = format!("{}/stat", procfs_path);
        let content = fs::read_to_string(path).ok()?;
        Self::parse_boot_time(&content)
    }

    /// Extract the `btime` field from the contents of /proc/stat
    fn parse_boot_time(stat_content: &str) -> Option<u64> {
        for line in stat_content.lines() {
            if let Some(rest) = line.strip_prefix("btime ") {
                // The kernel formats btime from a *signed* timespec64 as %llu, so a machine
                // whose wall clock is near the epoch prints a wrapped value (~1.8e19) after
                // a backwards clock step or a suspend/resume. Accepting it would publish a
                // nonsense boot time and, worse, latch it as every counter's start time.
                let btime: i64 = rest.trim().parse().ok()?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(i64::MAX);
                if btime <= 0 || btime > now {
                    warn!(
                        "Ignoring implausible boot time from /proc/stat: btime {}",
                        rest.trim()
                    );
                    return None;
                }
                return Some(btime as u64);
            }
        }
        None
    }

    /// Get the current boot time in seconds since epoch, or 0 if it is not known
    pub fn boot_time(&self) -> u64 {
        self.boot_time.load(Ordering::Relaxed)
    }

    /// Get the boot time to use as `start_time_unix_nano` for cumulative counters, or 0
    /// if it is not known.
    ///
    /// Latched at the first known value so that a wall-clock step does not look like a
    /// counter reset downstream.
    pub fn counter_start_time(&self) -> u64 {
        self.counter_start_time.load(Ordering::Relaxed)
    }

    /// Collect all enabled metrics based on config
    pub fn collect(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        // A default collector set on a many-core host produces well over a thousand
        // metrics (vmstat and netstat alone contribute several hundred).
        let mut metrics = Vec::with_capacity(2048);

        // Read /proc/stat once for the collectors that need it and for the boot time.
        // `Time` is included because `node_boot_time_seconds` comes from the `btime` line:
        // without it, a clock step would go unnoticed whenever cpu and stat are disabled.
        let stat_content = if self.counter_start_time() == 0
            || config
                .collectors
                .iter()
                .any(|c| matches!(c, Collector::Cpu | Collector::Stat | Collector::Time))
        {
            let path = format!("{}/stat", self.procfs_path);
            match fs::read_to_string(&path) {
                Ok(c) => Some(c),
                Err(e) => {
                    debug!("Failed to read {}: {}", path, e);
                    None
                }
            }
        } else {
            None
        };

        // Refresh the boot time every scrape: the kernel's `btime` moves whenever the wall
        // clock is stepped, and procfs may only have become readable after startup.
        if let Some(btime) = stat_content.as_deref().and_then(Self::parse_boot_time) {
            let previous = self.boot_time.swap(btime, Ordering::Relaxed);
            // btime tracks the timekeeper's wall-clock offset, so it also drifts by a
            // second under ordinary slewing, suspend/resume or VM migration. Only a real
            // step is worth an info line; the rest is debug.
            if previous.abs_diff(btime) > 1 {
                info!("System boot time is {} (previously {})", btime, previous);
            } else if previous != btime {
                debug!("System boot time adjusted to {} (was {})", btime, previous);
            }
            // Latch the counter start time at the first known value, so a clock step is
            // not reported downstream as a counter reset.
            let _ = self.counter_start_time.compare_exchange(
                0,
                btime,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }

        for collector in &config.collectors {
            let result = match collector {
                Collector::Cpu => self.collect_cpu(config, stat_content.as_deref()),
                Collector::Loadavg => self.collect_loadavg(config),
                Collector::Memory => self.collect_meminfo(config),
                Collector::Network => self.collect_netdev(config),
                Collector::Filesystem => self.collect_filesystem(config),
                Collector::Uname => self.collect_uname(config),
                Collector::Stat => self.collect_stat(config, stat_content.as_deref()),
                Collector::Processes => self.collect_processes(config),
                Collector::Diskstats => self.collect_diskstats(config),
                Collector::Vmstat => self.collect_vmstat(config),
                Collector::Netstat => self.collect_netstat(config),
                Collector::Sockstat => self.collect_sockstat(config),
                Collector::Filefd => self.collect_filefd(config),
                Collector::Cpufreq => self.collect_cpufreq(config),
                Collector::ThermalZone => self.collect_thermal_zone(config),
                Collector::Nvme => self.collect_nvme(config),
                Collector::Hwmon => self.collect_hwmon(config),
                Collector::Textfile => self.collect_textfile(config),
                Collector::Time => self.collect_time(config),
            };
            metrics.extend(result);
        }

        metrics
    }
}

impl Default for SystemCollector {
    fn default() -> Self {
        Self::new("/proc", "/sys")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::node_metrics::collector::test_support::*;

    // ---------------------------------------------------------------------
    // Construction
    // ---------------------------------------------------------------------

    #[test]
    fn test_boot_time_read_from_stat() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "stat", STAT_FIXTURE);

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        assert_eq!(collector.boot_time(), 1_700_000_000);

        // Without a readable /proc/stat the boot time stays unknown rather than
        // being invented; collect() retries the read on every scrape.
        let empty = tempfile::tempdir().unwrap();
        let without_stat = SystemCollector::new(empty.path().to_str().unwrap(), "/sys");
        assert_eq!(without_stat.boot_time(), 0);
    }

    #[test]
    fn test_boot_time_retried_on_collect_until_readable() {
        // A procfs that is not mounted yet when the receiver starts: without the
        // per-scrape retry every counter would keep epoch 0 as its start time for
        // the whole life of the process.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let collector = SystemCollector::new(&path, &path);
        assert_eq!(collector.boot_time(), 0);

        // While boot time is unknown the time collector suppresses the metric
        // rather than reporting a plausible-looking 1970 boot.
        let before = collector.collect(&config_for(vec![Collector::Time]));
        assert!(matching(&before, "node_boot_time_seconds", &[]).is_empty());
        find_one(&before, "node_time_seconds", &[]);

        // procfs appears between two scrapes.
        write(tmp.path(), "stat", STAT_FIXTURE);
        let after = collector.collect(&config_for(vec![Collector::Time]));

        assert_eq!(collector.boot_time(), 1_700_000_000);
        let boot = find_one(&after, "node_boot_time_seconds", &[]);
        assert_eq!(boot.value, 1_700_000_000.0);
        assert_eq!(boot.unit.as_deref(), Some("s"));
        assert_eq!(boot.metric_type, MetricType::Gauge);
        assert!(boot.labels.is_empty());
    }

    /// The kernel formats `btime` from a signed value as `%llu`, so a machine whose clock
    /// is near the epoch can print a wrapped value. Accepting it would publish a boot time
    /// ~580 billion years in the future and latch it into every counter's start time.
    #[test]
    fn test_implausible_boot_time_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        // 2^64 - 10, i.e. -10 seconds printed as unsigned
        write(
            tmp.path(),
            "stat",
            "btime 18446744073709551606\ncpu  1 2 3 4 5 6 7 8 9 10\n",
        );

        let collector = SystemCollector::new(&path, &path);
        assert_eq!(collector.boot_time(), 0, "a wrapped btime must be rejected");
        assert_eq!(collector.counter_start_time(), 0);

        // A boot time in the future is equally implausible
        write(tmp.path(), "stat", "btime 99999999999\n");
        let metrics = collector.collect(&config_for(vec![Collector::Time]));
        assert_eq!(collector.boot_time(), 0);
        assert!(matching(&metrics, "node_boot_time_seconds", &[]).is_empty());

        // ...and a plausible one is still accepted
        write(tmp.path(), "stat", STAT_FIXTURE);
        collector.collect(&config_for(vec![Collector::Time]));
        assert_eq!(collector.boot_time(), 1_700_000_000);
    }

    /// A wall-clock step moves the kernel's `btime`, so the reported boot time must track
    /// it — while the start time stamped on cumulative counters stays put, since moving it
    /// would be read downstream as a counter reset.
    #[test]
    fn test_boot_time_tracks_clock_steps_but_counter_start_time_is_latched() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        write(tmp.path(), "stat", STAT_FIXTURE);

        let collector = SystemCollector::new(&path, &path);
        assert_eq!(collector.boot_time(), 1_700_000_000);
        assert_eq!(collector.counter_start_time(), 1_700_000_000);

        // The clock is stepped forward, so the kernel reports a later boot time. This is
        // what an RTC-less device does when NTP first syncs.
        write(
            tmp.path(),
            "stat",
            &STAT_FIXTURE.replace("btime 1700000000", "btime 1700000500"),
        );
        let after = collector.collect(&config_for(vec![Collector::Time]));

        // The gauge follows the kernel...
        assert_eq!(collector.boot_time(), 1_700_000_500);
        assert_eq!(
            find_one(&after, "node_boot_time_seconds", &[]).value,
            1_700_000_500.0
        );
        // ...but the counter start time keeps its first known value.
        assert_eq!(collector.counter_start_time(), 1_700_000_000);
    }

    /// `/proc/stat` must be read when the `time` collector is enabled, even with cpu and
    /// stat disabled and a boot time already latched — otherwise a clock step goes unseen.
    #[test]
    fn test_time_collector_alone_still_refreshes_boot_time() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        write(tmp.path(), "stat", STAT_FIXTURE);

        let collector = SystemCollector::new(&path, &path);
        assert_eq!(collector.counter_start_time(), 1_700_000_000);

        write(
            tmp.path(),
            "stat",
            &STAT_FIXTURE.replace("btime 1700000000", "btime 1700009999"),
        );
        // Only Time — neither Cpu nor Stat forces the /proc/stat read here.
        collector.collect(&config_for(vec![Collector::Time]));

        assert_eq!(collector.boot_time(), 1_700_009_999);
    }

    // ---------------------------------------------------------------------
    // Top-level dispatch
    // ---------------------------------------------------------------------

    #[test]
    fn test_collect_with_filter() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "loadavg", "0.52 0.58 0.59 2/1234 5678\n");

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let config = NodeMetricsConfig::new(60)
            .with_include_filter("^node_load1$")
            .unwrap();

        let metrics = collector.collect_loadavg(&config);

        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "node_load1");
        assert_eq!(metrics[0].value, 0.52);
    }

    #[test]
    fn test_collect_dispatches_to_every_enabled_collector() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "stat", STAT_FIXTURE);

        let path = tmp.path().to_str().unwrap();
        let collector = SystemCollector::new(path, path);
        let metrics = collector.collect(&config_for(vec![Collector::Cpu, Collector::Stat]));

        // A single collect() call returns the metrics of both collectors, from one
        // read of /proc/stat.
        assert_eq!(
            find_one(
                &metrics,
                "node_cpu_seconds_total",
                &[("cpu", "0"), ("mode", "user")]
            )
            .value,
            1000.0 / collector.clk_tck
        );
        assert_eq!(find_one(&metrics, "node_forks_total", &[]).value, 4242.0);
    }

    #[test]
    fn test_collect_without_stat_file() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "loadavg", "1.5 1.0 0.5 1/100 200\n");

        let path = tmp.path().to_str().unwrap();
        let collector = SystemCollector::new(path, path);
        let metrics = collector.collect(&config_for(vec![Collector::Loadavg]));

        assert_eq!(metrics.len(), 3);
        assert_eq!(find_one(&metrics, "node_load1", &[]).value, 1.5);
        // The boot-time retry finds nothing to read and leaves it unknown.
        assert_eq!(collector.boot_time(), 0);
    }

    #[test]
    fn test_collect_empty_fixture_reports_only_syscall_backed_metrics() {
        let procfs = tempfile::tempdir().unwrap();
        let sysfs = tempfile::tempdir().unwrap();

        let mut collectors = NodeMetricsConfig::default().collectors;
        collectors.push(Collector::Textfile);

        let collector = SystemCollector::new(
            procfs.path().to_str().unwrap(),
            sysfs.path().to_str().unwrap(),
        );
        let metrics = collector.collect(&config_for(collectors));

        // Every file-backed collector finds nothing and reports nothing, without
        // panicking. Only uname and the wall clock, which read no files, remain;
        // node_boot_time_seconds is suppressed because btime is unknown.
        let mut names: Vec<&str> = metrics.iter().map(|m| m.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, ["node_time_seconds", "node_uname_info"]);
    }
}
