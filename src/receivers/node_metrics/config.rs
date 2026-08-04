// SPDX-License-Identifier: Apache-2.0

//! Configuration for the node metrics receiver

use std::time::Duration;

/// Default procfs mount point
pub const DEFAULT_PROCFS_PATH: &str = "/proc";
/// Default sysfs mount point
pub const DEFAULT_SYSFS_PATH: &str = "/sys";

/// Available metric collectors
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Collector {
    /// CPU metrics (seconds per mode: user, system, idle, iowait, etc.)
    Cpu,
    /// Load average metrics (node_load1, node_load5, node_load15)
    Loadavg,
    /// Memory metrics (all fields from /proc/meminfo)
    Memory,
    /// Network interface metrics (bytes/packets rx/tx per interface)
    Network,
    /// Filesystem metrics (size/free/avail space per mount)
    Filesystem,
    /// System information (uname: kernel, hostname)
    Uname,
    /// Kernel stat counters (forks, context switches, interrupts, procs running/blocked) from /proc/stat
    Stat,
    /// Process metrics (kernel limits: threads-max, pid_max) from /proc/sys/kernel/
    Processes,
    /// Disk I/O statistics from /proc/diskstats
    Diskstats,
    /// Virtual memory statistics from /proc/vmstat
    Vmstat,
    /// Network statistics from /proc/net/netstat and /proc/net/snmp
    Netstat,
    /// Socket statistics from /proc/net/sockstat and /proc/net/sockstat6
    Sockstat,
    /// File descriptor statistics from /proc/sys/fs/file-nr
    Filefd,
    /// CPU frequency from /sys/devices/system/cpu/cpu*/cpufreq/
    Cpufreq,
    /// Thermal zone temperatures from /sys/class/thermal/thermal_zone*/
    ThermalZone,
    /// NVMe device info from /sys/class/nvme/
    Nvme,
    /// Hardware monitoring sensors from /sys/class/hwmon/
    Hwmon,
    /// Custom metrics from Prometheus-format text files
    Textfile,
    /// System time (node_time_seconds, node_boot_time_seconds)
    Time,
}

/// Configuration for the node metrics receiver
///
/// This struct cannot derive `Deserialize` because `regex::Regex` does not implement it.
/// Supporting file-based configuration (YAML/TOML) would mean adding a raw config struct
/// with `String` filter fields that validates into this one.
#[derive(Clone, Debug)]
pub struct NodeMetricsConfig {
    /// Interval between metric collections
    pub scrape_interval: Duration,
    /// Which collectors to enable
    pub collectors: Vec<Collector>,
    /// Optional regex pattern to include only matching metric names
    pub metric_include_filter: Option<regex::Regex>,
    /// Optional regex pattern to exclude matching metric names
    pub metric_exclude_filter: Option<regex::Regex>,
    /// Optional regex pattern to exclude matching filesystem mount points,
    /// in addition to the built-in virtual and network filesystem exclusions
    pub filesystem_mount_exclude: Option<regex::Regex>,
    /// Path to procfs mount (default: /proc)
    pub procfs_path: String,
    /// Path to sysfs mount (default: /sys)
    pub sysfs_path: String,
    /// Prefix under which the host's root filesystem is reachable, or "/" when the
    /// collector runs directly on the host.
    ///
    /// Needed by the filesystem collector: the mount *table* can be read from a
    /// bind-mounted host procfs, but `statfs` still has to be called on a path that
    /// resolves in this process's own mount namespace.
    pub rootfs_path: String,
    /// Directory containing Prometheus-format textfiles (*.prom), or a single .prom file
    pub textfile_directory: Option<String>,
    /// Service name used in the OTLP Resource (default: "node_metrics")
    pub service_name: String,
}

impl Default for NodeMetricsConfig {
    fn default() -> Self {
        Self {
            scrape_interval: Duration::from_secs(60),
            collectors: vec![
                Collector::Cpu,
                Collector::Loadavg,
                Collector::Memory,
                Collector::Network,
                Collector::Filesystem,
                Collector::Uname,
                Collector::Stat,
                Collector::Processes,
                Collector::Diskstats,
                Collector::Vmstat,
                Collector::Netstat,
                Collector::Sockstat,
                Collector::Filefd,
                Collector::Cpufreq,
                Collector::ThermalZone,
                Collector::Nvme,
                Collector::Hwmon,
                Collector::Time,
                // Textfile not included by default - requires a directory to be set
            ],
            metric_include_filter: None,
            metric_exclude_filter: None,
            filesystem_mount_exclude: None,
            procfs_path: DEFAULT_PROCFS_PATH.to_string(),
            sysfs_path: DEFAULT_SYSFS_PATH.to_string(),
            rootfs_path: "/".to_string(),
            textfile_directory: None,
            service_name: "node_metrics".to_string(),
        }
    }
}

impl NodeMetricsConfig {
    /// Create a new config with the specified scrape interval in seconds
    pub fn new(scrape_interval_secs: u64) -> Self {
        Self {
            scrape_interval: Duration::from_secs(scrape_interval_secs),
            ..Default::default()
        }
    }

    /// Set which collectors to enable
    pub fn with_collectors(mut self, collectors: Vec<Collector>) -> Self {
        self.collectors = collectors;
        self
    }

    /// Set the metric include filter regex pattern
    pub fn with_include_filter(mut self, pattern: &str) -> Result<Self, regex::Error> {
        self.metric_include_filter = Some(regex::Regex::new(pattern)?);
        Ok(self)
    }

    /// Set the metric exclude filter regex pattern
    pub fn with_exclude_filter(mut self, pattern: &str) -> Result<Self, regex::Error> {
        self.metric_exclude_filter = Some(regex::Regex::new(pattern)?);
        Ok(self)
    }

    /// Set the filesystem mount point exclude regex pattern
    pub fn with_filesystem_mount_exclude(mut self, pattern: &str) -> Result<Self, regex::Error> {
        self.filesystem_mount_exclude = Some(regex::Regex::new(pattern)?);
        Ok(self)
    }

    /// Check if a metric name passes the configured filters
    pub fn should_include_metric(&self, name: &str) -> bool {
        // If include filter is set, metric must match it
        if let Some(ref include) = self.metric_include_filter
            && !include.is_match(name)
        {
            return false;
        }

        // If exclude filter is set, metric must not match it
        if let Some(ref exclude) = self.metric_exclude_filter
            && exclude.is_match(name)
        {
            return false;
        }

        true
    }

    /// Validate the configuration and normalize collector list (deduplicate)
    pub fn normalize_and_validate(&mut self) -> Result<(), String> {
        if self.scrape_interval < Duration::from_secs(1) {
            return Err(format!(
                "Scrape interval must be at least 1 second, got {:?}",
                self.scrape_interval
            ));
        }

        if self.collectors.is_empty() {
            return Err("At least one collector must be enabled".to_string());
        }

        // Deduplicate collectors while preserving order
        let mut seen = std::collections::HashSet::new();
        self.collectors.retain(|c| seen.insert(c.clone()));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The collector set enabled by default (textfile requires an explicit directory)
    const DEFAULT_COLLECTORS: [Collector; 18] = [
        Collector::Cpu,
        Collector::Loadavg,
        Collector::Memory,
        Collector::Network,
        Collector::Filesystem,
        Collector::Uname,
        Collector::Stat,
        Collector::Processes,
        Collector::Diskstats,
        Collector::Vmstat,
        Collector::Netstat,
        Collector::Sockstat,
        Collector::Filefd,
        Collector::Cpufreq,
        Collector::ThermalZone,
        Collector::Nvme,
        Collector::Hwmon,
        Collector::Time,
    ];

    #[test]
    fn test_default_config() {
        let config = NodeMetricsConfig::default();
        assert_eq!(config.scrape_interval, Duration::from_secs(60));
        // Compare the actual set, not just its size: a count assertion would still pass
        // if one collector were swapped for another.
        assert_eq!(config.collectors, DEFAULT_COLLECTORS.to_vec());
        assert!(config.metric_include_filter.is_none());
        assert!(config.metric_exclude_filter.is_none());
        assert!(!config.collectors.contains(&Collector::Textfile));
    }

    #[test]
    fn test_normalize_deduplicates_collectors() {
        let mut config = NodeMetricsConfig::default().with_collectors(vec![
            Collector::Cpu,
            Collector::Memory,
            Collector::Cpu,
        ]);
        config.normalize_and_validate().unwrap();
        assert_eq!(config.collectors, vec![Collector::Cpu, Collector::Memory]);
    }

    #[test]
    fn test_validate_rejects_sub_second_interval_and_reports_it() {
        let mut config = NodeMetricsConfig {
            scrape_interval: Duration::from_millis(500),
            ..Default::default()
        };
        let err = config.normalize_and_validate().unwrap_err();
        // The rejected value must be reported accurately, not truncated to "0s"
        assert!(
            err.contains("500ms"),
            "error should report the offending duration, got: {}",
            err
        );
    }

    #[test]
    fn test_validate_rejects_empty_collector_list() {
        let mut config = NodeMetricsConfig::default().with_collectors(vec![]);
        assert!(config.normalize_and_validate().is_err());
    }

    #[test]
    fn test_exclude_filter_drops_matching_metrics() {
        let config = NodeMetricsConfig::default()
            .with_exclude_filter("^node_vmstat_")
            .unwrap();
        assert!(!config.should_include_metric("node_vmstat_pgfault"));
        assert!(config.should_include_metric("node_load1"));
    }

    #[test]
    fn test_include_filter_takes_precedence_over_unmatched() {
        let config = NodeMetricsConfig::default()
            .with_include_filter("^node_load")
            .unwrap();
        assert!(config.should_include_metric("node_load1"));
        assert!(!config.should_include_metric("node_cpu_seconds_total"));
    }

    #[test]
    fn test_config_with_collectors() {
        let config =
            NodeMetricsConfig::new(30).with_collectors(vec![Collector::Loadavg, Collector::Memory]);
        assert_eq!(config.scrape_interval, Duration::from_secs(30));
        assert_eq!(
            config.collectors,
            vec![Collector::Loadavg, Collector::Memory]
        );
    }

    #[test]
    fn test_combined_filters() {
        let config = NodeMetricsConfig::new(60)
            .with_include_filter("node_.*")
            .unwrap()
            .with_exclude_filter(".*packets.*")
            .unwrap();

        assert!(config.should_include_metric("node_load1"));
        assert!(config.should_include_metric("node_network_receive_bytes_total"));
        assert!(!config.should_include_metric("node_network_receive_packets_total"));
        assert!(!config.should_include_metric("other_metric"));
    }

    #[test]
    fn test_filesystem_mount_exclude() {
        let config = NodeMetricsConfig::new(60)
            .with_filesystem_mount_exclude("^/nfs/")
            .unwrap();

        let re = config.filesystem_mount_exclude.as_ref().unwrap();
        assert!(re.is_match("/nfs/share"));
        assert!(!re.is_match("/mnt/data"));
    }
}
