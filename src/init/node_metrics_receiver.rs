// SPDX-License-Identifier: Apache-2.0

//! CLI integration for the node metrics receiver

use crate::receivers::node_metrics::collector::matches_path_prefix;
use crate::receivers::node_metrics::config::{
    Collector, DEFAULT_PROCFS_PATH, DEFAULT_SYSFS_PATH, NodeMetricsConfig,
};
use clap::Args;
use serde::Deserialize;
use std::time::Duration;
use tower::BoxError;

#[derive(Debug, Args, Clone, Deserialize)]
#[serde(default)]
pub struct NodeMetricsReceiverArgs {
    /// Interval between scrapes, as a time duration (minimum 1s)
    #[arg(
        id("NODE_METRICS_RECEIVER_SCRAPE_INTERVAL"),
        long("node-metrics-receiver-scrape-interval"),
        env = "ROTEL_NODE_METRICS_RECEIVER_SCRAPE_INTERVAL",
        default_value = "60s",
        value_parser = humantime::parse_duration
    )]
    #[serde(with = "humantime_serde")]
    pub scrape_interval: Duration,

    /// Enable CPU metrics collection
    #[arg(
        id("NODE_METRICS_RECEIVER_CPU"),
        long("node-metrics-receiver-cpu"),
        env = "ROTEL_NODE_METRICS_RECEIVER_CPU",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub cpu: bool,

    /// Enable load average metrics collection
    #[arg(
        id("NODE_METRICS_RECEIVER_LOADAVG"),
        long("node-metrics-receiver-loadavg"),
        env = "ROTEL_NODE_METRICS_RECEIVER_LOADAVG",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub loadavg: bool,

    /// Enable memory metrics collection
    #[arg(
        id("NODE_METRICS_RECEIVER_MEMORY"),
        long("node-metrics-receiver-memory"),
        env = "ROTEL_NODE_METRICS_RECEIVER_MEMORY",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub memory: bool,

    /// Enable network metrics collection
    #[arg(
        id("NODE_METRICS_RECEIVER_NETWORK"),
        long("node-metrics-receiver-network"),
        env = "ROTEL_NODE_METRICS_RECEIVER_NETWORK",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub network: bool,

    /// Enable filesystem metrics collection
    #[arg(
        id("NODE_METRICS_RECEIVER_FILESYSTEM"),
        long("node-metrics-receiver-filesystem"),
        env = "ROTEL_NODE_METRICS_RECEIVER_FILESYSTEM",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub filesystem: bool,

    /// Enable system/uname info metrics collection (kernel info, hostname)
    #[arg(
        id("NODE_METRICS_RECEIVER_UNAME"),
        long("node-metrics-receiver-uname"),
        env = "ROTEL_NODE_METRICS_RECEIVER_UNAME",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub uname: bool,

    /// Enable kernel stat counters (forks, context switches, interrupts, procs running/blocked from /proc/stat)
    #[arg(
        id("NODE_METRICS_RECEIVER_STAT"),
        long("node-metrics-receiver-stat"),
        env = "ROTEL_NODE_METRICS_RECEIVER_STAT",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub stat: bool,

    /// Enable process metrics collection (kernel limits: threads-max, pid_max)
    #[arg(
        id("NODE_METRICS_RECEIVER_PROCESSES"),
        long("node-metrics-receiver-processes"),
        env = "ROTEL_NODE_METRICS_RECEIVER_PROCESSES",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub processes: bool,

    /// Enable disk I/O statistics collection (Linux only, reads /proc/diskstats)
    #[arg(
        id("NODE_METRICS_RECEIVER_DISKSTATS"),
        long("node-metrics-receiver-diskstats"),
        env = "ROTEL_NODE_METRICS_RECEIVER_DISKSTATS",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub diskstats: bool,

    /// Enable virtual memory statistics collection (/proc/vmstat)
    #[arg(
        id("NODE_METRICS_RECEIVER_VMSTAT"),
        long("node-metrics-receiver-vmstat"),
        env = "ROTEL_NODE_METRICS_RECEIVER_VMSTAT",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub vmstat: bool,

    /// Enable network statistics collection (/proc/net/netstat and /proc/net/snmp)
    #[arg(
        id("NODE_METRICS_RECEIVER_NETSTAT"),
        long("node-metrics-receiver-netstat"),
        env = "ROTEL_NODE_METRICS_RECEIVER_NETSTAT",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub netstat: bool,

    /// Enable socket statistics collection (/proc/net/sockstat and /proc/net/sockstat6)
    #[arg(
        id("NODE_METRICS_RECEIVER_SOCKSTAT"),
        long("node-metrics-receiver-sockstat"),
        env = "ROTEL_NODE_METRICS_RECEIVER_SOCKSTAT",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub sockstat: bool,

    /// Enable file descriptor statistics collection (/proc/sys/fs/file-nr)
    #[arg(
        id("NODE_METRICS_RECEIVER_FILEFD"),
        long("node-metrics-receiver-filefd"),
        env = "ROTEL_NODE_METRICS_RECEIVER_FILEFD",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub filefd: bool,

    /// Enable CPU frequency metrics collection (/sys/devices/system/cpu/cpu*/cpufreq/)
    #[arg(
        id("NODE_METRICS_RECEIVER_CPUFREQ"),
        long("node-metrics-receiver-cpufreq"),
        env = "ROTEL_NODE_METRICS_RECEIVER_CPUFREQ",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub cpufreq: bool,

    /// Enable thermal zone and cooling device metrics (/sys/class/thermal/)
    #[arg(
        id("NODE_METRICS_RECEIVER_THERMAL_ZONE"),
        long("node-metrics-receiver-thermal-zone"),
        env = "ROTEL_NODE_METRICS_RECEIVER_THERMAL_ZONE",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub thermal_zone: bool,

    /// Enable NVMe device info metrics (/sys/class/nvme/)
    #[arg(
        id("NODE_METRICS_RECEIVER_NVME"),
        long("node-metrics-receiver-nvme"),
        env = "ROTEL_NODE_METRICS_RECEIVER_NVME",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub nvme: bool,

    /// Enable hardware monitoring sensor metrics (/sys/class/hwmon/)
    #[arg(
        id("NODE_METRICS_RECEIVER_HWMON"),
        long("node-metrics-receiver-hwmon"),
        env = "ROTEL_NODE_METRICS_RECEIVER_HWMON",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub hwmon: bool,

    /// Enable time metrics (node_time_seconds, node_boot_time_seconds)
    #[arg(
        id("NODE_METRICS_RECEIVER_TIME"),
        long("node-metrics-receiver-time"),
        env = "ROTEL_NODE_METRICS_RECEIVER_TIME",
        action = clap::ArgAction::Set,
        default_value = "true"
    )]
    pub time: bool,

    /// Enable textfile collector for custom Prometheus-format metrics
    #[arg(
        id("NODE_METRICS_RECEIVER_TEXTFILE"),
        long("node-metrics-receiver-textfile"),
        env = "ROTEL_NODE_METRICS_RECEIVER_TEXTFILE",
        action = clap::ArgAction::Set,
        default_value = "false"
    )]
    pub textfile: bool,

    /// Directory or file path for Prometheus-format textfiles (*.prom)
    #[arg(
        id("NODE_METRICS_RECEIVER_TEXTFILE_DIRECTORY"),
        long("node-metrics-receiver-textfile-directory"),
        env = "ROTEL_NODE_METRICS_RECEIVER_TEXTFILE_DIRECTORY"
    )]
    pub textfile_directory: Option<String>,

    /// Root filesystem path prefix, auto-composed onto procfs/sysfs paths
    /// (e.g. --node-metrics-receiver-rootfs-path=/host sets procfs to /host/proc and sysfs to /host/sys)
    #[arg(
        id("NODE_METRICS_RECEIVER_ROOTFS_PATH"),
        long("node-metrics-receiver-rootfs-path"),
        env = "ROTEL_NODE_METRICS_RECEIVER_ROOTFS_PATH",
        default_value = "/"
    )]
    pub rootfs_path: String,

    /// Path to procfs mount point (for containerized monitoring)
    #[arg(
        id("NODE_METRICS_RECEIVER_PROCFS_PATH"),
        long("node-metrics-receiver-procfs-path"),
        env = "ROTEL_NODE_METRICS_RECEIVER_PROCFS_PATH",
        default_value = DEFAULT_PROCFS_PATH
    )]
    pub procfs_path: String,

    /// Path to sysfs mount point (for containerized monitoring)
    #[arg(
        id("NODE_METRICS_RECEIVER_SYSFS_PATH"),
        long("node-metrics-receiver-sysfs-path"),
        env = "ROTEL_NODE_METRICS_RECEIVER_SYSFS_PATH",
        default_value = DEFAULT_SYSFS_PATH
    )]
    pub sysfs_path: String,

    /// Service name for the OTLP Resource attribute
    #[arg(
        id("NODE_METRICS_RECEIVER_SERVICE_NAME"),
        long("node-metrics-receiver-service-name"),
        env = "ROTEL_NODE_METRICS_RECEIVER_SERVICE_NAME",
        default_value = "node_metrics"
    )]
    pub service_name: String,

    /// Regex pattern to exclude filesystem mount points, in addition to the built-in exclusions
    #[arg(
        id("NODE_METRICS_RECEIVER_FILESYSTEM_MOUNT_EXCLUDE"),
        long("node-metrics-receiver-filesystem-mount-exclude"),
        env = "ROTEL_NODE_METRICS_RECEIVER_FILESYSTEM_MOUNT_EXCLUDE"
    )]
    pub filesystem_mount_exclude: Option<String>,

    /// Regex pattern to include only matching metric names
    #[arg(
        id("NODE_METRICS_RECEIVER_INCLUDE_FILTER"),
        long("node-metrics-receiver-include-filter"),
        env = "ROTEL_NODE_METRICS_RECEIVER_INCLUDE_FILTER"
    )]
    pub include_filter: Option<String>,

    /// Regex pattern to exclude matching metric names
    #[arg(
        id("NODE_METRICS_RECEIVER_EXCLUDE_FILTER"),
        long("node-metrics-receiver-exclude-filter"),
        env = "ROTEL_NODE_METRICS_RECEIVER_EXCLUDE_FILTER"
    )]
    pub exclude_filter: Option<String>,
}

impl Default for NodeMetricsReceiverArgs {
    fn default() -> Self {
        Self {
            scrape_interval: Duration::from_secs(60),
            cpu: true,
            loadavg: true,
            memory: true,
            network: true,
            filesystem: true,
            uname: true,
            stat: true,
            processes: true,
            diskstats: true,
            vmstat: true,
            netstat: true,
            sockstat: true,
            filefd: true,
            cpufreq: true,
            thermal_zone: true,
            nvme: true,
            hwmon: true,
            time: true,
            textfile: false,
            textfile_directory: None,
            rootfs_path: "/".to_string(),
            procfs_path: DEFAULT_PROCFS_PATH.to_string(),
            sysfs_path: DEFAULT_SYSFS_PATH.to_string(),
            service_name: "node_metrics".to_string(),
            filesystem_mount_exclude: None,
            include_filter: None,
            exclude_filter: None,
        }
    }
}

impl NodeMetricsReceiverArgs {
    /// Build a NodeMetricsConfig from the CLI args
    pub fn build_config(&self) -> Result<NodeMetricsConfig, BoxError> {
        // Collect enabled collectors
        let mut collectors = Vec::new();
        if self.cpu {
            collectors.push(Collector::Cpu);
        }
        if self.loadavg {
            collectors.push(Collector::Loadavg);
        }
        if self.memory {
            collectors.push(Collector::Memory);
        }
        if self.network {
            collectors.push(Collector::Network);
        }
        if self.filesystem {
            collectors.push(Collector::Filesystem);
        }
        if self.uname {
            collectors.push(Collector::Uname);
        }
        if self.stat {
            collectors.push(Collector::Stat);
        }
        if self.processes {
            collectors.push(Collector::Processes);
        }
        if self.diskstats {
            collectors.push(Collector::Diskstats);
        }
        if self.vmstat {
            collectors.push(Collector::Vmstat);
        }
        if self.netstat {
            collectors.push(Collector::Netstat);
        }
        if self.sockstat {
            collectors.push(Collector::Sockstat);
        }
        if self.filefd {
            collectors.push(Collector::Filefd);
        }
        if self.cpufreq {
            collectors.push(Collector::Cpufreq);
        }
        if self.thermal_zone {
            collectors.push(Collector::ThermalZone);
        }
        if self.nvme {
            collectors.push(Collector::Nvme);
        }
        if self.hwmon {
            collectors.push(Collector::Hwmon);
        }
        if self.time {
            collectors.push(Collector::Time);
        }
        if self.textfile {
            if self.textfile_directory.is_none() {
                return Err(
                    "Textfile collector requires --node-metrics-receiver-textfile-directory".into(),
                );
            }
            collectors.push(Collector::Textfile);
        } else if self.textfile_directory.is_some() {
            tracing::warn!(
                "--node-metrics-receiver-textfile-directory is set but textfile collector is disabled; \
                 add --node-metrics-receiver-textfile true to enable it"
            );
        }

        // When rootfs is set to something other than "/", compose it onto the
        // procfs and sysfs paths. This allows containerized setups to set a single
        // --node-metrics-receiver-rootfs-path=/host instead of separate
        // --node-metrics-receiver-procfs-path=/host/proc and
        // --node-metrics-receiver-sysfs-path=/host/sys.
        // `trim_end_matches` already reduces "/" to "", so only the empty case remains.
        let rootfs = self.rootfs_path.trim_end_matches('/');

        // Reject the combination rather than silently producing "/host/host/proc", which
        // would leave every collector reading a path that does not exist.
        // Compare with path-component awareness: a plain `starts_with` would reject
        // `--procfs-path /hostile/proc` against `--rootfs-path /host`, which is not
        // prefixed at all and composes correctly.
        if !rootfs.is_empty()
            && (matches_path_prefix(&self.procfs_path, rootfs)
                || matches_path_prefix(&self.sysfs_path, rootfs))
        {
            return Err(format!(
                "--node-metrics-receiver-rootfs-path ({}) is prefixed onto the procfs and sysfs \
                 paths, so it cannot be combined with an explicit \
                 --node-metrics-receiver-procfs-path ({}) or --node-metrics-receiver-sysfs-path \
                 ({}). Set either the rootfs path or the individual paths.",
                self.rootfs_path, self.procfs_path, self.sysfs_path
            )
            .into());
        }

        let compose = |path: &str| -> String {
            if rootfs.is_empty() {
                path.to_string()
            } else {
                // Join on a single separator regardless of how the suffix is written
                format!("{}/{}", rootfs, path.trim_start_matches('/'))
            }
        };

        let procfs_path = compose(&self.procfs_path);
        let sysfs_path = compose(&self.sysfs_path);
        // Deliberately not composed: the textfile directory is rotel's own data, not a
        // kernel pseudo-filesystem, and prefixing it would point at a path that usually
        // does not exist inside the host mount.
        let textfile_directory = self.textfile_directory.clone();

        let mut config = NodeMetricsConfig {
            scrape_interval: self.scrape_interval,
            collectors,
            metric_include_filter: None,
            metric_exclude_filter: None,
            filesystem_mount_exclude: None,
            procfs_path,
            sysfs_path,
            rootfs_path: self.rootfs_path.clone(),
            textfile_directory,
            service_name: self.service_name.clone(),
        };

        // Set filters if provided
        if let Some(ref pattern) = self.include_filter {
            config = config.with_include_filter(pattern).map_err(|e| {
                format!(
                    "Invalid --node-metrics-receiver-include-filter regex: {}",
                    e
                )
            })?;
        }

        if let Some(ref pattern) = self.exclude_filter {
            config = config.with_exclude_filter(pattern).map_err(|e| {
                format!(
                    "Invalid --node-metrics-receiver-exclude-filter regex: {}",
                    e
                )
            })?;
        }

        if let Some(ref pattern) = self.filesystem_mount_exclude {
            config = config.with_filesystem_mount_exclude(pattern).map_err(|e| {
                format!(
                    "Invalid --node-metrics-receiver-filesystem-mount-exclude regex: {}",
                    e
                )
            })?;
        }

        config.normalize_and_validate().map_err(|e| {
            format!(
                "{} (see --node-metrics-receiver-scrape-interval and the \
                 --node-metrics-receiver-<collector> flags)",
                e
            )
        })?;

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_args() {
        let args = NodeMetricsReceiverArgs::default();

        let config = args.build_config().unwrap();
        assert_eq!(config.scrape_interval, Duration::from_secs(60));
        // Compare the actual collector set: a length check would still pass if one
        // collector were swapped for another.
        assert_eq!(config.collectors, NodeMetricsConfig::default().collectors);
        assert_eq!(config.procfs_path, DEFAULT_PROCFS_PATH);
        assert_eq!(config.sysfs_path, DEFAULT_SYSFS_PATH);
    }

    /// The hand-written `Default` impl duplicates ~28 clap `default_value` strings.
    /// Without this test, changing one and forgetting the other ships a different
    /// default than the CLI documents.
    #[test]
    fn test_clap_defaults_match_default_impl() {
        use clap::Parser;

        /// Minimal command that flattens only these args, so clap can be driven with no
        /// arguments at all
        #[derive(Debug, Parser)]
        struct OnlyNodeMetrics {
            #[command(flatten)]
            args: NodeMetricsReceiverArgs,
        }

        let parsed = OnlyNodeMetrics::parse_from(["rotel"]).args;
        let defaulted = NodeMetricsReceiverArgs::default();

        let from_clap = parsed.build_config().unwrap();
        let from_default = defaulted.build_config().unwrap();

        assert_eq!(from_clap.scrape_interval, from_default.scrape_interval);
        assert_eq!(from_clap.collectors, from_default.collectors);
        assert_eq!(from_clap.procfs_path, from_default.procfs_path);
        assert_eq!(from_clap.sysfs_path, from_default.sysfs_path);
        assert_eq!(from_clap.service_name, from_default.service_name);
        assert_eq!(
            from_clap.textfile_directory,
            from_default.textfile_directory
        );
    }

    /// The collector toggles must accept an explicit value; clap would otherwise infer
    /// `SetTrue` from the `bool` type and make `--...-cpu false` a parse error.
    #[test]
    fn test_collector_toggles_accept_explicit_values() {
        use clap::Parser;

        #[derive(Debug, Parser)]
        struct OnlyNodeMetrics {
            #[command(flatten)]
            args: NodeMetricsReceiverArgs,
        }

        let args = OnlyNodeMetrics::parse_from([
            "rotel",
            "--node-metrics-receiver-cpu",
            "false",
            "--node-metrics-receiver-vmstat=false",
        ])
        .args;

        assert!(!args.cpu);
        assert!(!args.vmstat);
        assert!(args.loadavg, "untouched toggles stay enabled");

        let config = args.build_config().unwrap();
        assert!(!config.collectors.contains(&Collector::Cpu));
        assert!(!config.collectors.contains(&Collector::Vmstat));
        assert!(config.collectors.contains(&Collector::Loadavg));
    }

    #[test]
    fn test_rootfs_path_conflicts_with_explicit_procfs_path() {
        let args = NodeMetricsReceiverArgs {
            rootfs_path: "/host".to_string(),
            procfs_path: "/host/proc".to_string(),
            ..Default::default()
        };

        // Silently composing these would yield /host/host/proc
        let err = args.build_config().unwrap_err().to_string();
        assert!(
            err.contains("--node-metrics-receiver-rootfs-path"),
            "error should name the conflicting flags, got: {}",
            err
        );
    }

    #[test]
    fn test_rootfs_path_composes_onto_defaults() {
        let args = NodeMetricsReceiverArgs {
            rootfs_path: "/host/".to_string(),
            ..Default::default()
        };

        let config = args.build_config().unwrap();
        assert_eq!(config.procfs_path, "/host/proc");
        assert_eq!(config.sysfs_path, "/host/sys");
    }

    /// clap's duplicate-id and conflicting-flag assertions only fire when the whole
    /// command is built, which no other test does. This makes the explicit `id(...)`
    /// values self-enforcing against a future collision with another receiver's args.
    #[test]
    fn test_full_cli_definition_is_valid() {
        use clap::{CommandFactory, Parser};

        // Flattening every receiver's and exporter's args is what makes a duplicate id
        // or flag collide, so build the whole agent command here.
        #[derive(Debug, Parser)]
        struct FullAgent {
            #[command(flatten)]
            agent: crate::init::args::AgentRun,
        }

        FullAgent::command().debug_assert();
    }

    /// The toggles take a value, so a bare flag must be rejected rather than silently
    /// meaning "true".
    #[test]
    fn test_bare_collector_toggle_is_an_error() {
        use clap::Parser;

        #[derive(Debug, Parser)]
        struct OnlyNodeMetrics {
            #[command(flatten)]
            args: NodeMetricsReceiverArgs,
        }

        let result = OnlyNodeMetrics::try_parse_from(["rotel", "--node-metrics-receiver-cpu"]);
        assert!(
            result.is_err(),
            "a bare collector toggle must require a value"
        );
    }

    #[test]
    fn test_scrape_interval_accepts_duration_strings() {
        use clap::Parser;

        #[derive(Debug, Parser)]
        struct OnlyNodeMetrics {
            #[command(flatten)]
            args: NodeMetricsReceiverArgs,
        }

        let args =
            OnlyNodeMetrics::parse_from(["rotel", "--node-metrics-receiver-scrape-interval", "2m"])
                .args;
        assert_eq!(args.scrape_interval, Duration::from_secs(120));
    }

    #[test]
    fn test_rootfs_path_conflicts_with_explicit_sysfs_path() {
        let args = NodeMetricsReceiverArgs {
            rootfs_path: "/host".to_string(),
            sysfs_path: "/host/sys".to_string(),
            ..Default::default()
        };

        let err = args.build_config().unwrap_err().to_string();
        assert!(
            err.contains("--node-metrics-receiver-rootfs-path"),
            "error should name the conflicting flags, got: {}",
            err
        );
    }

    /// A path whose first component merely *begins* with the rootfs string is not under it,
    /// so it must compose rather than be rejected. A plain `starts_with` got this wrong.
    #[test]
    fn test_rootfs_path_accepts_a_path_that_only_shares_a_prefix_string() {
        let args = NodeMetricsReceiverArgs {
            rootfs_path: "/host".to_string(),
            procfs_path: "/hostile/proc".to_string(),
            ..Default::default()
        };

        let config = args
            .build_config()
            .expect("/hostile/proc is not under /host and must be accepted");
        assert_eq!(config.procfs_path, "/host/hostile/proc");
    }

    /// An explicit procfs path that is not under the rootfs prefix is a legitimate
    /// combination and must still compose.
    #[test]
    fn test_rootfs_path_composes_onto_non_default_procfs_path() {
        let args = NodeMetricsReceiverArgs {
            rootfs_path: "/host".to_string(),
            procfs_path: "/proc-alt".to_string(),
            ..Default::default()
        };

        let config = args.build_config().unwrap();
        assert_eq!(config.procfs_path, "/host/proc-alt");
    }

    /// The textfile directory is rotel's own data, so the rootfs prefix must not be
    /// applied to it.
    #[test]
    fn test_rootfs_path_does_not_compose_onto_textfile_directory() {
        let args = NodeMetricsReceiverArgs {
            rootfs_path: "/host".to_string(),
            textfile: true,
            textfile_directory: Some("/var/lib/rotel/textfile".to_string()),
            ..Default::default()
        };

        let config = args.build_config().unwrap();
        assert_eq!(
            config.textfile_directory.as_deref(),
            Some("/var/lib/rotel/textfile")
        );
    }

    /// The receiver layer reports the property; the flag name is added here.
    #[test]
    fn test_validation_error_points_at_the_flags() {
        let args = NodeMetricsReceiverArgs {
            scrape_interval: Duration::from_millis(500),
            ..Default::default()
        };

        let err = args.build_config().unwrap_err().to_string();
        assert!(
            err.contains("--node-metrics-receiver-"),
            "error should point at the receiver's flags, got: {}",
            err
        );
        assert!(
            err.contains("500ms"),
            "error should report the value, got: {}",
            err
        );
    }

    #[test]
    fn test_selective_collectors() {
        let args = NodeMetricsReceiverArgs {
            scrape_interval: Duration::from_secs(30),
            cpu: false,
            loadavg: true,
            memory: true,
            network: false,
            filesystem: false,
            uname: false,
            stat: false,
            processes: false,
            diskstats: false,
            vmstat: false,
            netstat: false,
            sockstat: false,
            filefd: false,
            cpufreq: false,
            thermal_zone: false,
            nvme: false,
            hwmon: false,
            time: false,
            ..Default::default()
        };

        let config = args.build_config().unwrap();
        assert_eq!(
            config.collectors,
            vec![Collector::Loadavg, Collector::Memory]
        );
    }

    #[test]
    fn test_with_filters() {
        let args = NodeMetricsReceiverArgs {
            loadavg: true,
            include_filter: Some("node_load.*".to_string()),
            ..Default::default()
        };

        let config = args.build_config().unwrap();
        // Assert the filter's effect, not just that one was compiled
        assert!(config.should_include_metric("node_load1"));
        assert!(!config.should_include_metric("node_cpu_seconds_total"));
    }

    #[test]
    fn test_no_collectors_error() {
        let args = NodeMetricsReceiverArgs {
            cpu: false,
            loadavg: false,
            memory: false,
            network: false,
            filesystem: false,
            uname: false,
            stat: false,
            processes: false,
            diskstats: false,
            vmstat: false,
            netstat: false,
            sockstat: false,
            filefd: false,
            cpufreq: false,
            thermal_zone: false,
            nvme: false,
            hwmon: false,
            time: false,
            ..Default::default()
        };

        let result = args.build_config();
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_filter_regex() {
        let args = NodeMetricsReceiverArgs {
            include_filter: Some("[invalid".to_string()),
            ..Default::default()
        };

        // The whole point of the wrapper is to name the flag
        let err = args.build_config().unwrap_err().to_string();
        assert!(
            err.contains("--node-metrics-receiver-include-filter"),
            "error should name the offending flag, got: {}",
            err
        );
    }

    #[test]
    fn test_filesystem_mount_exclude() {
        let args = NodeMetricsReceiverArgs {
            filesystem_mount_exclude: Some("^/nfs/".to_string()),
            ..Default::default()
        };

        let config = args.build_config().unwrap();
        assert!(config.filesystem_mount_exclude.is_some());
        let re = config.filesystem_mount_exclude.as_ref().unwrap();
        assert!(re.is_match("/nfs/share"));
        assert!(!re.is_match("/mnt/data"));
    }

    #[test]
    fn test_textfile_requires_directory() {
        let args = NodeMetricsReceiverArgs {
            textfile: true,
            textfile_directory: None,
            ..Default::default()
        };

        let result = args.build_config();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("textfile-directory")
        );
    }

    #[test]
    fn test_textfile_with_directory() {
        let args = NodeMetricsReceiverArgs {
            textfile: true,
            textfile_directory: Some("/var/lib/node_exporter/textfile".to_string()),
            ..Default::default()
        };

        let config = args.build_config().unwrap();
        assert!(config.collectors.contains(&Collector::Textfile));
        assert_eq!(
            config.textfile_directory,
            Some("/var/lib/node_exporter/textfile".to_string())
        );
    }
}
