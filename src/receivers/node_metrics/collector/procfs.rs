// SPDX-License-Identifier: Apache-2.0

//! Collectors reading `/proc`: CPU, load average, memory, network, kernel stat counters,
//! processes, disk I/O, vmstat, netstat and socket/file-descriptor statistics.
//!
//! Three members are wholly or partly syscall-backed but live here because they belong to
//! the same host-level group: `collect_uname` calls `uname(2)`, `collect_time` reads the
//! wall clock, and `collect_filesystem` takes the mount list from `/proc/mounts` but its
//! values from `statfs(2)`.
//!
//! Metric names and HELP strings, and the filesystem exclusion lists, are derived from
//! Prometheus node_exporter — see the notice in this module's `mod.rs`.

use crate::receivers::node_metrics::collector::util::{
    decode_mount_path, is_partition, is_virtual_disk, matches_path_prefix, utsname_field,
};
use crate::receivers::node_metrics::collector::{CollectedMetric, MetricType, SystemCollector};
use crate::receivers::node_metrics::config::NodeMetricsConfig;
use std::fs;
use tracing::{debug, warn};

// On Linux use the LFS variant: plain `statfs` is the 32-bit interface on glibc and returns
// EOVERFLOW for a filesystem whose block or inode counts do not fit, which would drop large
// filesystems entirely on the 32-bit targets this receiver supports. The BSD `statfs` is
// already 64-bit.
#[cfg(not(target_os = "linux"))]
use libc::{statfs, statfs as StatFs};
#[cfg(target_os = "linux")]
use libc::{statfs64 as StatFs, statfs64 as statfs};

impl SystemCollector {
    /// Collect CPU metrics from /proc/stat
    /// Emits `node_cpu_seconds_total` (modes: user..steal) and
    /// `node_cpu_guest_seconds_total` (modes: user, nice), matching
    /// Prometheus node_exporter conventions.
    pub(super) fn collect_cpu(
        &self,
        config: &NodeMetricsConfig,
        stat_content: Option<&str>,
    ) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let content = match stat_content {
            Some(c) => c,
            None => return metrics,
        };

        // /proc/stat fields (indices 1..=10):
        //   user nice system idle iowait irq softirq steal guest guest_nice
        // Fields 1-8 -> node_cpu_seconds_total
        // Fields 9-10 -> node_cpu_guest_seconds_total (modes: user, nice)
        let cpu_modes = [
            "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal",
        ];
        let guest_modes = ["user", "nice"];

        for line in content.lines() {
            // Match cpu lines (cpu, cpu0, cpu1, etc.)
            if !line.starts_with("cpu") {
                continue;
            }

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            let cpu_label = parts[0];

            // Skip the aggregate "cpu" line — only report per-core metrics,
            // matching Prometheus node_exporter behaviour.
            if cpu_label == "cpu" {
                continue;
            }
            let cpu_num = if let Some(num) = cpu_label.strip_prefix("cpu") {
                num.to_string()
            } else {
                continue;
            };

            // node_cpu_seconds_total: fields 1-8
            if config.should_include_metric("node_cpu_seconds_total") {
                for (i, mode) in cpu_modes.iter().enumerate() {
                    let field_idx = i + 1; // user=1, nice=2, ... softirq=7, steal=8
                    if field_idx >= parts.len() {
                        break;
                    }
                    // Skip rather than substitute 0: for a monotonic counter a fabricated
                    // zero looks like a counter reset and makes rate() spike.
                    let Ok(ticks) = parts[field_idx].parse::<f64>() else {
                        continue;
                    };
                    let seconds = ticks / self.clk_tck;

                    metrics.push(CollectedMetric {
                        name: "node_cpu_seconds_total".to_string(),
                        value: seconds,
                        labels: vec![
                            ("cpu".to_string(), cpu_num.clone()),
                            ("mode".to_string(), mode.to_string()),
                        ],
                        metric_type: MetricType::Counter,
                        unit: Some("s".to_string()),
                        description: Some("Seconds the CPUs spent in each mode".to_string()),
                    });
                }
            }

            // node_cpu_guest_seconds_total: fields 9-10
            if config.should_include_metric("node_cpu_guest_seconds_total") {
                for (i, mode) in guest_modes.iter().enumerate() {
                    let field_idx = 9 + i; // guest=9, guest_nice=10
                    if field_idx >= parts.len() {
                        break;
                    }
                    // Skip rather than substitute 0: for a monotonic counter a fabricated
                    // zero looks like a counter reset and makes rate() spike.
                    let Ok(ticks) = parts[field_idx].parse::<f64>() else {
                        continue;
                    };
                    let seconds = ticks / self.clk_tck;

                    metrics.push(CollectedMetric {
                        name: "node_cpu_guest_seconds_total".to_string(),
                        value: seconds,
                        labels: vec![
                            ("cpu".to_string(), cpu_num.clone()),
                            ("mode".to_string(), mode.to_string()),
                        ],
                        metric_type: MetricType::Counter,
                        unit: Some("s".to_string()),
                        description: Some("Seconds the CPUs spent in guest mode".to_string()),
                    });
                }
            }
        }

        metrics
    }

    /// Collect load average from /proc/loadavg
    pub(super) fn collect_loadavg(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let path = format!("{}/loadavg", self.procfs_path);
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to read {}: {}", path, e);
                return metrics;
            }
        };

        let parts: Vec<&str> = content.split_whitespace().collect();
        if parts.len() < 3 {
            return metrics;
        }

        let loads = [
            ("node_load1", parts[0], "1m load average"),
            ("node_load5", parts[1], "5m load average"),
            ("node_load15", parts[2], "15m load average"),
        ];

        for (name, value_str, description) in loads {
            if config.should_include_metric(name)
                && let Ok(value) = value_str.parse::<f64>()
            {
                metrics.push(CollectedMetric {
                    name: name.to_string(),
                    value,
                    labels: Vec::new(),
                    metric_type: MetricType::Gauge,
                    unit: None,
                    description: Some(description.to_string()),
                });
            }
        }

        metrics
    }

    /// Collect memory metrics from /proc/meminfo
    /// Dynamically reads ALL fields, converting kB to bytes
    pub(super) fn collect_meminfo(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let path = format!("{}/meminfo", self.procfs_path);
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to read {}: {}", path, e);
                return metrics;
            }
        };

        for line in content.lines() {
            // Format: "FieldName:     12345 kB" or "FieldName:     12345"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                continue;
            }

            // Normalize parenthesized suffixes: "Active(anon)" -> "Active_anon"
            let field_name = parts[0]
                .trim_end_matches(':')
                .replace('(', "_")
                .replace(')', "");
            let value: f64 = match parts[1].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Check if value is in kB (most fields are)
            let (final_value, has_unit) = if parts.len() >= 3 && parts[2] == "kB" {
                (value * 1024.0, true) // Convert kB to bytes
            } else {
                (value, false)
            };

            // Convert field name to metric name: "MemTotal" -> "node_memory_MemTotal_bytes"
            let metric_name = if has_unit {
                format!("node_memory_{}_bytes", field_name)
            } else {
                format!("node_memory_{}", field_name)
            };

            if config.should_include_metric(&metric_name) {
                metrics.push(CollectedMetric {
                    name: metric_name,
                    value: final_value,
                    labels: Vec::new(),
                    metric_type: MetricType::Gauge,
                    unit: if has_unit {
                        Some("By".to_string())
                    } else {
                        None
                    },
                    description: None,
                });
            }
        }

        metrics
    }

    /// Collect network device metrics from /proc/net/dev
    pub(super) fn collect_netdev(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let path = format!("{}/net/dev", self.procfs_path);
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to read {}: {}", path, e);
                return metrics;
            }
        };

        // Skip 2 header lines:
        //   "Inter-|   Receive ..."
        //   " face |bytes packets ..."
        for line in content.lines().skip(2) {
            // Split on ':' first to cleanly separate interface name from values.
            // The kernel format is "%6s: %8lu ..." so the colon is always present.
            let (iface, rest) = match line.split_once(':') {
                Some(pair) => pair,
                None => continue,
            };
            let interface = iface.trim();

            let parts: Vec<&str> = rest.split_whitespace().collect();
            if parts.len() < 16 {
                continue;
            }

            let labels = vec![("device".to_string(), interface.to_string())];

            // Receive fields (columns 0-7): bytes, packets, errs, drop, fifo, frame, compressed, multicast
            // Transmit fields (columns 8-15): bytes, packets, errs, drop, fifo, colls, carrier, compressed
            let netdev_metrics = [
                (
                    "node_network_receive_bytes_total",
                    parts[0],
                    "By",
                    "Network device statistic receive_bytes",
                ),
                (
                    "node_network_receive_packets_total",
                    parts[1],
                    "{packets}",
                    "Network device statistic receive_packets",
                ),
                (
                    "node_network_receive_errs_total",
                    parts[2],
                    "{errors}",
                    "Network device statistic receive_errs",
                ),
                (
                    "node_network_receive_drop_total",
                    parts[3],
                    "{packets}",
                    "Network device statistic receive_drop",
                ),
                (
                    "node_network_receive_fifo_total",
                    parts[4],
                    "{events}",
                    "Network device statistic receive_fifo",
                ),
                (
                    "node_network_receive_frame_total",
                    parts[5],
                    "{events}",
                    "Network device statistic receive_frame",
                ),
                (
                    "node_network_receive_compressed_total",
                    parts[6],
                    "{packets}",
                    "Network device statistic receive_compressed",
                ),
                (
                    "node_network_receive_multicast_total",
                    parts[7],
                    "{packets}",
                    "Network device statistic receive_multicast",
                ),
                (
                    "node_network_transmit_bytes_total",
                    parts[8],
                    "By",
                    "Network device statistic transmit_bytes",
                ),
                (
                    "node_network_transmit_packets_total",
                    parts[9],
                    "{packets}",
                    "Network device statistic transmit_packets",
                ),
                (
                    "node_network_transmit_errs_total",
                    parts[10],
                    "{errors}",
                    "Network device statistic transmit_errs",
                ),
                (
                    "node_network_transmit_drop_total",
                    parts[11],
                    "{packets}",
                    "Network device statistic transmit_drop",
                ),
                (
                    "node_network_transmit_fifo_total",
                    parts[12],
                    "{events}",
                    "Network device statistic transmit_fifo",
                ),
                (
                    "node_network_transmit_colls_total",
                    parts[13],
                    "{events}",
                    "Network device statistic transmit_colls",
                ),
                (
                    "node_network_transmit_carrier_total",
                    parts[14],
                    "{events}",
                    "Network device statistic transmit_carrier",
                ),
                (
                    "node_network_transmit_compressed_total",
                    parts[15],
                    "{packets}",
                    "Network device statistic transmit_compressed",
                ),
            ];

            for (name, value_str, unit, description) in netdev_metrics {
                if config.should_include_metric(name)
                    && let Ok(value) = value_str.parse::<f64>()
                {
                    metrics.push(CollectedMetric {
                        name: name.to_string(),
                        value,
                        labels: labels.clone(),
                        metric_type: MetricType::Counter,
                        unit: Some(unit.to_string()),
                        description: Some(description.to_string()),
                    });
                }
            }
        }

        metrics
    }

    /// Collect filesystem metrics using statfs
    pub(super) fn collect_filesystem(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        // Prefer PID 1's mount table, as node_exporter does. `{procfs}/mounts` is a symlink
        // to `self/mounts`, so with a bind-mounted host procfs it still yields *this*
        // process's mount namespace — i.e. the container's — which would report the
        // container's bind mounts labelled as if they were the host's filesystems.
        let init_path = format!("{}/1/mounts", self.procfs_path);
        let self_path = format!("{}/mounts", self.procfs_path);
        let (path, content) = match fs::read_to_string(&init_path) {
            Ok(c) => (init_path, c),
            Err(e) => {
                debug!(
                    "Failed to read {}: {}; falling back to {}",
                    init_path, e, self_path
                );
                match fs::read_to_string(&self_path) {
                    Ok(c) => (self_path, c),
                    Err(e) => {
                        debug!("Failed to read {}: {}", self_path, e);
                        return metrics;
                    }
                }
            }
        };
        debug!("Read mount table from {}", path);

        // Filesystem types to ignore (virtual filesystems)
        let ignore_fstypes = [
            "sysfs",
            "proc",
            "devtmpfs",
            "devpts",
            "securityfs",
            "cgroup",
            "cgroup2",
            "pstore",
            "debugfs",
            "hugetlbfs",
            "mqueue",
            "configfs",
            "binfmt_misc",
            "fusectl",
            "fuse.lxcfs", // virtual, checked before the remote list
            "overlay",
            "nsfs",
            "squashfs",
            "autofs",
            "tracefs",
            "bpf",
            "rpc_pipefs",
            "selinuxfs",
            "iso9660",
            "procfs", // BSD-style procfs type name
        ];

        // Network filesystem types are skipped by default: `statfs` on a mount whose
        // server has gone away blocks in the kernel uninterruptibly, and the scrape
        // runs on a shared blocking thread that cannot be aborted. Local mounts can
        // still be excluded individually with `filesystem_mount_exclude`.
        let ignore_remote_fstypes = [
            "nfs",
            "nfs4",
            "cifs",
            "smb3",
            "smbfs",
            "glusterfs",
            "ceph",
            "lustre",
            "beegfs",
            "gpfs",
            "davfs",
            "davfs2",
            "9p",
            "afs",
            "ncpfs",
            "orangefs",
            "coda",
            // Shared-disk cluster filesystems rather than network mounts, but excluded for
            // the same reason: cluster locking can make statfs block indefinitely.
            "ocfs2",
            "gfs2",
            // FUSE mounts are named by subtype; only the network-backed ones are
            // excluded, so local FUSE filesystems (mergerfs, gocryptfs, encfs, bindfs)
            // keep reporting.
            "fuse.sshfs",
            "fuse.s3fs",
            "fuse.rclone",
            "fuse.glusterfs",
            "fuse.cephfs",
            "fuse.davfs2",
            "fuse.smbnetfs",
            "fuse.curlftpfs",
            "fuse.juicefs",
            "fuse.gcsfuse",
            "fuse.blobfuse2",
            "fuse.goofys",
            "fuse.s3ql",
            "fuse.ceph-fuse",
            "fuse.mfs",
        ];

        // Mount points to ignore (matched with path-boundary awareness:
        // a prefix matches if the mount point equals it exactly or continues
        // with '/', preventing "/dev" from matching "/developer").
        // Excluded including the mount point itself: these are kernel pseudo-filesystems.
        let ignore_mount_paths = ["/proc", "/sys", "/dev"];
        // Excluded only *below* the path: a filesystem mounted exactly here is a real
        // volume worth reporting (a dedicated /var/lib/docker disk is precisely the one
        // most likely to fill up). This follows node_exporter for the docker and
        // credentials paths, and is deliberately broader for /var/lib/containers (where it
        // excludes only .../storage) and /run/user (which it does not exclude at all).
        let ignore_mount_subpaths = [
            "/run/credentials",
            "/run/user",
            "/var/lib/docker",
            "/var/lib/containers",
        ];

        // Deduplicate by (device, mountpoint), matching Prometheus node_exporter.
        // /proc/mounts can contain duplicate entries from bind mounts or mount propagation.
        let mut seen = std::collections::HashSet::new();

        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 4 {
                continue;
            }

            let device = decode_mount_path(parts[0]);
            let mount_point = decode_mount_path(parts[1]);
            let fstype = parts[2];
            // The mount options carry the read-only state, so it does not need a second
            // syscall — and `statfs.f_flags` does not exist on Linux, only on the BSDs.
            let read_only = parts[3].split(',').any(|opt| opt == "ro");

            if !seen.insert((device.clone(), mount_point.clone())) {
                continue;
            }

            // Skip ignored filesystem types
            if ignore_fstypes.contains(&fstype) {
                continue;
            }

            // Skip network filesystems, where statfs can block indefinitely
            if ignore_remote_fstypes.contains(&fstype) {
                debug!(
                    "Skipping remote filesystem {} of type {}",
                    mount_point, fstype
                );
                continue;
            }

            // Skip ignored mount point prefixes (with path boundary check:
            // "/dev" matches "/dev" and "/dev/shm" but not "/developer")
            if ignore_mount_paths
                .iter()
                .any(|prefix| matches_path_prefix(&mount_point, prefix))
            {
                continue;
            }

            if ignore_mount_subpaths
                .iter()
                .any(|prefix| mount_point != *prefix && matches_path_prefix(&mount_point, prefix))
            {
                continue;
            }

            // Skip mount points matching the user-provided exclude regex
            // (useful for excluding NFS/CIFS mounts where statfs may hang)
            if let Some(ref exclude) = config.filesystem_mount_exclude
                && exclude.is_match(&mount_point)
            {
                continue;
            }

            // Call statfs to get filesystem stats (matches Prometheus node_exporter)
            let mut stat: StatFs = unsafe { std::mem::zeroed() };
            // `statfs` resolves in this process's mount namespace, so when the host root is
            // reachable under a prefix the mount point has to be composed onto it. The
            // label keeps the host's own path, so series match a host-side node_exporter.
            let rootfs = config.rootfs_path.trim_end_matches('/');
            let statfs_target = if rootfs.is_empty() {
                mount_point.clone()
            } else {
                format!("{}/{}", rootfs, mount_point.trim_start_matches('/'))
            };

            let mount_point_cstr = match std::ffi::CString::new(statfs_target.as_str()) {
                Ok(c) => c,
                Err(_) => continue,
            };

            let ret = unsafe { statfs(mount_point_cstr.as_ptr(), &mut stat) };
            let statfs_err = std::io::Error::last_os_error();
            if ret != 0 {
                // Warn rather than debug: a filesystem that silently disappears from the
                // metrics is what an operator needs to know about during an incident, and
                // the usual causes (a stale mount, a permission) are actionable.
                warn!("statfs({}) failed: {}", statfs_target, statfs_err);
                continue;
            }

            let labels = vec![
                ("device".to_string(), device),
                ("fstype".to_string(), fstype.to_string()),
                ("mountpoint".to_string(), mount_point),
            ];

            let block_size = stat.f_bsize as f64;
            let total_bytes = stat.f_blocks as f64 * block_size;
            let free_bytes = stat.f_bfree as f64 * block_size;
            let avail_bytes = stat.f_bavail as f64 * block_size;

            let fs_metrics = [
                (
                    "node_filesystem_size_bytes",
                    total_bytes,
                    "Filesystem size in bytes",
                ),
                (
                    "node_filesystem_free_bytes",
                    free_bytes,
                    "Filesystem free space in bytes",
                ),
                (
                    "node_filesystem_avail_bytes",
                    avail_bytes,
                    "Filesystem space available to non-root users in bytes",
                ),
            ];

            for (name, value, description) in fs_metrics {
                if config.should_include_metric(name) {
                    metrics.push(CollectedMetric {
                        name: name.to_string(),
                        value,
                        labels: labels.clone(),
                        metric_type: MetricType::Gauge,
                        unit: Some("By".to_string()),
                        description: Some(description.to_string()),
                    });
                }
            }

            // Files (inodes)
            if config.should_include_metric("node_filesystem_files") {
                metrics.push(CollectedMetric {
                    name: "node_filesystem_files".to_string(),
                    value: stat.f_files as f64,
                    labels: labels.clone(),
                    metric_type: MetricType::Gauge,
                    unit: None,
                    description: Some("Filesystem total file nodes".to_string()),
                });
            }

            if config.should_include_metric("node_filesystem_files_free") {
                metrics.push(CollectedMetric {
                    name: "node_filesystem_files_free".to_string(),
                    value: stat.f_ffree as f64,
                    labels: labels.clone(),
                    metric_type: MetricType::Gauge,
                    unit: None,
                    description: Some("Filesystem free file nodes".to_string()),
                });
            }

            // Readonly flag, taken from the mount options above
            if config.should_include_metric("node_filesystem_readonly") {
                metrics.push(CollectedMetric {
                    name: "node_filesystem_readonly".to_string(),
                    value: if read_only { 1.0 } else { 0.0 },
                    labels,
                    metric_type: MetricType::Gauge,
                    unit: None,
                    description: Some("Filesystem read-only status".to_string()),
                });
            }
        }

        metrics
    }

    /// Collect uname/system information
    pub(super) fn collect_uname(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        // Get uname info
        if !config.should_include_metric("node_uname_info") {
            return metrics;
        }

        let mut uname_info: libc::utsname = unsafe { std::mem::zeroed() };
        let ret = unsafe { libc::uname(&mut uname_info) };

        if ret == 0 {
            // Convert C strings to Rust strings
            let sysname = utsname_field(&uname_info.sysname);
            let nodename = utsname_field(&uname_info.nodename);
            let release = utsname_field(&uname_info.release);
            let version = utsname_field(&uname_info.version);
            let machine = utsname_field(&uname_info.machine);

            #[cfg(target_os = "linux")]
            let domainname = utsname_field(&uname_info.domainname);

            #[allow(unused_mut)] // domainname is pushed only on Linux
            let mut labels = vec![
                ("sysname".to_string(), sysname),
                ("nodename".to_string(), nodename),
                ("release".to_string(), release),
                ("version".to_string(), version),
                ("machine".to_string(), machine),
            ];
            #[cfg(target_os = "linux")]
            labels.push(("domainname".to_string(), domainname));

            metrics.push(CollectedMetric {
                name: "node_uname_info".to_string(),
                value: 1.0,
                labels,
                metric_type: MetricType::Gauge,
                unit: None,
                description: Some(
                    "Labeled system information as provided by the uname system call".to_string(),
                ),
            });
        }

        metrics
    }

    /// Collect time metrics (boot time, current time)
    ///
    /// This is a separate collector from uname, matching Prometheus node_exporter's
    /// `time` collector.  Boot time originates from `/proc/stat` (the `btime` line) and is
    /// grouped here so it can be toggled independently. Note node_exporter emits
    /// `node_boot_time_seconds` from its `stat` collector instead.
    pub(super) fn collect_time(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        // Boot time, read from /proc/stat btime. Suppressed entirely while unknown:
        // reporting 0 would look like a plausible (1970) boot time.
        let boot_time = self.boot_time();
        if boot_time > 0 && config.should_include_metric("node_boot_time_seconds") {
            metrics.push(CollectedMetric {
                name: "node_boot_time_seconds".to_string(),
                value: boot_time as f64,
                labels: Vec::new(),
                metric_type: MetricType::Gauge,
                unit: Some("s".to_string()),
                description: Some("Node boot time in seconds since epoch".to_string()),
            });
        }

        // Current time.  This reads the clock independently of the OTLP
        // data-point timestamp set later in convert_to_otlp_metrics, so the
        // two may differ by the time it takes to run the remaining collectors.
        // Prometheus node_exporter has the same inherent skew.
        if config.should_include_metric("node_time_seconds") {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);

            metrics.push(CollectedMetric {
                name: "node_time_seconds".to_string(),
                value: now,
                labels: Vec::new(),
                metric_type: MetricType::Gauge,
                unit: Some("s".to_string()),
                description: Some("System time in seconds since epoch".to_string()),
            });
        }

        metrics
    }

    /// Collect kernel stat counters from /proc/stat
    ///
    /// Includes forks, context switches, interrupts, and procs running/blocked.
    /// The per-CPU `cpu*` lines are handled by the cpu collector; `softirq` and the
    /// per-interrupt vector of `intr` are not currently exposed.
    pub(super) fn collect_stat(
        &self,
        config: &NodeMetricsConfig,
        stat_content: Option<&str>,
    ) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let content = match stat_content {
            Some(c) => c,
            None => return metrics,
        };

        for line in content.lines() {
            if let Some(rest) = line.strip_prefix("processes ") {
                if config.should_include_metric("node_forks_total")
                    && let Ok(value) = rest.trim().parse::<f64>()
                {
                    metrics.push(CollectedMetric {
                        name: "node_forks_total".to_string(),
                        value,
                        labels: Vec::new(),
                        metric_type: MetricType::Counter,
                        unit: None,
                        description: Some("Total number of forks".to_string()),
                    });
                }
            } else if let Some(rest) = line.strip_prefix("ctxt ") {
                if config.should_include_metric("node_context_switches_total")
                    && let Ok(value) = rest.trim().parse::<f64>()
                {
                    metrics.push(CollectedMetric {
                        name: "node_context_switches_total".to_string(),
                        value,
                        labels: Vec::new(),
                        metric_type: MetricType::Counter,
                        unit: None,
                        description: Some("Total number of context switches".to_string()),
                    });
                }
            } else if let Some(rest) = line.strip_prefix("intr ") {
                // Test the metric filter inside the arm, like the sibling arms do, so that
                // control flow through this chain does not depend on the filter config.
                // The first number is total interrupts. Skip the metric entirely if it
                // cannot be parsed — a fabricated 0 reads as a counter reset downstream.
                if config.should_include_metric("node_intr_total")
                    && let Some(total) = rest
                        .split_whitespace()
                        .next()
                        .and_then(|s| s.parse::<f64>().ok())
                {
                    metrics.push(CollectedMetric {
                        name: "node_intr_total".to_string(),
                        value: total,
                        labels: Vec::new(),
                        metric_type: MetricType::Counter,
                        unit: Some("{interrupts}".to_string()),
                        description: Some("Total number of interrupts serviced".to_string()),
                    });
                }
            } else if let Some(rest) = line.strip_prefix("procs_running ") {
                if config.should_include_metric("node_procs_running")
                    && let Ok(value) = rest.trim().parse::<f64>()
                {
                    metrics.push(CollectedMetric {
                        name: "node_procs_running".to_string(),
                        value,
                        labels: Vec::new(),
                        metric_type: MetricType::Gauge,
                        unit: None,
                        description: Some("Number of processes in runnable state".to_string()),
                    });
                }
            } else if let Some(rest) = line.strip_prefix("procs_blocked ") {
                // Collapsing this into the chain condition would make control flow through
                // the whole chain depend on the metric filter; only the final arm trips the
                // lint, and all five arms deliberately test the filter inside the arm.
                #[allow(clippy::collapsible_if)]
                if config.should_include_metric("node_procs_blocked")
                    && let Ok(value) = rest.trim().parse::<f64>()
                {
                    metrics.push(CollectedMetric {
                        name: "node_procs_blocked".to_string(),
                        value,
                        labels: Vec::new(),
                        metric_type: MetricType::Gauge,
                        unit: None,
                        description: Some(
                            "Number of processes blocked waiting for I/O".to_string(),
                        ),
                    });
                }
            }
        }

        metrics
    }

    /// Collect process metrics: kernel limits from /proc/sys/kernel/
    pub(super) fn collect_processes(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        // Kernel limits (single-file reads, no per-PID scanning)
        if config.should_include_metric("node_processes_max_threads") {
            let path = format!("{}/sys/kernel/threads-max", self.procfs_path);
            if let Ok(content) = fs::read_to_string(&path)
                && let Ok(value) = content.trim().parse::<f64>()
            {
                metrics.push(CollectedMetric {
                    name: "node_processes_max_threads".to_string(),
                    value,
                    labels: Vec::new(),
                    metric_type: MetricType::Gauge,
                    unit: None,
                    description: Some("Limit of threads in the system".to_string()),
                });
            }
        }

        if config.should_include_metric("node_processes_max_processes") {
            let path = format!("{}/sys/kernel/pid_max", self.procfs_path);
            if let Ok(content) = fs::read_to_string(&path)
                && let Ok(value) = content.trim().parse::<f64>()
            {
                metrics.push(CollectedMetric {
                    name: "node_processes_max_processes".to_string(),
                    value,
                    labels: Vec::new(),
                    metric_type: MetricType::Gauge,
                    unit: None,
                    description: Some("Limit of PID values in the system".to_string()),
                });
            }
        }

        metrics
    }

    /// Collect disk I/O statistics from /proc/diskstats
    pub(super) fn collect_diskstats(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let path = format!("{}/diskstats", self.procfs_path);
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to read {}: {}", path, e);
                return metrics;
            }
        };

        for line in content.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 14 {
                continue;
            }

            let device = fields[2];

            // Skip virtual devices, matching node_exporter's default exclusion of
            // `^(z?ram|loop|fd)\d+$` — anchored on a trailing device number so that a
            // real disk sharing one of these prefixes is not dropped.
            if is_virtual_disk(device) {
                continue;
            }

            // Skip partitions: sda1, nvme0n1p1, etc.
            // Keep whole disks: sda, nvme0n1, vda, xvda
            if is_partition(device) {
                continue;
            }

            let labels = vec![("device".to_string(), device.to_string())];

            // Parse helper: any field that cannot be parsed causes the entire device to
            // be skipped, rather than emitting a misleading zero value or a partial set
            // of metrics for that device.
            let parse = |idx: usize| -> Option<f64> { fields[idx].parse().ok() };

            // Parse core values - skip entire device on any parse error
            let Some(rd_ios) = parse(3) else { continue };
            let Some(rd_merges) = parse(4) else { continue };
            let Some(rd_sectors) = parse(5) else { continue };
            let Some(rd_ticks) = parse(6) else { continue };
            let Some(wr_ios) = parse(7) else { continue };
            let Some(wr_merges) = parse(8) else { continue };
            let Some(wr_sectors) = parse(9) else { continue };
            let Some(wr_ticks) = parse(10) else { continue };
            let Some(ios_in_progress) = parse(11) else {
                continue;
            };
            let Some(io_ticks) = parse(12) else { continue };
            let Some(time_in_queue) = parse(13) else {
                continue;
            };

            // Parse the optional extended field groups up front, so that a corrupt field
            // skips the whole device instead of emitting the core metrics and then
            // silently dropping the rest of the groups.
            let discard = if fields.len() >= 18 {
                match (parse(14), parse(15), parse(16), parse(17)) {
                    (Some(a), Some(b), Some(c), Some(d)) => Some((a, b, c, d)),
                    _ => continue,
                }
            } else {
                None
            };
            let flush = if fields.len() >= 20 {
                match (parse(18), parse(19)) {
                    (Some(a), Some(b)) => Some((a, b)),
                    _ => continue,
                }
            } else {
                None
            };

            // Convert sectors to bytes (sector = 512 bytes)
            let rd_bytes = rd_sectors * 512.0;
            let wr_bytes = wr_sectors * 512.0;

            // Convert milliseconds to seconds
            let rd_time_seconds = rd_ticks / 1000.0;
            let wr_time_seconds = wr_ticks / 1000.0;
            let io_time_seconds = io_ticks / 1000.0;
            let time_in_queue_seconds = time_in_queue / 1000.0;

            let disk_metrics = [
                (
                    "node_disk_reads_completed_total",
                    rd_ios,
                    MetricType::Counter,
                    "{operations}",
                    "The total number of reads completed successfully",
                ),
                (
                    "node_disk_reads_merged_total",
                    rd_merges,
                    MetricType::Counter,
                    "{operations}",
                    "The total number of reads merged",
                ),
                (
                    "node_disk_read_bytes_total",
                    rd_bytes,
                    MetricType::Counter,
                    "By",
                    "The total number of bytes read successfully",
                ),
                (
                    "node_disk_read_time_seconds_total",
                    rd_time_seconds,
                    MetricType::Counter,
                    "s",
                    "The total number of seconds spent by all reads",
                ),
                (
                    "node_disk_writes_completed_total",
                    wr_ios,
                    MetricType::Counter,
                    "{operations}",
                    "The total number of writes completed successfully",
                ),
                (
                    "node_disk_writes_merged_total",
                    wr_merges,
                    MetricType::Counter,
                    "{operations}",
                    "The total number of writes merged",
                ),
                (
                    "node_disk_written_bytes_total",
                    wr_bytes,
                    MetricType::Counter,
                    "By",
                    "The total number of bytes written successfully",
                ),
                (
                    "node_disk_write_time_seconds_total",
                    wr_time_seconds,
                    MetricType::Counter,
                    "s",
                    "The total number of seconds spent by all writes",
                ),
                (
                    "node_disk_io_now",
                    ios_in_progress,
                    MetricType::Gauge,
                    "{operations}",
                    "The number of I/Os currently in progress",
                ),
                (
                    "node_disk_io_time_seconds_total",
                    io_time_seconds,
                    MetricType::Counter,
                    "s",
                    "Total seconds spent doing I/Os",
                ),
                (
                    "node_disk_io_time_weighted_seconds_total",
                    time_in_queue_seconds,
                    MetricType::Counter,
                    "s",
                    "The weighted number of seconds spent doing I/Os",
                ),
            ];

            for (name, value, metric_type, unit, description) in disk_metrics {
                if config.should_include_metric(name) {
                    metrics.push(CollectedMetric {
                        name: name.to_string(),
                        value,
                        labels: labels.clone(),
                        metric_type,
                        unit: Some(unit.to_string()),
                        description: Some(description.to_string()),
                    });
                }
            }

            // Discard metrics (kernel 4.18+, fields 14-17)
            if let Some((discard_ios, discard_merges, discard_sectors, discard_ticks)) = discard {
                let discard_time_seconds = discard_ticks / 1000.0;

                let discard_metrics = [
                    (
                        "node_disk_discards_completed_total",
                        discard_ios,
                        "{operations}",
                        "The total number of discards completed successfully",
                    ),
                    (
                        "node_disk_discards_merged_total",
                        discard_merges,
                        "{operations}",
                        "The total number of discards merged",
                    ),
                    (
                        "node_disk_discarded_sectors_total",
                        discard_sectors,
                        "{sectors}",
                        "The total number of sectors discarded successfully",
                    ),
                    (
                        "node_disk_discard_time_seconds_total",
                        discard_time_seconds,
                        "s",
                        "The total number of seconds spent discarding",
                    ),
                ];

                for (name, value, unit, description) in discard_metrics {
                    if config.should_include_metric(name) {
                        metrics.push(CollectedMetric {
                            name: name.to_string(),
                            value,
                            labels: labels.clone(),
                            metric_type: MetricType::Counter,
                            unit: Some(unit.to_string()),
                            description: Some(description.to_string()),
                        });
                    }
                }
            }

            // Flush metrics (kernel 5.5+, fields 18-19)
            if let Some((flush_ios, flush_ticks)) = flush {
                let flush_time_seconds = flush_ticks / 1000.0;

                let flush_metrics = [
                    (
                        "node_disk_flush_requests_total",
                        flush_ios,
                        "{operations}",
                        "The total number of flush requests completed successfully",
                    ),
                    (
                        "node_disk_flush_requests_time_seconds_total",
                        flush_time_seconds,
                        "s",
                        "The total number of seconds spent flushing",
                    ),
                ];

                for (name, value, unit, description) in flush_metrics {
                    if config.should_include_metric(name) {
                        metrics.push(CollectedMetric {
                            name: name.to_string(),
                            value,
                            labels: labels.clone(),
                            metric_type: MetricType::Counter,
                            unit: Some(unit.to_string()),
                            description: Some(description.to_string()),
                        });
                    }
                }
            }
        }

        metrics
    }

    /// Collect virtual memory statistics from /proc/vmstat
    pub(super) fn collect_vmstat(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let path = format!("{}/vmstat", self.procfs_path);
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to read {}: {}", path, e);
                return metrics;
            }
        };

        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                continue;
            }

            let field_name = parts[0];
            let value: f64 = match parts[1].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Convert field name to metric name: "pgfault" -> "node_vmstat_pgfault"
            let metric_name = format!("node_vmstat_{}", field_name);

            if config.should_include_metric(&metric_name) {
                // Most vmstat fields are counters, but nr_* fields are generally gauges
                // (they represent current page counts, not cumulative events).
                // The workingset_* family includes both gauges (workingset_nodes)
                // and counters (workingset_refault_*).  We treat nr_* as gauges except
                // for the cumulative event counters that also carry the nr_ prefix; for
                // workingset_* we whitelist the known gauge.
                // numa_* fields (numa_hit, numa_miss, etc.) are cumulative counters.
                let is_nr_counter = matches!(
                    field_name,
                    "nr_dirtied"
                        | "nr_written"
                        | "nr_throttled_written"
                        | "nr_vmscan_write"
                        | "nr_vmscan_immediate_reclaim"
                        | "nr_foll_pin_acquired"
                        | "nr_foll_pin_released"
                ) || field_name.starts_with("nr_tlb_");

                let metric_type = if !is_nr_counter
                    && (field_name.starts_with("nr_") || field_name == "workingset_nodes")
                {
                    MetricType::Gauge
                } else {
                    MetricType::Counter
                };

                metrics.push(CollectedMetric {
                    name: metric_name,
                    value,
                    labels: Vec::new(),
                    metric_type,
                    unit: None,
                    description: None,
                });
            }
        }

        metrics
    }

    /// Collect network statistics from /proc/net/netstat and /proc/net/snmp.
    ///
    /// Both files share the same alternating header/value line format.
    /// `/proc/net/netstat` contains TcpExt, IpExt, MPTcpExt counters.
    /// `/proc/net/snmp` contains Tcp, Udp, Ip, Icmp, etc. — including the
    /// commonly monitored gauge `CurrEstab`.
    pub(super) fn collect_netstat(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        // Known gauge fields in /proc/net/snmp (all others are counters)
        let snmp_gauges = [
            "CurrEstab",
            "RtoAlgorithm",
            "RtoMin",
            "RtoMax",
            "MaxConn",
            "Forwarding",
            "DefaultTTL",
        ];

        for subpath in ["net/netstat", "net/snmp"] {
            let path = format!("{}/{}", self.procfs_path, subpath);
            let content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) => {
                    debug!("Failed to read {}: {}", path, e);
                    continue;
                }
            };

            // File format: alternating lines of "Protocol: key1 key2 ..." and "Protocol: val1 val2 ..."
            let lines: Vec<&str> = content.lines().collect();
            let mut i = 0;

            while i + 1 < lines.len() {
                let header_line = lines[i];
                let value_line = lines[i + 1];

                let header_parts: Vec<&str> = header_line.split_whitespace().collect();
                let value_parts: Vec<&str> = value_line.split_whitespace().collect();

                // Advance by one, not two, when the pair does not line up: an unexpected
                // or missing line would otherwise desynchronise every remaining protocol
                // in the file instead of resynchronising on the next header.
                if header_parts.is_empty() || value_parts.is_empty() {
                    debug!("Skipping unpaired line {} in {}", i, path);
                    i += 1;
                    continue;
                }

                // Preserve original casing to match Prometheus node_exporter
                // conventions (e.g. "Tcp", "Udp", "TcpExt", "IpExt").
                let protocol = header_parts[0].trim_end_matches(':');

                let value_protocol = value_parts[0].trim_end_matches(':');
                if protocol != value_protocol {
                    debug!(
                        "Skipping line {} in {}: expected values for {}, found {}",
                        i, path, protocol, value_protocol
                    );
                    i += 1;
                    continue;
                }

                for j in 1..header_parts.len().min(value_parts.len()) {
                    let key = header_parts[j];
                    let value: f64 = match value_parts[j].parse() {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let metric_name = format!("node_netstat_{}_{}", protocol, key);

                    if config.should_include_metric(&metric_name) {
                        let metric_type = if snmp_gauges.contains(&key) {
                            MetricType::Gauge
                        } else {
                            MetricType::Counter
                        };

                        metrics.push(CollectedMetric {
                            name: metric_name,
                            value,
                            labels: Vec::new(),
                            metric_type,
                            unit: None,
                            description: None,
                        });
                    }
                }

                i += 2;
            }
        }

        metrics
    }

    /// Collect socket statistics from /proc/net/sockstat and /proc/net/sockstat6
    pub(super) fn collect_sockstat(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();
        let page_size = self.page_size;

        for subpath in ["net/sockstat", "net/sockstat6"] {
            let path = format!("{}/{}", self.procfs_path, subpath);
            let content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) => {
                    debug!("Failed to read {}: {}", path, e);
                    continue;
                }
            };

            // Preserve the original casing to match Prometheus node_exporter
            // conventions (e.g. "TCP", "UDP", "FRAG", "TCP6").
            for line in content.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() < 3 {
                    continue;
                }

                // Format: "protocol: key1 val1 key2 val2 ..."
                let protocol = parts[0].trim_end_matches(':');

                // Parse key-value pairs
                let mut j = 1;
                while j + 1 < parts.len() {
                    let key = parts[j];
                    let raw_value: f64 = match parts[j + 1].parse() {
                        Ok(v) => v,
                        Err(_) => {
                            j += 2;
                            continue;
                        }
                    };

                    // Emit the raw metric
                    let metric_name = format!("node_sockstat_{}_{}", protocol, key);
                    if config.should_include_metric(&metric_name) {
                        metrics.push(CollectedMetric {
                            name: metric_name,
                            value: raw_value,
                            labels: Vec::new(),
                            metric_type: MetricType::Gauge,
                            unit: None,
                            description: None,
                        });
                    }

                    // For "mem" fields, also emit a _bytes metric (pages -> bytes)
                    if key == "mem" {
                        let bytes_name = format!("node_sockstat_{}_{}_bytes", protocol, key);
                        if config.should_include_metric(&bytes_name) {
                            metrics.push(CollectedMetric {
                                name: bytes_name,
                                value: raw_value * page_size,
                                labels: Vec::new(),
                                metric_type: MetricType::Gauge,
                                unit: Some("By".to_string()),
                                description: None,
                            });
                        }
                    }

                    j += 2;
                }
            }
        }

        metrics
    }

    /// Collect file descriptor statistics from /proc/sys/fs/file-nr
    pub(super) fn collect_filefd(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let path = format!("{}/sys/fs/file-nr", self.procfs_path);
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                debug!("Failed to read {}: {}", path, e);
                return metrics;
            }
        };

        // Format: "allocated_fds  free_fds  max_fds"
        let parts: Vec<&str> = content.split_whitespace().collect();
        if parts.len() < 3 {
            return metrics;
        }

        // Skip a field that does not parse rather than reporting a misleading 0
        // (0 allocated descriptors is a plausible-looking but impossible value).
        // parts[1] is "free" — always 0 on kernels ≥2.6, not exposed by node_exporter
        if config.should_include_metric("node_filefd_allocated")
            && let Ok(allocated) = parts[0].parse::<f64>()
        {
            metrics.push(CollectedMetric {
                name: "node_filefd_allocated".to_string(),
                value: allocated,
                labels: Vec::new(),
                metric_type: MetricType::Gauge,
                unit: Some("{file_descriptors}".to_string()),
                description: Some("Number of allocated file descriptors".to_string()),
            });
        }

        if config.should_include_metric("node_filefd_maximum")
            && let Ok(max) = parts[2].parse::<f64>()
        {
            metrics.push(CollectedMetric {
                name: "node_filefd_maximum".to_string(),
                value: max,
                labels: Vec::new(),
                metric_type: MetricType::Gauge,
                unit: Some("{file_descriptors}".to_string()),
                description: Some("Maximum number of file descriptors".to_string()),
            });
        }

        metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::node_metrics::collector::test_support::*;
    use crate::receivers::node_metrics::config::Collector;
    use tracing_test::traced_test;

    #[test]
    fn test_collect_cpu() {
        let tmp = tempfile::tempdir().unwrap();
        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let tck = collector.clk_tck;

        let metrics = collector.collect_cpu(&config_for(vec![Collector::Cpu]), Some(STAT_FIXTURE));

        // The aggregate "cpu" line yields no data point: only per-core lines are
        // reported, matching Prometheus node_exporter.
        assert_eq!(label_values(&metrics, "cpu"), ["0", "1", "2"]);

        let user0 = find_one(
            &metrics,
            "node_cpu_seconds_total",
            &[("cpu", "0"), ("mode", "user")],
        );
        assert_eq!(user0.value, 1000.0 / tck);
        assert_eq!(user0.metric_type, MetricType::Counter);
        assert_eq!(user0.unit.as_deref(), Some("s"));

        assert_eq!(
            find_one(
                &metrics,
                "node_cpu_seconds_total",
                &[("cpu", "1"), ("mode", "idle")]
            )
            .value,
            800_000.0 / tck
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_cpu_seconds_total",
                &[("cpu", "0"), ("mode", "steal")]
            )
            .value,
            8.0 / tck
        );

        // The guest fields go to their own metric, not to node_cpu_seconds_total.
        assert_eq!(
            find_one(
                &metrics,
                "node_cpu_guest_seconds_total",
                &[("cpu", "0"), ("mode", "user")]
            )
            .value,
            900.0 / tck
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_cpu_guest_seconds_total",
                &[("cpu", "0"), ("mode", "nice")]
            )
            .value,
            10.0 / tck
        );
        assert!(
            matching(
                &metrics,
                "node_cpu_seconds_total",
                &[("cpu", "0"), ("mode", "guest")]
            )
            .is_empty()
        );

        // A truncated line reports exactly the fields it has: absent columns are
        // skipped rather than reported as a counter-resetting zero.
        let cpu2_modes: Vec<&str> = metrics
            .iter()
            .filter(|m| label(m, "cpu") == "2")
            .map(|m| label(m, "mode"))
            .collect();
        assert_eq!(cpu2_modes, ["user", "nice", "system"]);
        assert!(matching(&metrics, "node_cpu_guest_seconds_total", &[("cpu", "2")]).is_empty());
    }

    #[test]
    fn test_collect_meminfo() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "meminfo",
            "\
MemTotal:             16 kB
MemFree:            8192 kB
Active(anon):          4 kB
HugePages_Total:       0
Broken:        notanumber kB
",
        );

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_meminfo(&config_for(vec![Collector::Memory]));

        let total = find_one(&metrics, "node_memory_MemTotal_bytes", &[]);
        assert_eq!(total.value, 16384.0);
        assert_eq!(total.unit.as_deref(), Some("By"));
        assert_eq!(total.metric_type, MetricType::Gauge);

        // Parenthesised suffixes are normalised into the metric name.
        assert_eq!(
            find_one(&metrics, "node_memory_Active_anon_bytes", &[]).value,
            4096.0
        );

        // A field without the "kB" unit keeps its raw value, gets no _bytes
        // suffix and carries no OTLP unit.
        let hugepages = find_one(&metrics, "node_memory_HugePages_Total", &[]);
        assert_eq!(hugepages.value, 0.0);
        assert_eq!(hugepages.unit, None);

        // The unparseable line is skipped entirely, leaving only the four good ones.
        assert!(
            !metrics.iter().any(|m| m.name.contains("Broken")),
            "garbage line should be skipped, got {:?}",
            metrics.iter().map(|m| &m.name).collect::<Vec<_>>()
        );
        assert_eq!(metrics.len(), 4);
    }

    #[test]
    fn test_collect_netdev() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "net/dev",
            "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
  eth0: 123456789   98765    1    2    3     4          5         6 987654321   87654    7    8    9   10      11         12
    lo:      1000      10    0    0    0     0          0         0      1000      10
",
        );

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_netdev(&config_for(vec![Collector::Network]));

        // Only eth0 is reported: the two header lines carry no counters and the
        // 10-column "lo" line is too short to be trusted.
        assert_eq!(label_values(&metrics, "device"), ["eth0"]);
        assert_eq!(metrics.len(), 16);

        let rx_bytes = find_one(
            &metrics,
            "node_network_receive_bytes_total",
            &[("device", "eth0")],
        );
        assert_eq!(rx_bytes.value, 123_456_789.0);
        assert_eq!(rx_bytes.unit.as_deref(), Some("By"));
        assert_eq!(rx_bytes.metric_type, MetricType::Counter);

        // Transmit starts at column 9, not wherever the receive block happened to end.
        assert_eq!(
            find_one(
                &metrics,
                "node_network_transmit_bytes_total",
                &[("device", "eth0")]
            )
            .value,
            987_654_321.0
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_network_receive_multicast_total",
                &[("device", "eth0")]
            )
            .value,
            6.0
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_network_transmit_compressed_total",
                &[("device", "eth0")]
            )
            .value,
            12.0
        );
    }

    #[test]
    fn test_collect_diskstats() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "diskstats",
            "\
   8       0 sda 100 10 2000 3000 200 20 4000 5000 0 6000 7000
   8       1 sda1 50 5 1000 1500 100 10 2000 2500 0 3000 3500
   8      16 sdb 100 10 abc 3000 200 20 4000 5000 0 6000 7000
   7       0 loop0 1 0 8 2 0 0 0 0 0 4 2
 252       0 zram0 5 0 40 1 6 0 48 2 0 3 3
 253       0 vda 10 1 200 300 20 2 400 500 0 600 700 1 2 3 400
 259       0 nvme0n1 1000 100 20000 30000 2000 200 40000 50000 0 60000 70000 5 6 7 800 9 1000
",
        );

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_diskstats(&config_for(vec![Collector::Diskstats]));

        // Partitions (sda1), virtual devices (loop0, zram0) and the device with a
        // corrupt core field (sdb) contribute nothing at all.
        assert_eq!(label_values(&metrics, "device"), ["nvme0n1", "sda", "vda"]);

        let sda = [("device", "sda")];

        // Sectors are 512 bytes; the tick fields are milliseconds.
        assert_eq!(
            find_one(&metrics, "node_disk_read_bytes_total", &sda).value,
            2000.0 * 512.0
        );
        assert_eq!(
            find_one(&metrics, "node_disk_written_bytes_total", &sda).value,
            4000.0 * 512.0
        );
        assert_eq!(
            find_one(&metrics, "node_disk_read_time_seconds_total", &sda).value,
            3.0
        );
        assert_eq!(
            find_one(&metrics, "node_disk_io_time_weighted_seconds_total", &sda).value,
            7.0
        );

        // In-flight I/O is a point-in-time reading, unlike its siblings.
        let io_now = find_one(&metrics, "node_disk_io_now", &sda);
        assert_eq!(io_now.value, 0.0);
        assert_eq!(io_now.metric_type, MetricType::Gauge);
        assert_eq!(
            find_one(&metrics, "node_disk_reads_completed_total", &sda).metric_type,
            MetricType::Counter
        );
        assert_eq!(
            find_one(&metrics, "node_disk_io_time_seconds_total", &sda).metric_type,
            MetricType::Counter
        );

        // 14 fields: no discard group and no flush group.
        assert!(matching(&metrics, "node_disk_discards_completed_total", &sda).is_empty());
        assert!(matching(&metrics, "node_disk_flush_requests_total", &sda).is_empty());

        // 18 fields: discard only.
        let vda = [("device", "vda")];
        assert_eq!(
            find_one(&metrics, "node_disk_discarded_sectors_total", &vda).value,
            3.0
        );
        assert_eq!(
            find_one(&metrics, "node_disk_discard_time_seconds_total", &vda).value,
            0.4
        );
        assert!(matching(&metrics, "node_disk_flush_requests_total", &vda).is_empty());

        // 20 fields: discard and flush.
        let nvme = [("device", "nvme0n1")];
        assert_eq!(
            find_one(&metrics, "node_disk_discards_completed_total", &nvme).value,
            5.0
        );
        assert_eq!(
            find_one(&metrics, "node_disk_flush_requests_total", &nvme).value,
            9.0
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_disk_flush_requests_time_seconds_total",
                &nvme
            )
            .value,
            1.0
        );
    }

    #[test]
    fn test_collect_stat_and_processes() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "stat", STAT_FIXTURE);
        write(tmp.path(), "sys/kernel/threads-max", "126988\n");
        write(tmp.path(), "sys/kernel/pid_max", "4194304\n");

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let config = config_for(vec![Collector::Stat, Collector::Processes]);

        let stat = std::fs::read_to_string(tmp.path().join("stat")).unwrap();
        let mut metrics = collector.collect_stat(&config, Some(&stat));
        metrics.extend(collector.collect_processes(&config));

        // Only the first number on the "intr" line is the total; the per-vector
        // counts that follow are not summed into it.
        let intr = find_one(&metrics, "node_intr_total", &[]);
        assert_eq!(intr.value, 1234.0);
        assert_eq!(intr.metric_type, MetricType::Counter);
        assert_eq!(intr.unit.as_deref(), Some("{interrupts}"));

        let forks = find_one(&metrics, "node_forks_total", &[]);
        assert_eq!(forks.value, 4242.0);
        assert_eq!(forks.metric_type, MetricType::Counter);

        let ctxt = find_one(&metrics, "node_context_switches_total", &[]);
        assert_eq!(ctxt.value, 987_654_321.0);
        assert_eq!(ctxt.metric_type, MetricType::Counter);

        // Process counts are instantaneous readings, not cumulative.
        let running = find_one(&metrics, "node_procs_running", &[]);
        assert_eq!(running.value, 3.0);
        assert_eq!(running.metric_type, MetricType::Gauge);

        let blocked = find_one(&metrics, "node_procs_blocked", &[]);
        assert_eq!(blocked.value, 1.0);
        assert_eq!(blocked.metric_type, MetricType::Gauge);

        // The two limits come from /proc/sys/kernel, not from /proc/stat.
        assert_eq!(
            find_one(&metrics, "node_processes_max_threads", &[]).value,
            126_988.0
        );
        assert_eq!(
            find_one(&metrics, "node_processes_max_processes", &[]).value,
            4_194_304.0
        );
    }

    #[test]
    fn test_collect_sockstat() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "net/sockstat",
            "\
sockets: used 100
TCP: inuse 1 orphan 0 tw 0 alloc 2 mem 5
UDP: inuse 3 mem 1
",
        );
        write(
            tmp.path(),
            "net/sockstat6",
            "\
TCP6: inuse 2
UDP6: inuse 1
",
        );

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let page_size = collector.page_size;
        let metrics = collector.collect_sockstat(&config_for(vec![Collector::Sockstat]));

        assert_eq!(
            find_one(&metrics, "node_sockstat_sockets_used", &[]).value,
            100.0
        );
        assert_eq!(
            find_one(&metrics, "node_sockstat_TCP_inuse", &[]).value,
            1.0
        );
        assert_eq!(
            find_one(&metrics, "node_sockstat_TCP_alloc", &[]).value,
            2.0
        );

        // "mem" is a page count; the derived _bytes metric scales it by the page size.
        assert_eq!(find_one(&metrics, "node_sockstat_TCP_mem", &[]).value, 5.0);
        let mem_bytes = find_one(&metrics, "node_sockstat_TCP_mem_bytes", &[]);
        assert_eq!(mem_bytes.value, 5.0 * page_size);
        assert_eq!(mem_bytes.unit.as_deref(), Some("By"));
        assert_eq!(mem_bytes.metric_type, MetricType::Gauge);

        // The IPv6 file contributes its own protocol names.
        assert_eq!(
            find_one(&metrics, "node_sockstat_TCP6_inuse", &[]).value,
            2.0
        );
        assert_eq!(
            find_one(&metrics, "node_sockstat_UDP6_inuse", &[]).value,
            1.0
        );
        assert!(matching(&metrics, "node_sockstat_TCP6_mem_bytes", &[]).is_empty());
    }

    #[test]
    fn test_collect_filefd() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "sys/fs/file-nr",
            "1234\t0\t9223372036854775807\n",
        );

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_filefd(&config_for(vec![Collector::Filefd]));

        let allocated = find_one(&metrics, "node_filefd_allocated", &[]);
        assert_eq!(allocated.value, 1234.0);
        assert_eq!(allocated.metric_type, MetricType::Gauge);
        assert_eq!(allocated.unit.as_deref(), Some("{file_descriptors}"));
        assert_eq!(
            find_one(&metrics, "node_filefd_maximum", &[]).value,
            9_223_372_036_854_775_807f64
        );

        // A field that does not parse is skipped, not reported as an impossible 0.
        let broken = tempfile::tempdir().unwrap();
        write(broken.path(), "sys/fs/file-nr", "notanumber\t0\t100\n");
        let collector = SystemCollector::new(broken.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_filefd(&config_for(vec![Collector::Filefd]));
        assert!(matching(&metrics, "node_filefd_allocated", &[]).is_empty());
        assert_eq!(find_one(&metrics, "node_filefd_maximum", &[]).value, 100.0);
    }

    #[test]
    fn test_collect_filesystem() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let excluded = tmp.path().join("excluded");
        let labelled = tmp.path().join("labelled");
        let merged = tmp.path().join("merged");
        let remote = tmp.path().join("remote");
        let ubi = tmp.path().join("ubi");
        let readonly_dir = tmp.path().join("readonly");
        for dir in [
            &data,
            &excluded,
            &labelled,
            &merged,
            &remote,
            &ubi,
            &readonly_dir,
        ] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let (data, excluded, labelled, merged, remote, ubi, readonly) = (
            data.to_str().unwrap(),
            excluded.to_str().unwrap(),
            labelled.to_str().unwrap(),
            merged.to_str().unwrap(),
            remote.to_str().unwrap(),
            ubi.to_str().unwrap(),
            readonly_dir.to_str().unwrap(),
        );

        // The mount points are real directories so the collector's statfs(2) call
        // succeeds; only the fstype and device columns are synthetic.
        let mounts = format!(
            "/dev/sda2 {data} ext4 rw,relatime 0 0\n\
             /dev/sda2 {data} ext4 rw,relatime 0 0\n\
             proc {data} proc rw,nosuid,nodev,noexec,relatime 0 0\n\
             sysfs {data} sysfs rw,nosuid,nodev,noexec,relatime 0 0\n\
             fileserver:/exports/home {data} nfs4 rw,relatime 0 0\n\
             /dev/sdb1 {excluded} ext4 rw,relatime 0 0\n\
             /dev/disk/by-label/My\\040Disk {labelled} ext4 rw,relatime 0 0\n\
             mergerfs {merged} fuse.mergerfs rw,relatime 0 0\n\
             user@fileserver:/srv {remote} fuse.sshfs rw,relatime 0 0\n\
             ubi0:rootfs {ubi} ubifs rw,relatime 0 0\n\
             /dev/sdc1 {readonly} ext4 ro,relatime 0 0\n"
        );
        write(tmp.path(), "mounts", &mounts);

        let config = NodeMetricsConfig {
            collectors: vec![Collector::Filesystem],
            filesystem_mount_exclude: Some(
                regex::Regex::new(&format!("^{}$", regex::escape(excluded))).unwrap(),
            ),
            ..Default::default()
        };

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_filesystem(&config);

        // proc/sysfs are dropped by fstype, nfs4 and fuse.sshfs as remote filesystems
        // whose statfs can block, the repeated (device, mountpoint) pair as a duplicate,
        // and `excluded` by filesystem_mount_exclude.
        assert_eq!(
            label_values(&metrics, "fstype"),
            ["ext4", "fuse.mergerfs", "ubifs"]
        );
        // The survivors share the temp-dir prefix, so they sort by directory name.
        assert_eq!(
            label_values(&metrics, "mountpoint"),
            [
                data.to_string(),
                labelled.to_string(),
                merged.to_string(),
                readonly.to_string(),
                ubi.to_string()
            ]
        );

        let size = find_one(
            &metrics,
            "node_filesystem_size_bytes",
            &[("mountpoint", data)],
        );
        assert_eq!(size.value, statfs_size_bytes(data));
        assert_eq!(size.unit.as_deref(), Some("By"));
        assert_eq!(size.metric_type, MetricType::Gauge);
        assert_eq!(label(size, "device"), "/dev/sda2");
        assert_eq!(label(size, "fstype"), "ext4");

        // The remaining statfs-derived metrics are reported for the same mount.
        for name in [
            "node_filesystem_free_bytes",
            "node_filesystem_avail_bytes",
            "node_filesystem_files",
            "node_filesystem_files_free",
            "node_filesystem_readonly",
        ] {
            find_one(&metrics, name, &[("mountpoint", data)]);
        }

        // The read-only state comes from the mount options, since `statfs.f_flags` exists
        // only on the BSDs and not on Linux.
        assert_eq!(
            find_one(
                &metrics,
                "node_filesystem_readonly",
                &[("mountpoint", data)]
            )
            .value,
            0.0
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_filesystem_readonly",
                &[("mountpoint", readonly)]
            )
            .value,
            1.0
        );

        // Octal escapes in the device column are decoded before labelling.
        assert_eq!(
            label(
                find_one(
                    &metrics,
                    "node_filesystem_size_bytes",
                    &[("mountpoint", labelled)]
                ),
                "device"
            ),
            "/dev/disk/by-label/My Disk"
        );

        // FUSE mounts are named by subtype, and only the network-backed subtypes are
        // excluded: a local FUSE filesystem is as worth reporting as any other, so the
        // exclusion cannot be a blanket match on the "fuse" prefix.
        let local_fuse = find_one(
            &metrics,
            "node_filesystem_size_bytes",
            &[("mountpoint", merged)],
        );
        assert_eq!(label(local_fuse, "fstype"), "fuse.mergerfs");
        assert_eq!(label(local_fuse, "device"), "mergerfs");
        assert_eq!(local_fuse.value, statfs_size_bytes(merged));

        // ...while the network-backed subtype is skipped, because statfs on a mount
        // whose server has gone away blocks uninterruptibly on a shared blocking thread.
        assert!(
            matching(
                &metrics,
                "node_filesystem_size_bytes",
                &[("mountpoint", remote)]
            )
            .is_empty(),
            "a remote FUSE mount must not be scraped"
        );

        // ubifs is the on-device filesystem of the embedded targets this receiver runs
        // on, so losing it would be the worst possible regression.
        let flash = find_one(
            &metrics,
            "node_filesystem_size_bytes",
            &[("mountpoint", ubi)],
        );
        assert_eq!(label(flash, "fstype"), "ubifs");
        assert_eq!(label(flash, "device"), "ubi0:rootfs");
        assert_eq!(flash.value, statfs_size_bytes(ubi));
        assert_eq!(flash.unit.as_deref(), Some("By"));
    }

    /// The mount table comes from PID 1, not from `self`: with a bind-mounted host procfs
    /// the `self` table is the *reader's* namespace, which would report a container's own
    /// bind mounts labelled as if they were the host's filesystems.
    #[test]
    fn test_collect_filesystem_prefers_pid1_mount_table() {
        let procfs = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let host_mount = root.path().join("data");
        std::fs::create_dir_all(&host_mount).unwrap();
        std::fs::create_dir_all(procfs.path().join("1")).unwrap();

        // What PID 1 sees (the host) versus what this process sees (a container).
        write(
            &procfs.path().join("1"),
            "mounts",
            "/dev/sda1 /data ext4 rw 0 0\n",
        );
        write(
            procfs.path(),
            "mounts",
            "/dev/sdb1 /container-only ext4 rw 0 0\n",
        );

        let collector = SystemCollector::new(procfs.path().to_str().unwrap(), "/sys");
        let config = NodeMetricsConfig {
            collectors: vec![Collector::Filesystem],
            procfs_path: procfs.path().to_str().unwrap().to_string(),
            // The host root is reachable under this prefix, so statfs is composed onto it
            rootfs_path: root.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let metrics = collector.collect_filesystem(&config);

        // The host's mount is reported, labelled with the host's own path...
        let size = find_one(
            &metrics,
            "node_filesystem_size_bytes",
            &[
                ("device", "/dev/sda1"),
                ("fstype", "ext4"),
                ("mountpoint", "/data"),
            ],
        );
        assert!(size.value > 0.0);
        // ...and the container's own table is not consulted.
        assert!(
            matching(
                &metrics,
                "node_filesystem_size_bytes",
                &[("device", "/dev/sdb1")]
            )
            .is_empty(),
            "the self mount table must not be used when PID 1's is readable"
        );
    }

    /// The two halves of the mount-point exclusion: the kernel pseudo-filesystem paths
    /// are excluded including the mount point itself, while the container/runtime paths
    /// are excluded only *below* it — a dedicated volume mounted exactly at
    /// `/var/lib/docker` is a real filesystem, and the one most likely to fill up.
    ///
    /// Those paths are absolute, so a fixture cannot place a real directory at them and
    /// statfs will usually fail. What the test can observe is whether the collector got
    /// as far as calling statfs at all: an excluded mount point is skipped before that,
    /// and so logs no statfs failure and produces no metric.
    #[traced_test]
    #[test]
    fn test_collect_filesystem_mount_exclusion_boundaries() {
        let tmp = tempfile::tempdir().unwrap();

        // Every line uses a real, non-ignored fstype, so the only rule that can drop
        // any of them is the mount-point exclusion under test.
        write(
            tmp.path(),
            "mounts",
            "\
/dev/sda1 /dev ext4 rw,relatime 0 0
/dev/sda2 /dev/shm ext4 rw,relatime 0 0
/dev/sda3 /proc ext4 rw,relatime 0 0
/dev/sda4 /sys ext4 rw,relatime 0 0
/dev/sda5 /var/lib/docker ext4 rw,relatime 0 0
/dev/sda6 /var/lib/docker/volumes/vol1 ext4 rw,relatime 0 0
/dev/sda7 /var/lib/containers ext4 rw,relatime 0 0
/dev/sda8 /var/lib/containers/storage ext4 rw,relatime 0 0
/dev/sda9 /run/user/1000 ext4 rw,relatime 0 0
/dev/sda10 /devices ext4 rw,relatime 0 0
",
        );

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_filesystem(&config_for(vec![Collector::Filesystem]));

        // True when the mount point reached the statfs call: either it succeeded and
        // produced a metric, or it failed and said so.
        let reached_statfs = |mount_point: &str| {
            !matching(
                &metrics,
                "node_filesystem_size_bytes",
                &[("mountpoint", mount_point)],
            )
            .is_empty()
                || logs_contain(&format!("statfs({}) failed", mount_point))
        };

        // Kernel pseudo-filesystems: the mount point itself is excluded, not just what
        // is below it. `/dev` exists on every unix, so it would otherwise be reported.
        for excluded in ["/dev", "/dev/shm", "/proc", "/sys"] {
            assert!(
                !reached_statfs(excluded),
                "{} must be excluded before statfs is called",
                excluded
            );
        }

        // Container and runtime paths: excluded only below the mount point.
        for excluded in [
            "/var/lib/docker/volumes/vol1",
            "/var/lib/containers/storage",
            "/run/user/1000",
        ] {
            assert!(
                !reached_statfs(excluded),
                "{} is below an excluded path and must be skipped",
                excluded
            );
        }
        for reported in ["/var/lib/docker", "/var/lib/containers"] {
            assert!(
                reached_statfs(reported),
                "a filesystem mounted exactly at {} is a real volume and must be scraped",
                reported
            );
        }

        // The exclusions are path-boundary aware: /devices is not below /dev.
        assert!(
            reached_statfs("/devices"),
            "/devices must not be excluded by the /dev prefix"
        );
    }

    /// `node_uname_info` is an annotation metric, so its label *values* are
    /// host-dependent — but the key set, the value, the unit and the type are not.
    #[test]
    fn test_collect_uname() {
        let tmp = tempfile::tempdir().unwrap();
        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_uname(&config_for(vec![Collector::Uname]));

        assert_eq!(metrics.len(), 1);
        let info = &metrics[0];
        assert_eq!(info.name, "node_uname_info");
        // An info metric carries no measurement: the labels are the payload.
        assert_eq!(info.value, 1.0);
        assert_eq!(info.unit, None);
        assert_eq!(info.metric_type, MetricType::Gauge);

        // Order matters as well as membership: these keys are the series identity, and
        // `domainname` exists only in the Linux `utsname`.
        let keys: Vec<&str> = info.labels.iter().map(|(k, _)| k.as_str()).collect();
        #[cfg(target_os = "linux")]
        assert_eq!(
            keys,
            [
                "sysname",
                "nodename",
                "release",
                "version",
                "machine",
                "domainname"
            ]
        );
        #[cfg(not(target_os = "linux"))]
        assert_eq!(
            keys,
            ["sysname", "nodename", "release", "version", "machine"]
        );

        // uname(2) always fills these, so an empty value would mean the fixed-size C
        // character array was decoded wrongly rather than that the host is unusual.
        for key in ["sysname", "release", "machine"] {
            assert!(
                !label(info, key).is_empty(),
                "{} should be populated from uname(2)",
                key
            );
        }

        // A filtered-out uname metric is dropped entirely, not emitted unlabelled.
        let filtered = config_for(vec![Collector::Uname])
            .with_exclude_filter("^node_uname_info$")
            .unwrap();
        assert!(collector.collect_uname(&filtered).is_empty());
    }

    #[test]
    fn test_collect_loadavg() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "loadavg", "0.52 0.58 0.59 2/1234 5678\n");

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_loadavg(&NodeMetricsConfig::default());

        assert_eq!(metrics.len(), 3);
        assert_eq!(find_one(&metrics, "node_load1", &[]).value, 0.52);
        assert_eq!(find_one(&metrics, "node_load5", &[]).value, 0.58);
        assert_eq!(find_one(&metrics, "node_load15", &[]).value, 0.59);
        for metric in &metrics {
            assert_eq!(metric.metric_type, MetricType::Gauge);
            assert!(metric.labels.is_empty());
            assert_eq!(metric.unit, None);
        }
    }

    // ---------------------------------------------------------------------
    // Gauge/counter classification
    // ---------------------------------------------------------------------

    #[test]
    fn test_vmstat_gauge_counter_classification() {
        let tmp = tempfile::tempdir().unwrap();
        let vmstat_content = "\
nr_free_pages 123456
nr_anon_pages 654
nr_zone_inactive_anon 7890
nr_dirtied 11
nr_written 12
nr_throttled_written 13
nr_vmscan_write 13
nr_vmscan_immediate_reclaim 14
nr_foll_pin_acquired 15
nr_foll_pin_released 16
nr_tlb_remote_flush 17
nr_tlb_local_flush_all 18
pgfault 999999
pgmajfault 1234
pswpin 100
pswpout 200
workingset_nodes 500
workingset_refault_anon 300
numa_hit 42
oom_kill 1
";
        std::fs::write(tmp.path().join("vmstat"), vmstat_content).unwrap();

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let config = NodeMetricsConfig::default();
        let metrics = collector.collect_vmstat(&config);

        let find = |name: &str| metrics.iter().find(|m| m.name == name);

        // nr_* fields are gauges
        assert_eq!(
            find("node_vmstat_nr_free_pages").unwrap().metric_type,
            MetricType::Gauge
        );
        assert_eq!(
            find("node_vmstat_nr_zone_inactive_anon")
                .unwrap()
                .metric_type,
            MetricType::Gauge
        );

        // ... except for the cumulative event counters that share the nr_ prefix.
        // These are the control cases for the exception list: without it they would
        // silently be exported as gauges and lose their monotonic semantics.
        for (name, value) in [
            ("node_vmstat_nr_dirtied", 11.0),
            ("node_vmstat_nr_written", 12.0),
            ("node_vmstat_nr_throttled_written", 13.0),
            ("node_vmstat_nr_vmscan_write", 13.0),
            ("node_vmstat_nr_vmscan_immediate_reclaim", 14.0),
            ("node_vmstat_nr_foll_pin_acquired", 15.0),
            ("node_vmstat_nr_foll_pin_released", 16.0),
            // Every nr_tlb_* field is a cumulative flush count.
            ("node_vmstat_nr_tlb_remote_flush", 17.0),
            ("node_vmstat_nr_tlb_local_flush_all", 18.0),
        ] {
            let metric = find(name).unwrap_or_else(|| panic!("{} not collected", name));
            assert_eq!(metric.metric_type, MetricType::Counter, "{}", name);
            assert_eq!(metric.value, value, "{}", name);
        }

        // A neighbouring page-count gauge, to pin that the exception list is
        // name-exact rather than a blanket "nr_ is a counter" rule.
        let anon = find("node_vmstat_nr_anon_pages").unwrap();
        assert_eq!(anon.metric_type, MetricType::Gauge);
        assert_eq!(anon.value, 654.0);

        // vmstat fields carry no unit and no labels, whichever type they get.
        for metric in &metrics {
            assert_eq!(metric.unit, None, "{}", metric.name);
            assert!(metric.labels.is_empty(), "{}", metric.name);
        }

        // workingset_nodes is the one gauge exception
        let workingset_nodes = find("node_vmstat_workingset_nodes").unwrap();
        assert_eq!(workingset_nodes.metric_type, MetricType::Gauge);
        assert_eq!(workingset_nodes.value, 500.0);

        // workingset_refault_* are counters
        let refault = find("node_vmstat_workingset_refault_anon").unwrap();
        assert_eq!(refault.metric_type, MetricType::Counter);
        assert_eq!(refault.value, 300.0);

        // pg*, psw*, numa_*, oom_kill are counters
        assert_eq!(
            find("node_vmstat_pgfault").unwrap().metric_type,
            MetricType::Counter
        );
        assert_eq!(
            find("node_vmstat_pgmajfault").unwrap().metric_type,
            MetricType::Counter
        );
        assert_eq!(
            find("node_vmstat_pswpin").unwrap().metric_type,
            MetricType::Counter
        );
        assert_eq!(
            find("node_vmstat_pswpout").unwrap().metric_type,
            MetricType::Counter
        );
        assert_eq!(
            find("node_vmstat_numa_hit").unwrap().metric_type,
            MetricType::Counter
        );
        assert_eq!(
            find("node_vmstat_oom_kill").unwrap().metric_type,
            MetricType::Counter
        );
    }

    #[test]
    fn test_netstat_snmp_gauge_counter_classification() {
        let tmp = tempfile::tempdir().unwrap();
        let net_dir = tmp.path().join("net");
        std::fs::create_dir(&net_dir).unwrap();

        // /proc/net/snmp contains CurrEstab (gauge) and InSegs (counter)
        let snmp_content = "\
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors
Tcp: 1 200 120000 -1 500 100 10 5 42 99999 88888 100 2 50 0
Ip: Forwarding DefaultTTL InReceives InHdrErrors
Ip: 1 64 123456 0
";
        std::fs::write(net_dir.join("snmp"), snmp_content).unwrap();

        // /proc/net/netstat — all fields are counters
        let netstat_content = "\
TcpExt: SyncookiesSent SyncookiesRecv SyncookiesFailed
TcpExt: 10 20 5
";
        std::fs::write(net_dir.join("netstat"), netstat_content).unwrap();

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let config = NodeMetricsConfig::default();
        let metrics = collector.collect_netstat(&config);

        let find = |name: &str| metrics.iter().find(|m| m.name == name);

        // Known gauges from /proc/net/snmp
        assert_eq!(
            find("node_netstat_Tcp_CurrEstab").unwrap().metric_type,
            MetricType::Gauge
        );
        assert_eq!(
            find("node_netstat_Tcp_RtoAlgorithm").unwrap().metric_type,
            MetricType::Gauge
        );
        assert_eq!(
            find("node_netstat_Tcp_RtoMin").unwrap().metric_type,
            MetricType::Gauge
        );
        assert_eq!(
            find("node_netstat_Tcp_RtoMax").unwrap().metric_type,
            MetricType::Gauge
        );
        assert_eq!(
            find("node_netstat_Tcp_MaxConn").unwrap().metric_type,
            MetricType::Gauge
        );
        assert_eq!(
            find("node_netstat_Ip_Forwarding").unwrap().metric_type,
            MetricType::Gauge
        );
        assert_eq!(
            find("node_netstat_Ip_DefaultTTL").unwrap().metric_type,
            MetricType::Gauge
        );

        // Counters from /proc/net/snmp
        assert_eq!(
            find("node_netstat_Tcp_ActiveOpens").unwrap().metric_type,
            MetricType::Counter
        );
        assert_eq!(
            find("node_netstat_Tcp_InSegs").unwrap().metric_type,
            MetricType::Counter
        );
        assert_eq!(
            find("node_netstat_Ip_InReceives").unwrap().metric_type,
            MetricType::Counter
        );

        // Counters from /proc/net/netstat
        assert_eq!(
            find("node_netstat_TcpExt_SyncookiesSent")
                .unwrap()
                .metric_type,
            MetricType::Counter
        );
    }

    #[test]
    fn test_netstat_resynchronises_after_an_unpaired_line() {
        let tmp = tempfile::tempdir().unwrap();

        // The IpExt header has no value line: the following line is the next
        // protocol's header.  Advancing by two would consume MPTcpExt's header as if
        // it were a value line and lose every protocol after the anomaly.
        write(
            tmp.path(),
            "net/netstat",
            "\
TcpExt: SyncookiesSent SyncookiesRecv
TcpExt: 10 20
IpExt: InNoRoutes InTruncatedPkts
MPTcpExt: MPCapableSYNRX MPCapableACKRX
MPTcpExt: 7 8
",
        );

        // Another shape of anomaly: a stray line the parser cannot pair with
        // anything, sitting between two well-formed protocol blocks.  A blank line
        // follows, as a kernel with no MPTcpExt support leaves behind.
        write(
            tmp.path(),
            "net/snmp",
            "\
Ip: Forwarding DefaultTTL InReceives
Ip: 1 64 123456
some unexpected line the kernel never wrote
Udp: InDatagrams NoPorts
Udp: 100 5

Tcp: CurrEstab InSegs
Tcp: 42 99999
",
        );

        let collector = SystemCollector::new(tmp.path().to_str().unwrap(), "/sys");
        let metrics = collector.collect_netstat(&NodeMetricsConfig::default());

        // Everything before the anomaly is unaffected.
        assert_eq!(
            find_one(&metrics, "node_netstat_TcpExt_SyncookiesSent", &[]).value,
            10.0
        );
        assert_eq!(
            find_one(&metrics, "node_netstat_TcpExt_SyncookiesRecv", &[]).value,
            20.0
        );

        // The unpaired header contributes nothing, rather than pairing up with the
        // next protocol's header and mislabelling its keys.
        assert!(matching(&metrics, "node_netstat_IpExt_InNoRoutes", &[]).is_empty());
        assert!(matching(&metrics, "node_netstat_IpExt_InTruncatedPkts", &[]).is_empty());

        // The protocol *after* the anomaly is still collected, with the right value:
        // the parser resynchronised on the next header instead of staying one line
        // out of step for the rest of the file.
        let mptcp = find_one(&metrics, "node_netstat_MPTcpExt_MPCapableSYNRX", &[]);
        assert_eq!(mptcp.value, 7.0);
        assert_eq!(mptcp.metric_type, MetricType::Counter);
        assert!(mptcp.labels.is_empty());
        assert_eq!(mptcp.unit, None);
        assert_eq!(
            find_one(&metrics, "node_netstat_MPTcpExt_MPCapableACKRX", &[]).value,
            8.0
        );

        // Same in /proc/net/snmp: the block after the stray line survives, and the
        // gauge/counter classification is still applied to the recovered keys.
        assert_eq!(
            find_one(&metrics, "node_netstat_Ip_InReceives", &[]).value,
            123_456.0
        );
        let udp = find_one(&metrics, "node_netstat_Udp_InDatagrams", &[]);
        assert_eq!(udp.value, 100.0);
        assert_eq!(udp.metric_type, MetricType::Counter);
        assert_eq!(
            find_one(&metrics, "node_netstat_Udp_NoPorts", &[]).value,
            5.0
        );

        let curr_estab = find_one(&metrics, "node_netstat_Tcp_CurrEstab", &[]);
        assert_eq!(curr_estab.value, 42.0);
        assert_eq!(curr_estab.metric_type, MetricType::Gauge);
        assert_eq!(
            find_one(&metrics, "node_netstat_Tcp_InSegs", &[]).value,
            99_999.0
        );
    }
}
