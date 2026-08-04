// SPDX-License-Identifier: Apache-2.0

//! Node Metrics Integration Tests
//!
//! These tests run against the real `/proc` and `/sys` of a Linux host. They exist to
//! catch what the fixture-based unit tests cannot: a drift between the kernel's actual
//! file formats and the parsers in this receiver.
//!
//! To run these tests:
//!
//! ```
//! NODE_METRICS_INTEGRATION_TESTS=true cargo test --test node_metrics_integration_tests --features node_metrics_receiver
//! ```
//!
//! Note: collectors for optional hardware (thermal zones, hwmon sensors, NVMe devices,
//! cpufreq, diskstats) are absent in many containers and virtual machines. For those the
//! tests accept an empty result and require only that any value produced is finite; they
//! never require a particular piece of hardware to be present, which would be flaky by
//! design.

#![cfg(all(
    target_os = "linux",
    node_metrics_integration_tests = "true",
    feature = "node_metrics_receiver"
))]

use rotel::receivers::node_metrics::collector::{MetricType, SystemCollector};
use rotel::receivers::node_metrics::config::{Collector, NodeMetricsConfig};

/// Build a config restricted to the given collectors, reading the real procfs/sysfs
fn config_for(collectors: Vec<Collector>) -> NodeMetricsConfig {
    NodeMetricsConfig {
        collectors,
        ..Default::default()
    }
}

#[test]
fn test_boot_time_is_plausible() {
    let collector = SystemCollector::default();
    let boot_time = collector.boot_time();

    // Deliberately tolerant: a board with no RTC reports a near-epoch btime until NTP
    // syncs, and the collector itself treats 0 as "unknown" and suppresses the metric. So
    // only require that the value is not in the future.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        boot_time <= now,
        "boot time {} is in the future (now {})",
        boot_time,
        now
    );
}

#[test]
fn test_core_collectors_produce_metrics_on_a_real_host() {
    let collector = SystemCollector::default();
    let config = config_for(vec![
        Collector::Cpu,
        Collector::Loadavg,
        Collector::Memory,
        Collector::Filesystem,
        Collector::Stat,
        Collector::Vmstat,
    ]);

    let metrics = collector.collect(&config);
    assert!(
        !metrics.is_empty(),
        "expected metrics from a real Linux host"
    );

    // These four exist on every Linux kernel this receiver targets. If the kernel ever
    // changes a format the parser depends on, one of them disappears.
    // node_filesystem_size_bytes is deliberately absent from this list: a container whose
    // only mounts are overlay/proc/sysfs reports no filesystems at all, and those types are
    // excluded by design. test_filesystem_stats_are_self_consistent covers it where present.
    for expected in [
        "node_cpu_seconds_total",
        "node_memory_MemTotal_bytes",
        "node_load1",
    ] {
        assert!(
            metrics.iter().any(|m| m.name == expected),
            "missing {} from a real host scrape",
            expected
        );
    }

    // The payoff of running against a real kernel: no value is ever NaN or infinite,
    // whatever the host looks like.
    for m in &metrics {
        assert!(
            m.value.is_finite(),
            "metric {} has a non-finite value: {}",
            m.name,
            m.value
        );
        // Non-negativity is only asserted where it is part of the metric's definition —
        // byte, second and filesystem gauges. Kernel-supplied fields in general (and
        // temperatures in particular) may legitimately be negative.
        let must_be_non_negative = m.name.ends_with("_bytes")
            || m.name.ends_with("_bytes_total")
            || m.name.ends_with("_seconds_total")
            || m.name.starts_with("node_filesystem_")
            || m.name.starts_with("node_load");
        if must_be_non_negative {
            assert!(
                m.value >= 0.0,
                "metric {} has a negative value: {}",
                m.name,
                m.value
            );
        }
    }
}

#[test]
fn test_filesystem_stats_are_self_consistent() {
    let collector = SystemCollector::default();
    let metrics = collector.collect(&config_for(vec![Collector::Filesystem]));

    // Do not require a mount point of "/": inside a container the root is usually an
    // overlay mount, which is excluded by filesystem type. Pick the largest reported
    // filesystem instead, which exists on any host that reports one at all.
    let Some(labels) = metrics
        .iter()
        .filter(|m| m.name == "node_filesystem_size_bytes")
        .max_by(|a, b| a.value.total_cmp(&b.value))
        .map(|m| m.labels.clone())
    else {
        eprintln!("Skipping test: no filesystems reported on this host");
        return;
    };
    let mountpoint = labels
        .iter()
        .find(|(k, _)| k == "mountpoint")
        .map(|(_, v)| v.as_str())
        .unwrap_or("<unknown>");

    // Key on the whole label set, not just the mount point: with an over-mount two
    // devices share a mount point, and mixing their values would fail spuriously.
    let value_for = |name: &str| -> Option<f64> {
        metrics
            .iter()
            .find(|m| m.name == name && m.labels == labels)
            .map(|m| m.value)
    };

    let size = value_for("node_filesystem_size_bytes").expect("no size");
    let free = value_for("node_filesystem_free_bytes").expect("no free");
    let avail = value_for("node_filesystem_avail_bytes").expect("no avail");

    assert!(
        size > 0.0,
        "filesystem {} size should be positive",
        mountpoint
    );
    assert!(
        avail <= free && free <= size,
        "{}: expected avail ({}) <= free ({}) <= size ({})",
        mountpoint,
        avail,
        free,
        size
    );
}

#[test]
fn test_counters_and_gauges_are_classified_on_a_real_host() {
    let collector = SystemCollector::default();
    let metrics = collector.collect(&config_for(vec![Collector::Cpu, Collector::Stat]));

    let cpu = metrics
        .iter()
        .find(|m| m.name == "node_cpu_seconds_total")
        .expect("no CPU metric");
    assert_eq!(cpu.metric_type, MetricType::Counter);

    let running = metrics
        .iter()
        .find(|m| m.name == "node_procs_running")
        .expect("no node_procs_running metric");
    assert_eq!(running.metric_type, MetricType::Gauge);
}

/// Collectors backed by interfaces every Linux kernel provides must return something.
/// If one silently stops parsing, this is what notices.
#[test]
fn test_always_available_collectors_produce_metrics() {
    let collector = SystemCollector::default();

    // Netstat, Sockstat and Filefd are deliberately excluded: they need
    // /proc/net/{netstat,snmp,sockstat} and /proc/sys/fs/file-nr, which sandboxes such as
    // gVisor and WSL1 only partially synthesise. They are covered by the tolerate-empty
    // test below instead.
    for c in [
        Collector::Network,
        Collector::Uname,
        Collector::Time,
        Collector::Processes,
    ] {
        let metrics = collector.collect(&config_for(vec![c.clone()]));
        assert!(
            !metrics.is_empty(),
            "collector {:?} produced no metrics on a real Linux host",
            c
        );
        for m in &metrics {
            assert!(
                m.value.is_finite(),
                "collector {:?} produced a non-finite value for {}",
                c,
                m.name
            );
        }
    }
}

/// Hardware-dependent collectors may legitimately produce nothing (container, VM, no
/// sensors), so assert only that they degrade quietly. Note these can report negative
/// values — sub-zero temperatures are legal — so no sign assertion here.
#[test]
fn test_optional_hardware_collectors_do_not_panic() {
    let collector = SystemCollector::default();

    for c in [
        Collector::Cpufreq,
        Collector::ThermalZone,
        Collector::Nvme,
        Collector::Hwmon,
        Collector::Diskstats,
        // Present on ordinary kernels but only partially synthesised by some sandboxes
        Collector::Netstat,
        Collector::Sockstat,
        Collector::Filefd,
    ] {
        let metrics = collector.collect(&config_for(vec![c.clone()]));
        for m in &metrics {
            assert!(
                m.value.is_finite(),
                "collector {:?} produced a non-finite value for {}",
                c,
                m.name
            );
        }
    }
}

/// The invariant a fixture can never prove: on a real host's mount table, hwmon set and
/// network devices, no OTLP metric may contain two data points with the same attribute
/// set, and no (name, type) pair may appear twice.
#[test]
fn test_real_host_scrape_has_no_conflicting_data_points() {
    use rotel::receivers::node_metrics::convert::convert_to_otlp_metrics;

    let collector = SystemCollector::default();
    let metrics = collector.collect(&NodeMetricsConfig::default());
    let resource_metrics = convert_to_otlp_metrics(metrics, collector.boot_time(), "node_metrics");

    let mut seen_metrics = std::collections::HashSet::new();
    for sm in &resource_metrics.scope_metrics {
        for m in &sm.metrics {
            let points = match &m.data {
                Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(g)) => {
                    assert!(
                        seen_metrics.insert((m.name.clone(), "gauge")),
                        "duplicate gauge {}",
                        m.name
                    );
                    &g.data_points
                }
                Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(s)) => {
                    assert!(
                        seen_metrics.insert((m.name.clone(), "sum")),
                        "duplicate sum {}",
                        m.name
                    );
                    &s.data_points
                }
                _ => continue,
            };

            let mut seen_points = std::collections::HashSet::new();
            for dp in points {
                let mut key: Vec<(String, String)> = dp
                    .attributes
                    .iter()
                    .map(|kv| {
                        let value = match &kv.value {
                            Some(v) => format!("{:?}", v.value),
                            None => String::new(),
                        };
                        (kv.key.clone(), value)
                    })
                    .collect();
                key.sort();
                assert!(
                    seen_points.insert(key.clone()),
                    "metric {} has duplicate data points for attributes {:?}",
                    m.name,
                    key
                );
            }
        }
    }
}

#[test]
fn test_hwmon_chip_labels_are_unique_per_sensor() {
    // Two chips of the same model (e.g. two NVMe drives) must not collapse onto the same
    // chip label, which would produce duplicate OTLP attribute sets.
    let collector = SystemCollector::default();
    let metrics = collector.collect(&config_for(vec![Collector::Hwmon]));

    let mut seen = std::collections::HashSet::new();
    for m in metrics.iter().filter(|m| m.name != "node_hwmon_chip_names") {
        let key = (m.name.clone(), m.labels.clone());
        assert!(
            seen.insert(key),
            "duplicate label set for {}: {:?}",
            m.name,
            m.labels
        );
    }
}
