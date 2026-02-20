// SPDX-License-Identifier: Apache-2.0

//! Fixture helpers shared by the collector unit tests.

use crate::receivers::node_metrics::collector::CollectedMetric;
use crate::receivers::node_metrics::collector::SystemCollector;
use crate::receivers::node_metrics::config::Collector;
use crate::receivers::node_metrics::config::NodeMetricsConfig;
use std::path::Path;
use tempfile::TempDir;

// ---------------------------------------------------------------------
// Fixture helpers
//
// Every collector takes its procfs/sysfs root as a parameter, so the whole
// module is testable from a fixture directory tree on any platform.
// ---------------------------------------------------------------------

/// Write `contents` to `dir`/`rel`, creating any missing parent directories.
pub(super) fn write(dir: &Path, rel: &str, contents: &str) {
    let path = dir.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&path, contents).unwrap();
}

/// A default config restricted to `collectors`, with no metric filters.
pub(super) fn config_for(collectors: Vec<Collector>) -> NodeMetricsConfig {
    NodeMetricsConfig {
        collectors,
        ..Default::default()
    }
}

/// A collector reading `sysfs` as its sysfs root, with its procfs root pointed at an
/// empty fixture directory.
///
/// For collectors that read sysfs only: passing the real `/proc` would make the test
/// depend on the host, and would read `/proc/stat` on every construction.
/// The returned [`TempDir`] must be kept alive for as long as the collector is used.
pub(super) fn collector_for_sysfs(sysfs: &Path) -> (TempDir, SystemCollector) {
    let procfs = tempfile::tempdir().unwrap();
    let collector = SystemCollector::new(procfs.path().to_str().unwrap(), sysfs.to_str().unwrap());
    (procfs, collector)
}

/// A collector whose procfs and sysfs roots are both an empty fixture directory.
///
/// For collectors that read neither root — the textfile collector takes its directory
/// from the config — so the test touches nothing outside its own fixtures.
/// The returned [`TempDir`] must be kept alive for as long as the collector is used.
pub(super) fn collector_with_empty_roots() -> (TempDir, SystemCollector) {
    let roots = tempfile::tempdir().unwrap();
    let path = roots.path().to_str().unwrap().to_string();
    let collector = SystemCollector::new(&path, &path);
    (roots, collector)
}

/// All metrics named `name` that carry every one of `labels`.
pub(super) fn matching<'a>(
    metrics: &'a [CollectedMetric],
    name: &str,
    labels: &[(&str, &str)],
) -> Vec<&'a CollectedMetric> {
    metrics
        .iter()
        .filter(|m| {
            m.name == name
                && labels
                    .iter()
                    .all(|(k, v)| m.labels.iter().any(|(mk, mv)| mk == k && mv == v))
        })
        .collect()
}

/// The single metric named `name` carrying `labels`; panics otherwise.
pub(super) fn find_one<'a>(
    metrics: &'a [CollectedMetric],
    name: &str,
    labels: &[(&str, &str)],
) -> &'a CollectedMetric {
    let mut found = matching(metrics, name, labels);
    assert_eq!(
        found.len(),
        1,
        "expected exactly one {} with labels {:?}, found {}",
        name,
        labels,
        found.len()
    );
    found.pop().unwrap()
}

/// Value of label `key`, or `""` when the label is absent.
pub(super) fn label<'a>(metric: &'a CollectedMetric, key: &str) -> &'a str {
    metric
        .labels
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .unwrap_or_default()
}

/// Sorted, deduplicated values of label `key` across `metrics`.
pub(super) fn label_values(metrics: &[CollectedMetric], key: &str) -> Vec<String> {
    let mut values: Vec<String> = metrics.iter().map(|m| label(m, key).to_string()).collect();
    values.sort();
    values.dedup();
    values
}

/// Total bytes the real `statfs(2)` reports for `path`.
pub(super) fn statfs_size_bytes(path: &str) -> f64 {
    // Match the collector's choice of interface so the expected value is computed the same
    // way on every target: Linux uses the LFS variant.
    #[cfg(not(target_os = "linux"))]
    use libc::{statfs, statfs as StatFs};
    #[cfg(target_os = "linux")]
    use libc::{statfs64 as StatFs, statfs64 as statfs};

    let mut stat: StatFs = unsafe { std::mem::zeroed() };
    let cpath = std::ffi::CString::new(path).unwrap();
    assert_eq!(
        unsafe { statfs(cpath.as_ptr(), &mut stat) },
        0,
        "statfs({}) failed: {}",
        path,
        std::io::Error::last_os_error()
    );
    stat.f_blocks as f64 * stat.f_bsize as f64
}

/// A trimmed but realistically shaped `/proc/stat`.
///
/// `cpu2` is deliberately truncated after the `system` field, as a kernel
/// predating the later columns would report it.
pub(super) const STAT_FIXTURE: &str = "\
cpu  3005 606 907 1200000 1500 180 210 24 2700 30
cpu0 1000 200 300 400000 500 60 70 8 900 10
cpu1 2000 400 600 800000 1000 120 140 16 1800 20
cpu2 5 6 7
intr 1234 0 0
ctxt 987654321
btime 1700000000
processes 4242
procs_running 3
procs_blocked 1
";
