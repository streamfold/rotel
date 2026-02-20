// SPDX-License-Identifier: Apache-2.0

//! sysfs-backed collectors: CPU frequency, thermal zones, NVMe and hwmon sensors.
//!
//! Metric names, HELP strings and the hwmon chip-naming approach are derived from
//! Prometheus node_exporter — see the notice in this module's `mod.rs`.

use crate::receivers::node_metrics::collector::{CollectedMetric, MetricType, SystemCollector};
use crate::receivers::node_metrics::config::NodeMetricsConfig;
use std::fs;
use tracing::debug;

impl SystemCollector {
    /// Collect CPU frequency metrics from /sys/devices/system/cpu/cpu*/cpufreq/
    pub(super) fn collect_cpufreq(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();
        let cpu_dir = format!("{}/devices/system/cpu", self.sysfs_path);

        let entries = match fs::read_dir(&cpu_dir) {
            Ok(e) => e,
            Err(e) => {
                debug!("Failed to read {}: {}", cpu_dir, e);
                return metrics;
            }
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Match cpu0, cpu1, etc.
            let cpu_num = match name_str.strip_prefix("cpu") {
                Some(n) if n.chars().all(|c| c.is_ascii_digit()) && !n.is_empty() => n.to_string(),
                _ => continue,
            };

            let cpufreq_dir = entry.path().join("cpufreq");

            let freq_files = [
                (
                    "scaling_cur_freq",
                    "node_cpu_scaling_frequency_hertz",
                    "Current scaled CPU frequency",
                ),
                (
                    "scaling_min_freq",
                    "node_cpu_scaling_frequency_min_hertz",
                    "Minimum scaled CPU frequency",
                ),
                (
                    "scaling_max_freq",
                    "node_cpu_scaling_frequency_max_hertz",
                    "Maximum scaled CPU frequency",
                ),
                (
                    "cpuinfo_cur_freq",
                    "node_cpu_frequency_hertz",
                    "Current CPU frequency from hardware",
                ),
                (
                    "cpuinfo_min_freq",
                    "node_cpu_frequency_min_hertz",
                    "Minimum CPU frequency from hardware",
                ),
                (
                    "cpuinfo_max_freq",
                    "node_cpu_frequency_max_hertz",
                    "Maximum CPU frequency from hardware",
                ),
            ];

            for (file, metric_name, description) in &freq_files {
                if !config.should_include_metric(metric_name) {
                    continue;
                }

                let path = cpufreq_dir.join(file);
                if let Ok(content) = fs::read_to_string(&path) {
                    // Values in cpufreq are in kHz, convert to Hz
                    if let Ok(khz) = content.trim().parse::<f64>() {
                        metrics.push(CollectedMetric {
                            name: metric_name.to_string(),
                            value: khz * 1000.0,
                            labels: vec![("cpu".to_string(), cpu_num.clone())],
                            metric_type: MetricType::Gauge,
                            unit: Some("Hz".to_string()),
                            description: Some(description.to_string()),
                        });
                    }
                }
            }
        }

        metrics
    }

    /// Collect thermal zone temperatures from /sys/class/thermal/thermal_zone*/
    pub(super) fn collect_thermal_zone(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();
        let thermal_dir = format!("{}/class/thermal", self.sysfs_path);

        let entries = match fs::read_dir(&thermal_dir) {
            Ok(e) => e,
            Err(e) => {
                debug!("Failed to read {}: {}", thermal_dir, e);
                return metrics;
            }
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            let zone_num = match name_str.strip_prefix("thermal_zone") {
                Some(n) if n.chars().all(|c| c.is_ascii_digit()) && !n.is_empty() => n.to_string(),
                _ => continue,
            };

            let zone_path = entry.path();

            // Read zone type (e.g., "x86_pkg_temp", "acpitz")
            let zone_type = fs::read_to_string(zone_path.join("type"))
                .map(|s| s.trim().to_string())
                .unwrap_or_default();

            // Read temperature (millidegrees Celsius -> Celsius)
            if config.should_include_metric("node_thermal_zone_temp")
                && let Ok(content) = fs::read_to_string(zone_path.join("temp"))
                && let Ok(millideg) = content.trim().parse::<f64>()
            {
                metrics.push(CollectedMetric {
                    name: "node_thermal_zone_temp".to_string(),
                    value: millideg / 1000.0,
                    labels: vec![
                        ("zone".to_string(), zone_num.clone()),
                        ("type".to_string(), zone_type.clone()),
                    ],
                    metric_type: MetricType::Gauge,
                    unit: Some("Cel".to_string()),
                    description: Some("Zone temperature in Celsius".to_string()),
                });
            }
        }

        // Cooling devices
        if (config.should_include_metric("node_cooling_device_cur_state")
            || config.should_include_metric("node_cooling_device_max_state"))
            && let Ok(entries) = fs::read_dir(&thermal_dir)
        {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                // Require the trailing index, like the cpu/thermal_zone/nvme scans do
                let is_cooling_device = name_str
                    .strip_prefix("cooling_device")
                    .is_some_and(|r| !r.is_empty() && r.chars().all(|c| c.is_ascii_digit()));
                if !is_cooling_device {
                    continue;
                }

                let dev_path = entry.path();
                // Use bare index ("0", "1") matching Prometheus node_exporter convention.
                // The guard above already required the prefix, so this cannot be empty.
                let dev_name = name_str
                    .strip_prefix("cooling_device")
                    .unwrap_or_default()
                    .to_string();

                let dev_type = fs::read_to_string(dev_path.join("type"))
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();

                let labels = vec![
                    ("name".to_string(), dev_name),
                    ("type".to_string(), dev_type),
                ];

                for (file, metric_name, desc) in [
                    (
                        "cur_state",
                        "node_cooling_device_cur_state",
                        "Current state of the cooling device",
                    ),
                    (
                        "max_state",
                        "node_cooling_device_max_state",
                        "Maximum state of the cooling device",
                    ),
                ] {
                    if !config.should_include_metric(metric_name) {
                        continue;
                    }
                    if let Ok(content) = fs::read_to_string(dev_path.join(file))
                        && let Ok(val) = content.trim().parse::<f64>()
                    {
                        metrics.push(CollectedMetric {
                            name: metric_name.to_string(),
                            value: val,
                            labels: labels.clone(),
                            metric_type: MetricType::Gauge,
                            unit: None,
                            description: Some(desc.to_string()),
                        });
                    }
                }
            }
        }

        metrics
    }

    /// Collect NVMe device info from /sys/class/nvme/
    pub(super) fn collect_nvme(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        if !config.should_include_metric("node_nvme_info") {
            return metrics;
        }

        let nvme_dir = format!("{}/class/nvme", self.sysfs_path);
        let entries = match fs::read_dir(&nvme_dir) {
            Ok(e) => e,
            Err(e) => {
                debug!("Failed to read {}: {}", nvme_dir, e);
                return metrics;
            }
        };

        for entry in entries.flatten() {
            let dev_path = entry.path();
            let dev_name = entry.file_name().to_string_lossy().to_string();

            // Only real controller entries (nvme0, nvme1, ...), consistent with the
            // cpufreq and thermal-zone collectors. A stray file in this directory would
            // otherwise produce an info metric with five empty labels.
            let is_controller = dev_name
                .strip_prefix("nvme")
                .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()));
            if !is_controller {
                debug!("Skipping non-controller entry {} in {}", dev_name, nvme_dir);
                continue;
            }

            let read_attr = |attr: &str| -> String {
                fs::read_to_string(dev_path.join(attr))
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default()
            };

            metrics.push(CollectedMetric {
                name: "node_nvme_info".to_string(),
                value: 1.0,
                labels: vec![
                    ("device".to_string(), dev_name),
                    ("firmware_revision".to_string(), read_attr("firmware_rev")),
                    ("model".to_string(), read_attr("model")),
                    ("serial".to_string(), read_attr("serial")),
                    ("state".to_string(), read_attr("state")),
                ],
                metric_type: MetricType::Gauge,
                unit: None,
                description: Some("NVMe device information".to_string()),
            });
        }

        metrics
    }

    /// Collect hardware monitoring sensor data from /sys/class/hwmon/
    pub(super) fn collect_hwmon(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();
        let hwmon_dir = format!("{}/class/hwmon", self.sysfs_path);

        let entries = match fs::read_dir(&hwmon_dir) {
            Ok(e) => e,
            Err(e) => {
                debug!("Failed to read {}: {}", hwmon_dir, e);
                return metrics;
            }
        };

        for entry in entries.flatten() {
            let hwmon_path = entry.path();
            let hwmon_dir_name = entry.file_name().to_string_lossy().to_string();

            // Require the trailing index, like the cpu/thermal_zone/nvme/cooling_device
            // scans do, so a stray file in this directory is not treated as a chip.
            let is_hwmon_dir = hwmon_dir_name
                .strip_prefix("hwmon")
                .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()));
            if !is_hwmon_dir {
                debug!(
                    "Skipping non-chip entry {} in {}",
                    hwmon_dir_name, hwmon_dir
                );
                continue;
            }

            // The human-readable model name, published separately as an annotation metric.
            // It is not usable as an identity on its own: it is a chip *model* and repeats
            // across instances (two NVMe drives both report "nvme").
            let chip_name = fs::read_to_string(hwmon_path.join("name"))
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());

            // The `chip` label must uniquely identify the hwmon instance, otherwise two
            // chips of the same model produce identical attribute sets and one of them is
            // dropped. The backing device name is both unique and stable, so prefer it.
            // Without a `device` symlink, fall back to an identity that still includes the
            // hwmonN directory, which is unique within a host — the model name alone is
            // not. That index depends on module load order, so a fallback identity is not
            // stable across reboots.
            let chip = fs::read_link(hwmon_path.join("device"))
                .ok()
                .and_then(|target| {
                    target
                        .file_name()
                        .map(|n| sanitize_hwmon_chip(&n.to_string_lossy()))
                })
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| match &chip_name {
                    Some(name) => format!("{}_{}", sanitize_hwmon_chip(name), hwmon_dir_name),
                    None => hwmon_dir_name.clone(),
                });

            if let Some(chip_name) = &chip_name
                && config.should_include_metric("node_hwmon_chip_names")
            {
                metrics.push(CollectedMetric {
                    name: "node_hwmon_chip_names".to_string(),
                    value: 1.0,
                    labels: vec![
                        ("chip".to_string(), chip.clone()),
                        ("chip_name".to_string(), chip_name.clone()),
                    ],
                    metric_type: MetricType::Gauge,
                    unit: None,
                    description: Some(
                        "Annotation metric for human-readable chip names".to_string(),
                    ),
                });
            }

            // Scan for sensor files: temp*, in*, fan*, power* and curr* `_input` readings
            let dir_entries = match fs::read_dir(&hwmon_path) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for sensor_entry in dir_entries.flatten() {
                let fname = sensor_entry.file_name();
                let fname_str = fname.to_string_lossy();

                // Only process *_input files — these are the primary readings
                let Some(sensor_name) = fname_str.strip_suffix("_input") else {
                    continue;
                };

                // hwmon sensor files are named <kind><index>_input. Match on the alphabetic
                // prefix exactly rather than with `starts_with`, so that e.g. `intrusion0`
                // is not mistaken for a voltage input (`in`).
                let kind: &str = &sensor_name[..sensor_name
                    .find(|c: char| !c.is_ascii_alphabetic())
                    .unwrap_or(sensor_name.len())];

                let (metric_name, divisor, unit, desc) = match kind {
                    // temp1_input, temp2_input, etc. — millidegrees C
                    "temp" => (
                        "node_hwmon_temp_celsius",
                        1000.0,
                        "Cel",
                        "Hardware monitor temperature",
                    ),
                    // in0_input, in1_input, etc. — millivolts
                    "in" => (
                        "node_hwmon_in_volts",
                        1000.0,
                        "V",
                        "Hardware monitor voltage",
                    ),
                    // fan1_input, fan2_input, etc. — RPM (no conversion)
                    "fan" => (
                        "node_hwmon_fan_rpm",
                        1.0,
                        "{rev}/min",
                        "Hardware monitor fan speed",
                    ),
                    // power1_input, etc. — microwatts
                    "power" => (
                        "node_hwmon_power_watts",
                        1_000_000.0,
                        "W",
                        "Hardware monitor power consumption",
                    ),
                    // curr1_input, etc. — milliamps
                    "curr" => (
                        "node_hwmon_curr_amps",
                        1000.0,
                        "A",
                        "Hardware monitor current",
                    ),
                    _ => {
                        debug!(
                            "Skipping unsupported hwmon sensor {} on chip {}",
                            sensor_name, chip
                        );
                        continue;
                    }
                };

                // The threshold metrics below check their own names, so this sensor is only
                // skipped entirely when none of its metrics are wanted. Gating everything
                // on `metric_name` would make the thresholds unreachable whenever the
                // reading itself is filtered out.
                let want_reading = config.should_include_metric(metric_name);
                let want_thresholds = kind == "temp"
                    && (config.should_include_metric("node_hwmon_temp_max_celsius")
                        || config.should_include_metric("node_hwmon_temp_crit_celsius"));
                if !want_reading && !want_thresholds {
                    continue;
                }

                // The `sensor` label is the raw sysfs name (`temp1`, `in0`), which is unique
                // within a chip, matching Prometheus node_exporter. The human-readable text
                // from `<sensor>_label` is published separately as an annotation metric:
                // using it as the identity would collide whenever two sensors on one chip
                // carry the same text, and would move series whenever a driver gains or
                // changes a label file.
                let labels = vec![
                    ("chip".to_string(), chip.clone()),
                    ("sensor".to_string(), sensor_name.to_string()),
                ];

                let label_path = hwmon_path.join(format!("{}_label", sensor_name));
                if let Some(sensor_label) = fs::read_to_string(&label_path)
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    && config.should_include_metric("node_hwmon_sensor_label")
                {
                    metrics.push(CollectedMetric {
                        name: "node_hwmon_sensor_label".to_string(),
                        value: 1.0,
                        labels: vec![
                            ("chip".to_string(), chip.clone()),
                            ("sensor".to_string(), sensor_name.to_string()),
                            ("label".to_string(), sensor_label),
                        ],
                        metric_type: MetricType::Gauge,
                        unit: None,
                        description: Some(
                            "Annotation metric for human-readable sensor labels".to_string(),
                        ),
                    });
                }

                if want_reading
                    && let Ok(content) = fs::read_to_string(sensor_entry.path())
                    && let Ok(raw) = content.trim().parse::<f64>()
                {
                    metrics.push(CollectedMetric {
                        name: metric_name.to_string(),
                        value: raw / divisor,
                        labels: labels.clone(),
                        metric_type: MetricType::Gauge,
                        unit: Some(unit.to_string()),
                        description: Some(desc.to_string()),
                    });
                }

                // For temperature sensors, also read max and crit thresholds
                if kind == "temp" {
                    let max_path = hwmon_path.join(format!("{}_max", sensor_name));
                    if config.should_include_metric("node_hwmon_temp_max_celsius")
                        && let Ok(content) = fs::read_to_string(&max_path)
                        && let Ok(raw) = content.trim().parse::<f64>()
                    {
                        metrics.push(CollectedMetric {
                            name: "node_hwmon_temp_max_celsius".to_string(),
                            value: raw / 1000.0,
                            labels: labels.clone(),
                            metric_type: MetricType::Gauge,
                            unit: Some("Cel".to_string()),
                            description: Some(
                                "Hardware monitor temperature max threshold".to_string(),
                            ),
                        });
                    }

                    let crit_path = hwmon_path.join(format!("{}_crit", sensor_name));
                    if config.should_include_metric("node_hwmon_temp_crit_celsius")
                        && let Ok(content) = fs::read_to_string(&crit_path)
                        && let Ok(raw) = content.trim().parse::<f64>()
                    {
                        metrics.push(CollectedMetric {
                            name: "node_hwmon_temp_crit_celsius".to_string(),
                            value: raw / 1000.0,
                            labels,
                            metric_type: MetricType::Gauge,
                            unit: Some("Cel".to_string()),
                            description: Some(
                                "Hardware monitor temperature critical threshold".to_string(),
                            ),
                        });
                    }
                }
            }
        }

        metrics
    }
}

/// Make an hwmon chip identity safe to use as a metric label value.
///
/// Non-alphanumeric characters are replaced with `_`, following the same approach as
/// Prometheus `node_exporter`. This is a lossy mapping (`coretemp.0` and `coretemp-0`
/// both become `coretemp_0`), so it does not by itself guarantee uniqueness — a backing
/// device name is unique on its own, and the fallback identities compose the hwmon
/// directory to stay so. Values also differ from node_exporter's, which prefixes the
/// device's parent directory (`platform_coretemp_0` where this produces `coretemp_0`).
fn sanitize_hwmon_chip(device_name: &str) -> String {
    device_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::node_metrics::collector::test_support::*;
    use crate::receivers::node_metrics::config::Collector;
    use std::os::unix::fs::symlink;

    #[test]
    fn test_collect_cpufreq() {
        let sys = tempfile::tempdir().unwrap();
        write(
            sys.path(),
            "devices/system/cpu/cpu0/cpufreq/scaling_cur_freq",
            "2400000\n",
        );
        write(
            sys.path(),
            "devices/system/cpu/cpu0/cpufreq/scaling_min_freq",
            "800000\n",
        );
        write(
            sys.path(),
            "devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq",
            "3600000\n",
        );
        write(
            sys.path(),
            "devices/system/cpu/cpu1/cpufreq/scaling_cur_freq",
            "1200000\n",
        );
        // Siblings of the cpu<N> directories that must not be scanned as CPUs.
        write(sys.path(), "devices/system/cpu/cpufreq/boost", "1\n");
        write(
            sys.path(),
            "devices/system/cpu/cpuidle/current_driver",
            "none\n",
        );
        write(sys.path(), "devices/system/cpu/cpu_capacity", "1024\n");

        let (_procfs, collector) = collector_for_sysfs(sys.path());
        let metrics = collector.collect_cpufreq(&config_for(vec![Collector::Cpufreq]));

        assert_eq!(label_values(&metrics, "cpu"), ["0", "1"]);
        assert_eq!(metrics.len(), 4);

        // cpufreq reports kHz; the metric is Hz.
        let cur = find_one(
            &metrics,
            "node_cpu_scaling_frequency_hertz",
            &[("cpu", "0")],
        );
        assert_eq!(cur.value, 2_400_000_000.0);
        assert_eq!(cur.unit.as_deref(), Some("Hz"));
        assert_eq!(cur.metric_type, MetricType::Gauge);

        assert_eq!(
            find_one(
                &metrics,
                "node_cpu_scaling_frequency_min_hertz",
                &[("cpu", "0")]
            )
            .value,
            800_000_000.0
        );
        assert_eq!(
            find_one(&metrics, "node_cpu_frequency_max_hertz", &[("cpu", "0")]).value,
            3_600_000_000.0
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_cpu_scaling_frequency_hertz",
                &[("cpu", "1")]
            )
            .value,
            1_200_000_000.0
        );
    }

    #[test]
    fn test_collect_thermal_zone() {
        let sys = tempfile::tempdir().unwrap();
        write(
            sys.path(),
            "class/thermal/thermal_zone0/type",
            "x86_pkg_temp\n",
        );
        write(sys.path(), "class/thermal/thermal_zone0/temp", "45000\n");
        write(
            sys.path(),
            "class/thermal/cooling_device0/type",
            "Processor\n",
        );
        write(sys.path(), "class/thermal/cooling_device0/cur_state", "0\n");
        write(sys.path(), "class/thermal/cooling_device0/max_state", "3\n");

        let (_procfs, collector) = collector_for_sysfs(sys.path());
        let metrics = collector.collect_thermal_zone(&config_for(vec![Collector::ThermalZone]));

        // Millidegrees Celsius in the file, degrees Celsius in the metric.
        let temp = find_one(
            &metrics,
            "node_thermal_zone_temp",
            &[("zone", "0"), ("type", "x86_pkg_temp")],
        );
        assert_eq!(temp.value, 45.0);
        assert_eq!(temp.unit.as_deref(), Some("Cel"));
        assert_eq!(temp.metric_type, MetricType::Gauge);

        let cooling = [("name", "0"), ("type", "Processor")];
        assert_eq!(
            find_one(&metrics, "node_cooling_device_cur_state", &cooling).value,
            0.0
        );
        assert_eq!(
            find_one(&metrics, "node_cooling_device_max_state", &cooling).value,
            3.0
        );
        assert_eq!(metrics.len(), 3);
    }

    #[test]
    fn test_collect_nvme() {
        let sys = tempfile::tempdir().unwrap();
        write(
            sys.path(),
            "class/nvme/nvme0/model",
            "Samsung SSD 980 PRO 1TB\n",
        );
        write(sys.path(), "class/nvme/nvme0/serial", "S5GXNX0R123456\n");
        write(sys.path(), "class/nvme/nvme0/firmware_rev", "5B2QGXA7\n");
        write(sys.path(), "class/nvme/nvme0/state", "live\n");
        // Entries that are not controllers: a namespace directory, and a plain file.
        // Both carry none of the controller attributes, so accepting them would emit
        // an info metric whose five labels are all empty.
        write(sys.path(), "class/nvme/nvme0n1/size", "1953525168\n");
        write(sys.path(), "class/nvme/uevent", "\n");

        let (_procfs, collector) = collector_for_sysfs(sys.path());
        let metrics = collector.collect_nvme(&config_for(vec![Collector::Nvme]));

        assert_eq!(metrics.len(), 1);
        assert_eq!(label_values(&metrics, "device"), ["nvme0"]);
        let info = &metrics[0];
        assert_eq!(info.name, "node_nvme_info");
        assert_eq!(info.value, 1.0);
        assert_eq!(info.metric_type, MetricType::Gauge);
        assert_eq!(info.unit, None);
        assert_eq!(label(info, "device"), "nvme0");
        assert_eq!(label(info, "model"), "Samsung SSD 980 PRO 1TB");
        assert_eq!(label(info, "serial"), "S5GXNX0R123456");
        assert_eq!(label(info, "firmware_revision"), "5B2QGXA7");
        assert_eq!(label(info, "state"), "live");

        // The label set is exactly these five keys, so a stray entry cannot hide as
        // an extra series with the same name.
        let mut keys: Vec<&str> = info.labels.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["device", "firmware_revision", "model", "serial", "state"]
        );
    }

    #[test]
    fn test_collect_hwmon() {
        let sys = tempfile::tempdir().unwrap();
        write(sys.path(), "class/hwmon/hwmon0/name", "coretemp\n");
        write(sys.path(), "class/hwmon/hwmon0/temp1_input", "42000\n");
        write(
            sys.path(),
            "class/hwmon/hwmon0/temp1_label",
            "Package id 0\n",
        );
        write(sys.path(), "class/hwmon/hwmon0/temp1_max", "84000\n");
        write(sys.path(), "class/hwmon/hwmon0/temp1_crit", "100000\n");
        // No label file at all.
        write(sys.path(), "class/hwmon/hwmon0/temp2_input", "41000\n");
        // Present but empty label file.
        write(sys.path(), "class/hwmon/hwmon0/temp3_input", "40000\n");
        write(sys.path(), "class/hwmon/hwmon0/temp3_label", "\n");
        write(sys.path(), "class/hwmon/hwmon0/in0_input", "1200\n");
        write(sys.path(), "class/hwmon/hwmon0/fan1_input", "1500\n");
        write(sys.path(), "class/hwmon/hwmon0/power1_input", "7500000\n");
        write(sys.path(), "class/hwmon/hwmon0/curr1_input", "850\n");
        // Not a voltage input, despite sharing the "in" prefix.
        write(sys.path(), "class/hwmon/hwmon0/intrusion0_input", "0\n");
        symlink(
            "../../devices/platform/coretemp.0",
            sys.path().join("class/hwmon/hwmon0/device"),
        )
        .unwrap();

        let (_procfs, collector) = collector_for_sysfs(sys.path());
        let metrics = collector.collect_hwmon(&config_for(vec![Collector::Hwmon]));

        // The chip identity comes from the backing device, sanitised for use as a label.
        let chip = "coretemp_0";
        assert_eq!(label_values(&metrics, "chip"), [chip]);

        let names = find_one(
            &metrics,
            "node_hwmon_chip_names",
            &[("chip", chip), ("chip_name", "coretemp")],
        );
        assert_eq!(names.value, 1.0);
        // Info-style annotation metrics carry no unit, like node_nvme_info/node_uname_info
        assert_eq!(names.unit, None);

        // Temperatures, voltages and currents are milli-units; power is micro;
        // fan speed is already RPM.
        let temp1 = find_one(
            &metrics,
            "node_hwmon_temp_celsius",
            &[("chip", chip), ("sensor", "temp1")],
        );
        assert_eq!(temp1.value, 42.0);
        assert_eq!(temp1.unit.as_deref(), Some("Cel"));
        assert_eq!(temp1.metric_type, MetricType::Gauge);

        assert_eq!(
            find_one(
                &metrics,
                "node_hwmon_temp_max_celsius",
                &[("chip", chip), ("sensor", "temp1")]
            )
            .value,
            84.0
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_hwmon_temp_crit_celsius",
                &[("chip", chip), ("sensor", "temp1")]
            )
            .value,
            100.0
        );

        let volts = find_one(&metrics, "node_hwmon_in_volts", &[("sensor", "in0")]);
        assert_eq!(volts.value, 1.2);
        assert_eq!(volts.unit.as_deref(), Some("V"));

        let fan = find_one(&metrics, "node_hwmon_fan_rpm", &[("sensor", "fan1")]);
        assert_eq!(fan.value, 1500.0);
        assert_eq!(fan.unit.as_deref(), Some("{rev}/min"));

        let power = find_one(&metrics, "node_hwmon_power_watts", &[("sensor", "power1")]);
        assert_eq!(power.value, 7.5);
        assert_eq!(power.unit.as_deref(), Some("W"));

        let curr = find_one(&metrics, "node_hwmon_curr_amps", &[("sensor", "curr1")]);
        assert_eq!(curr.value, 0.85);
        assert_eq!(curr.unit.as_deref(), Some("A"));

        // Sensors are identified by their raw sysfs name whether or not they carry a label
        // file, so two sensors on one chip can never collide.
        assert_eq!(
            find_one(&metrics, "node_hwmon_temp_celsius", &[("sensor", "temp2")]).value,
            41.0
        );
        assert_eq!(
            find_one(&metrics, "node_hwmon_temp_celsius", &[("sensor", "temp3")]).value,
            40.0
        );
        assert!(matching(&metrics, "node_hwmon_temp_celsius", &[("sensor", "")]).is_empty());

        // The human-readable text is published separately, keyed by chip and sensor, as
        // Prometheus node_exporter does. Only the sensor that has a non-empty label file
        // gets one.
        let sensor_label = find_one(
            &metrics,
            "node_hwmon_sensor_label",
            &[
                ("chip", chip),
                ("sensor", "temp1"),
                ("label", "Package id 0"),
            ],
        );
        assert_eq!(sensor_label.value, 1.0);
        assert_eq!(sensor_label.unit, None);
        // ...and only that one sensor gets an annotation
        let labelled: Vec<String> = matching(&metrics, "node_hwmon_sensor_label", &[])
            .iter()
            .map(|m| label(m, "sensor").to_string())
            .collect();
        assert_eq!(labelled, ["temp1"]);

        // intrusion0_input is not a supported sensor kind and is skipped.
        assert_eq!(matching(&metrics, "node_hwmon_in_volts", &[]).len(), 1);
    }

    #[test]
    fn test_collect_hwmon_same_model_chips_get_distinct_labels() {
        let sys = tempfile::tempdir().unwrap();
        for (hwmon, device) in [("hwmon1", "nvme0"), ("hwmon2", "nvme1")] {
            write(sys.path(), &format!("class/hwmon/{}/name", hwmon), "nvme\n");
            write(
                sys.path(),
                &format!("class/hwmon/{}/temp1_input", hwmon),
                "38000\n",
            );
            symlink(
                format!("../../devices/pci0000:00/0000:00:1d.0/nvme/{}", device),
                sys.path().join(format!("class/hwmon/{}/device", hwmon)),
            )
            .unwrap();
        }

        let (_procfs, collector) = collector_for_sysfs(sys.path());
        let metrics = collector.collect_hwmon(&config_for(vec![Collector::Hwmon]));

        // Two chips of the same model must not collapse onto one attribute set:
        // the `chip` label identifies the instance, `chip_name` the model.
        assert_eq!(label_values(&metrics, "chip"), ["nvme0", "nvme1"]);
        assert_eq!(
            matching(&metrics, "node_hwmon_temp_celsius", &[("sensor", "temp1")]).len(),
            2
        );
        for chip in ["nvme0", "nvme1"] {
            assert_eq!(
                find_one(
                    &metrics,
                    "node_hwmon_temp_celsius",
                    &[("chip", chip), ("sensor", "temp1")]
                )
                .value,
                38.0
            );
            find_one(
                &metrics,
                "node_hwmon_chip_names",
                &[("chip", chip), ("chip_name", "nvme")],
            );
        }
    }

    /// The three ways a `chip` identity can be derived, in preference order.
    ///
    /// Only the first is exercised elsewhere; the fallbacks are what a host without
    /// `device` symlinks (embedded platform drivers) actually takes.
    /// The threshold metrics check their own names, so excluding the base reading must not
    /// make them unreachable — an operator trimming cardinality still wants the limits.
    #[test]
    fn test_collect_hwmon_thresholds_survive_excluding_the_reading() {
        let sys = tempfile::tempdir().unwrap();
        let hwmon = sys.path().join("class/hwmon/hwmon0");
        std::fs::create_dir_all(&hwmon).unwrap();
        std::fs::write(hwmon.join("name"), "coretemp\n").unwrap();
        std::fs::write(hwmon.join("temp1_input"), "42000\n").unwrap();
        std::fs::write(hwmon.join("temp1_max"), "84000\n").unwrap();
        std::fs::write(hwmon.join("temp1_crit"), "100000\n").unwrap();

        let (_procfs, collector) = collector_for_sysfs(sys.path());
        let config = NodeMetricsConfig {
            collectors: vec![Collector::Hwmon],
            sysfs_path: sys.path().to_str().unwrap().to_string(),
            ..Default::default()
        }
        .with_exclude_filter("^node_hwmon_temp_celsius$")
        .unwrap();

        let metrics = collector.collect_hwmon(&config);

        assert!(matching(&metrics, "node_hwmon_temp_celsius", &[]).is_empty());
        assert_eq!(
            find_one(&metrics, "node_hwmon_temp_max_celsius", &[]).value,
            84.0
        );
        assert_eq!(
            find_one(&metrics, "node_hwmon_temp_crit_celsius", &[]).value,
            100.0
        );
    }

    #[test]
    fn test_collect_hwmon_chip_identity_fallback_chain() {
        let sys = tempfile::tempdir().unwrap();

        // hwmon0: a `device` symlink, so the chip is the backing device name.
        write(sys.path(), "class/hwmon/hwmon0/name", "coretemp\n");
        write(sys.path(), "class/hwmon/hwmon0/temp1_input", "42000\n");
        symlink(
            "../../devices/platform/coretemp.0",
            sys.path().join("class/hwmon/hwmon0/device"),
        )
        .unwrap();

        // hwmon1: no symlink, but a name file, so the chip combines the two.
        write(sys.path(), "class/hwmon/hwmon1/name", "bq27xxx-battery\n");
        write(sys.path(), "class/hwmon/hwmon1/temp1_input", "38000\n");

        // hwmon2: neither, so only the directory name is left to identify it.
        write(sys.path(), "class/hwmon/hwmon2/temp1_input", "39000\n");

        // hwmon3: the same model name as hwmon1 and no symlink either — the case that
        // a name-only identity would collapse onto one attribute set.
        write(sys.path(), "class/hwmon/hwmon3/name", "bq27xxx-battery\n");
        write(sys.path(), "class/hwmon/hwmon3/temp1_input", "40000\n");

        let (_procfs, collector) = collector_for_sysfs(sys.path());
        let metrics = collector.collect_hwmon(&config_for(vec![Collector::Hwmon]));

        // Four chips, four distinct identities: the device name where there is one, the
        // sanitised model name plus the hwmonN directory where there is a name file, and
        // the bare directory name where there is neither.
        assert_eq!(
            label_values(&metrics, "chip"),
            [
                "bq27xxx_battery_hwmon1",
                "bq27xxx_battery_hwmon3",
                "coretemp_0",
                "hwmon2"
            ]
        );

        for (chip, temp) in [
            ("coretemp_0", 42.0),
            ("bq27xxx_battery_hwmon1", 38.0),
            ("hwmon2", 39.0),
            ("bq27xxx_battery_hwmon3", 40.0),
        ] {
            let reading = find_one(
                &metrics,
                "node_hwmon_temp_celsius",
                &[("chip", chip), ("sensor", "temp1")],
            );
            assert_eq!(reading.value, temp, "chip {}", chip);
            assert_eq!(reading.unit.as_deref(), Some("Cel"));
            assert_eq!(reading.metric_type, MetricType::Gauge);
        }

        // The invariant that matters: two chips reporting the same model with no
        // `device` symlink must still be distinguishable. Identical `chip` labels would
        // make them one duplicate attribute set, and one chip's readings would be
        // dropped in the OTLP conversion.
        let same_model: Vec<&str> = metrics
            .iter()
            .filter(|m| m.name == "node_hwmon_chip_names")
            .filter(|m| label(m, "chip_name") == "bq27xxx-battery")
            .map(|m| label(m, "chip"))
            .collect();
        assert_eq!(same_model.len(), 2, "expected both same-model chips");
        assert_ne!(
            same_model[0], same_model[1],
            "two chips of the same model must not share a chip label"
        );

        // The model name is only published where one exists; hwmon2 has no name file,
        // so it gets no annotation metric — but its readings are still reported above.
        let mut chip_names: Vec<&str> = metrics
            .iter()
            .filter(|m| m.name == "node_hwmon_chip_names")
            .map(|m| label(m, "chip_name"))
            .collect();
        chip_names.sort_unstable();
        chip_names.dedup();
        assert_eq!(chip_names, ["bq27xxx-battery", "coretemp"]);
        assert!(matching(&metrics, "node_hwmon_chip_names", &[("chip", "hwmon2")]).is_empty());
    }
}
