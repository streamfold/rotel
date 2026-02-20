// SPDX-License-Identifier: Apache-2.0

//! Convert collected system metrics to OTLP ResourceMetrics format

use crate::receivers::node_metrics::collector::{CollectedMetric, MetricType};
use gethostname::gethostname;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

const SCOPE_NAME: &str = "node_metrics";

// Resource attributes
const SERVICE_NAME_KEY: &str = "service.name";
const HOST_NAME_KEY: &str = "host.name";
#[cfg(target_os = "linux")]
const OS_TYPE_KEY: &str = "os.type";
#[cfg(target_os = "linux")]
const OS_TYPE_VALUE: &str = "linux";

/// Convert a batch of collected metrics to OTLP ResourceMetrics
///
/// `boot_time_secs` is the system boot time in seconds since epoch, used as
/// the start time for cumulative counters.
///
/// `service_name` is the value set on the `service.name` resource attribute.
pub fn convert_to_otlp_metrics(
    metrics: Vec<CollectedMetric>,
    boot_time_secs: u64,
    service_name: &str,
) -> ResourceMetrics {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let now_ns = now.as_secs() * 1_000_000_000 + now.subsec_nanos() as u64;

    let boot_time_ns = boot_time_secs.saturating_mul(1_000_000_000);

    // Group metrics by name *and* type so each OTLP Metric contains all its data points.
    // Keying on the type as well keeps a gauge and a counter that happen to share a name
    // (reachable via textfiles, whose `# TYPE` scope is per file) as two separate
    // metrics, rather than silently stamping one group's points with the other's
    // temporality and start time.
    // BTreeMap gives deterministic (alphabetical) ordering across scrapes,
    // which simplifies debugging and diffing.
    let mut grouped: BTreeMap<(String, MetricType), Vec<CollectedMetric>> = BTreeMap::new();
    for mut m in metrics {
        // The name moves into the key; the grouped metrics keep an empty `name` field,
        // which nothing below reads (only labels, value, unit and description are used).
        let key = (std::mem::take(&mut m.name), m.metric_type);
        grouped.entry(key).or_default().push(m);
    }

    // A name that appears with two types stays two OTLP metrics, which a backend keying
    // on resource+scope+name still treats as a conflict. Surface it rather than letting
    // it be resolved arbitrarily downstream.
    let mut previous: Option<&String> = None;
    for (name, _) in grouped.keys() {
        if previous == Some(name) {
            // Debug, not warn: a conflicting `# TYPE` persists until the file is fixed,
            // so warning would log this on every scrape forever. The same reasoning the
            // textfile collector applies to its own repeating failures.
            tracing::debug!(
                "Metric {} is reported as both a gauge and a counter; check for a \
                 conflicting `# TYPE` in the textfile directory",
                name
            );
        }
        previous = Some(name);
    }

    let otlp_metrics: Vec<Metric> = grouped
        .into_iter()
        .map(|((name, metric_type), group)| {
            convert_metric_group(name, metric_type, group, now_ns, boot_time_ns)
        })
        .collect();

    ResourceMetrics {
        resource: Some(build_resource(service_name)),
        scope_metrics: vec![ScopeMetrics {
            scope: Some(
                opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: SCOPE_NAME.to_string(),
                    version: String::new(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                },
            ),
            metrics: otlp_metrics,
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// Count the data points in a converted batch
///
/// Every variant is matched explicitly: a silent `_ => 0` would under-count without any
/// compile error if a new metric shape were ever emitted here.
pub(crate) fn count_data_points(resource_metrics: &ResourceMetrics) -> usize {
    resource_metrics
        .scope_metrics
        .iter()
        .flat_map(|sm| sm.metrics.iter())
        .map(|m| match &m.data {
            Some(metric::Data::Gauge(g)) => g.data_points.len(),
            Some(metric::Data::Sum(s)) => s.data_points.len(),
            Some(metric::Data::Histogram(h)) => h.data_points.len(),
            Some(metric::Data::ExponentialHistogram(h)) => h.data_points.len(),
            Some(metric::Data::Summary(s)) => s.data_points.len(),
            None => 0,
        })
        .sum()
}

/// Build the OTLP Resource identifying the host these metrics describe
///
/// `host.name` matters here: without it, the same series from different hosts share an
/// identical identity at the backend. Mirrors the kmsg receiver's resource attributes.
fn build_resource(service_name: &str) -> Resource {
    let mut attributes = vec![string_attribute(SERVICE_NAME_KEY, service_name.to_string())];

    if let Ok(hostname) = gethostname().into_string() {
        attributes.push(string_attribute(HOST_NAME_KEY, hostname));
    }

    #[cfg(target_os = "linux")]
    attributes.push(string_attribute(OS_TYPE_KEY, OS_TYPE_VALUE.to_string()));

    Resource {
        attributes,
        dropped_attributes_count: 0,
        entity_refs: vec![],
    }
}

/// Build a string-valued OTLP attribute
fn string_attribute(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),

        key_strindex: 0,
    }
}

/// Convert a group of collected metrics (same name and type) to a single OTLP Metric
fn convert_metric_group(
    name: String,
    metric_type: MetricType,
    group: Vec<CollectedMetric>,
    time_unix_nano: u64,
    boot_time_nano: u64,
) -> Metric {
    // Take the remaining metadata from the first metric in the group
    let (description, unit) = match group.first() {
        Some(first) => (
            first.description.clone().unwrap_or_default(),
            first.unit.clone().unwrap_or_default(),
        ),
        None => (String::new(), String::new()),
    };

    // For counters, start_time is the boot time (when counting began).
    // For gauges, start_time is not applicable (left unset).
    let start_time = match metric_type {
        MetricType::Counter => boot_time_nano,
        MetricType::Gauge => 0,
    };

    // Duplicate attribute sets within one metric violate the OTLP single-writer
    // principle and are rejected (or arbitrarily resolved) by backends. They are
    // reachable via textfiles and duplicated mount entries, so drop repeats here.
    let mut seen: HashSet<Vec<(String, String)>> = HashSet::with_capacity(group.len());
    let mut dropped = 0usize;
    let mut data_points: Vec<NumberDataPoint> = Vec::with_capacity(group.len());

    for mut m in group {
        // Compare label *sets*: `{a="1",b="2"}` and `{b="2",a="1"}` are the same
        // OTLP identity, and textfile labels arrive in whatever order the file used.
        // Sorting in place also makes the emitted attribute order stable across scrapes.
        m.labels.sort_unstable();
        if !seen.insert(m.labels.clone()) {
            dropped += 1;
            continue;
        }

        let attributes: Vec<KeyValue> = m
            .labels
            .into_iter()
            .map(|(k, v)| KeyValue {
                key: k,
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(v)),
                }),

                key_strindex: 0,
            })
            .collect();

        data_points.push(NumberDataPoint {
            attributes,
            start_time_unix_nano: start_time,
            time_unix_nano,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(m.value)),
        });
    }

    if dropped > 0 {
        // Debug for the same reason: duplicated mounts or a bad `.prom` file do not
        // self-heal, so this would repeat every scrape.
        tracing::debug!(
            "Dropped {} duplicate data point(s) for metric {}",
            dropped,
            name
        );
    }

    let data = match metric_type {
        MetricType::Gauge => {
            opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge { data_points })
        }
        MetricType::Counter => opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(Sum {
            data_points,
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        }),
    };

    Metric {
        name,
        description,
        unit,
        metadata: vec![],
        data: Some(data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_metric(
        name: &str,
        value: f64,
        metric_type: MetricType,
        labels: Vec<(String, String)>,
    ) -> CollectedMetric {
        CollectedMetric {
            name: name.to_string(),
            value,
            labels,
            metric_type,
            unit: Some("By".to_string()),
            description: Some("Test metric".to_string()),
        }
    }

    #[test]
    fn test_convert_gauge_metric() {
        let metric = make_test_metric("node_load1", 1.5, MetricType::Gauge, vec![]);

        let resource_metrics = convert_to_otlp_metrics(vec![metric], 1234567890, "node_metrics");

        assert!(resource_metrics.resource.is_some());
        assert_eq!(resource_metrics.scope_metrics.len(), 1);
        assert_eq!(resource_metrics.scope_metrics[0].metrics.len(), 1);

        let otlp_metric = &resource_metrics.scope_metrics[0].metrics[0];
        assert_eq!(otlp_metric.name, "node_load1");

        match &otlp_metric.data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge)) => {
                assert_eq!(gauge.data_points.len(), 1);
                match gauge.data_points[0].value {
                    Some(number_data_point::Value::AsDouble(v)) => {
                        assert!((v - 1.5).abs() < f64::EPSILON);
                    }
                    _ => panic!("Expected double value"),
                }
            }
            _ => panic!("Expected Gauge data"),
        }
    }

    #[test]
    fn test_convert_counter_metric() {
        let metric = make_test_metric(
            "node_network_receive_bytes_total",
            1000.0,
            MetricType::Counter,
            vec![],
        );

        let resource_metrics = convert_to_otlp_metrics(vec![metric], 1234567890, "node_metrics");

        let otlp_metric = &resource_metrics.scope_metrics[0].metrics[0];
        assert_eq!(otlp_metric.name, "node_network_receive_bytes_total");

        match &otlp_metric.data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) => {
                assert!(sum.is_monotonic);
                assert_eq!(
                    sum.aggregation_temporality,
                    AggregationTemporality::Cumulative as i32
                );
            }
            _ => panic!("Expected Sum data"),
        }
    }

    #[test]
    fn test_convert_metric_with_labels() {
        let labels = vec![("device".to_string(), "eth0".to_string())];

        let metric = make_test_metric(
            "node_network_receive_bytes_total",
            1000.0,
            MetricType::Counter,
            labels,
        );

        let resource_metrics = convert_to_otlp_metrics(vec![metric], 1234567890, "node_metrics");

        let otlp_metric = &resource_metrics.scope_metrics[0].metrics[0];

        match &otlp_metric.data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) => {
                assert_eq!(sum.data_points[0].attributes.len(), 1);
                assert_eq!(sum.data_points[0].attributes[0].key, "device");
                // The value has to survive the conversion too: a label whose key is
                // carried but whose value is dropped would still pass a key-only check.
                assert_eq!(
                    attribute_pairs(&sum.data_points[0].attributes),
                    vec![("device", "eth0")]
                );
            }
            _ => panic!("Expected Sum data"),
        }
    }

    #[test]
    fn test_convert_multiple_metrics_same_name_grouped() {
        // Multiple data points for the same metric name should be grouped
        let metrics = vec![
            make_test_metric(
                "node_cpu_seconds_total",
                10.0,
                MetricType::Counter,
                vec![
                    ("cpu".to_string(), "0".to_string()),
                    ("mode".to_string(), "user".to_string()),
                ],
            ),
            make_test_metric(
                "node_cpu_seconds_total",
                20.0,
                MetricType::Counter,
                vec![
                    ("cpu".to_string(), "0".to_string()),
                    ("mode".to_string(), "system".to_string()),
                ],
            ),
        ];

        let resource_metrics = convert_to_otlp_metrics(metrics, 1234567890, "node_metrics");

        // Should produce a single Metric with 2 data points
        assert_eq!(resource_metrics.scope_metrics[0].metrics.len(), 1);
        match &resource_metrics.scope_metrics[0].metrics[0].data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) => {
                assert_eq!(sum.data_points.len(), 2);
                // Which two points survive matters as much as how many: grouping that
                // kept one point twice, or swapped a value onto the wrong attribute
                // set, would still leave the count at 2.
                assert_eq!(
                    point_summaries(&sum.data_points),
                    vec![
                        (vec![("cpu", "0"), ("mode", "user")], 10.0),
                        (vec![("cpu", "0"), ("mode", "system")], 20.0),
                    ]
                );
            }
            _ => panic!("Expected Sum data"),
        }
    }

    #[test]
    fn test_resource_has_service_name() {
        let metric = make_test_metric("node_load1", 1.0, MetricType::Gauge, vec![]);

        let resource_metrics =
            convert_to_otlp_metrics(vec![metric], 1234567890, "my_custom_service");

        let resource = resource_metrics.resource.unwrap();

        assert_eq!(
            attribute_value(&resource, "service.name").as_deref(),
            Some("my_custom_service")
        );

        // host.name must be present so series from different hosts stay distinguishable.
        // The value itself is host-dependent and may be empty in a minimal container.
        assert!(
            resource.attributes.iter().any(|a| a.key == "host.name"),
            "expected a host.name resource attribute, got {:?}",
            resource.attributes
        );

        #[cfg(target_os = "linux")]
        assert_eq!(
            attribute_value(&resource, "os.type").as_deref(),
            Some("linux")
        );
    }

    /// The `(key, value)` pairs of a data point's attributes, in emitted order
    fn attribute_pairs(attributes: &[KeyValue]) -> Vec<(&str, &str)> {
        attributes
            .iter()
            .map(|kv| {
                let value = match &kv.value {
                    Some(AnyValue {
                        value: Some(any_value::Value::StringValue(v)),
                    }) => v.as_str(),
                    other => panic!(
                        "expected a string attribute for {}, got {:?}",
                        kv.key, other
                    ),
                };
                (kv.key.as_str(), value)
            })
            .collect()
    }

    /// The `(attributes, value)` of every data point, in emitted order
    fn point_summaries(points: &[NumberDataPoint]) -> Vec<(Vec<(&str, &str)>, f64)> {
        points
            .iter()
            .map(|p| {
                let value = match p.value {
                    Some(number_data_point::Value::AsDouble(v)) => v,
                    other => panic!("expected a double data point value, got {:?}", other),
                };
                (attribute_pairs(&p.attributes), value)
            })
            .collect()
    }

    /// The data points of the single metric in a converted batch
    fn only_data_points(resource_metrics: &ResourceMetrics) -> &[NumberDataPoint] {
        let metrics = &resource_metrics.scope_metrics[0].metrics;
        assert_eq!(metrics.len(), 1, "expected exactly one metric");
        match &metrics[0].data {
            Some(metric::Data::Gauge(gauge)) => &gauge.data_points,
            Some(metric::Data::Sum(sum)) => &sum.data_points,
            other => panic!("expected gauge or sum data, got {:?}", other),
        }
    }

    /// Look up a string-valued resource attribute by key
    fn attribute_value(resource: &Resource, key: &str) -> Option<String> {
        resource
            .attributes
            .iter()
            .find(|a| a.key == key)
            .and_then(|a| match &a.value {
                Some(AnyValue {
                    value: Some(any_value::Value::StringValue(v)),
                }) => Some(v.clone()),
                _ => None,
            })
    }

    #[test]
    fn test_same_name_gauge_and_counter_stay_separate() {
        // Reachable through textfiles, whose `# TYPE` scope is per file: the two must not
        // be merged into one metric with a single temporality.
        let gauge = make_test_metric("collision", 1.0, MetricType::Gauge, vec![]);
        let counter = make_test_metric("collision", 2.0, MetricType::Counter, vec![]);

        let resource_metrics = convert_to_otlp_metrics(vec![gauge, counter], 1000, "node_metrics");
        let metrics = &resource_metrics.scope_metrics[0].metrics;

        assert_eq!(metrics.len(), 2, "expected separate gauge and sum metrics");
        assert!(metrics.iter().all(|m| m.name == "collision"));

        let has_gauge = metrics.iter().any(|m| {
            matches!(
                m.data,
                Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(_))
            )
        });
        let sum = metrics.iter().find_map(|m| match &m.data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(s)) => Some(s),
            _ => None,
        });

        assert!(has_gauge, "gauge variant missing");
        let sum = sum.expect("sum variant missing");
        // The counter's points keep the boot-time start, the gauge's stay unset
        assert_eq!(
            sum.data_points[0].start_time_unix_nano,
            1000 * 1_000_000_000
        );
    }

    #[test]
    fn test_duplicate_attribute_sets_are_dropped() {
        // Two samples with identical label sets would violate the OTLP single-writer
        // principle and can be rejected wholesale by backends.
        let labels = vec![("device".to_string(), "sda".to_string())];
        let first = make_test_metric("node_disk_io_now", 1.0, MetricType::Gauge, labels.clone());
        let duplicate = make_test_metric("node_disk_io_now", 2.0, MetricType::Gauge, labels);
        let other = make_test_metric(
            "node_disk_io_now",
            3.0,
            MetricType::Gauge,
            vec![("device".to_string(), "sdb".to_string())],
        );

        let resource_metrics =
            convert_to_otlp_metrics(vec![first, duplicate, other], 1000, "node_metrics");
        let metric = &resource_metrics.scope_metrics[0].metrics[0];

        match &metric.data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge)) => {
                assert_eq!(gauge.data_points.len(), 2);
                // First occurrence wins, and the point that is *not* a duplicate keeps
                // its own value: dropping `sdb` instead would also leave two points.
                assert_eq!(
                    point_summaries(&gauge.data_points),
                    vec![
                        (vec![("device", "sda")], 1.0),
                        (vec![("device", "sdb")], 3.0),
                    ]
                );
            }
            _ => panic!("Expected Gauge data"),
        }
    }

    #[test]
    fn test_duplicate_attribute_sets_are_detected_regardless_of_label_order() {
        // The same OTLP identity written with its labels in two different orders, as
        // textfile lines and repeated mount entries can produce. Comparing the label
        // vectors as given would treat these as two distinct series, so both would be
        // emitted and the backend would reject or arbitrarily resolve them.
        let unsorted = make_test_metric(
            "node_disk_io_now",
            1.0,
            MetricType::Gauge,
            vec![
                ("b".to_string(), "2".to_string()),
                ("a".to_string(), "1".to_string()),
            ],
        );
        let sorted = make_test_metric(
            "node_disk_io_now",
            2.0,
            MetricType::Gauge,
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ],
        );

        let resource_metrics =
            convert_to_otlp_metrics(vec![unsorted, sorted], 1000, "node_metrics");
        let points = only_data_points(&resource_metrics);

        // One point survives, and its attributes are emitted in sorted order: the
        // in-place sort that makes the comparison order-insensitive also normalises
        // what is exported, so the attribute order is stable across scrapes.
        assert_eq!(
            point_summaries(points),
            vec![(vec![("a", "1"), ("b", "2")], 1.0)]
        );
    }

    #[test]
    fn test_count_data_points_counts_exported_points_not_collected_ones() {
        // A duplicate label set is dropped during conversion, so the collected count
        // over-reports what was actually exported.
        let labels = vec![("device".to_string(), "sda".to_string())];
        let collected = vec![
            make_test_metric("node_disk_io_now", 1.0, MetricType::Gauge, labels.clone()),
            make_test_metric("node_disk_io_now", 2.0, MetricType::Gauge, labels),
            make_test_metric(
                "node_disk_io_now",
                3.0,
                MetricType::Gauge,
                vec![("device".to_string(), "sdb".to_string())],
            ),
            // A counter as well, so the Sum arm of the count is exercised too.
            make_test_metric(
                "node_disk_reads_completed_total",
                4.0,
                MetricType::Counter,
                vec![("device".to_string(), "sda".to_string())],
            ),
        ];
        let collected_count = collected.len();

        let resource_metrics = convert_to_otlp_metrics(collected, 1000, "node_metrics");

        // Two gauge points (the duplicate dropped) plus one counter point.
        assert_eq!(count_data_points(&resource_metrics), 3);
        assert_ne!(
            count_data_points(&resource_metrics),
            collected_count,
            "the point count must reflect the conversion's drops"
        );

        // Cross-check against the emitted metrics rather than a hard-coded number.
        let emitted: usize = resource_metrics.scope_metrics[0]
            .metrics
            .iter()
            .map(|m| match &m.data {
                Some(metric::Data::Gauge(g)) => g.data_points.len(),
                Some(metric::Data::Sum(s)) => s.data_points.len(),
                other => panic!("unexpected metric data {:?}", other),
            })
            .sum();
        assert_eq!(count_data_points(&resource_metrics), emitted);
    }

    #[test]
    fn test_count_data_points_of_an_empty_batch_is_zero() {
        let resource_metrics = convert_to_otlp_metrics(vec![], 1000, "node_metrics");
        assert_eq!(count_data_points(&resource_metrics), 0);
    }

    #[test]
    fn test_gauge_start_time_is_unset_and_metadata_propagates() {
        let metric = make_test_metric("node_load1", 1.5, MetricType::Gauge, vec![]);

        let resource_metrics = convert_to_otlp_metrics(vec![metric], 1234567890, "node_metrics");
        let otlp_metric = &resource_metrics.scope_metrics[0].metrics[0];

        // unit/description come from the collected metric
        assert_eq!(otlp_metric.unit, "By");
        assert_eq!(otlp_metric.description, "Test metric");

        let scope = resource_metrics.scope_metrics[0].scope.as_ref().unwrap();
        assert_eq!(scope.name, "node_metrics");

        match &otlp_metric.data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge)) => {
                assert_eq!(gauge.data_points[0].start_time_unix_nano, 0);
                assert!(gauge.data_points[0].time_unix_nano > 0);
            }
            _ => panic!("Expected Gauge data"),
        }
    }

    #[test]
    fn test_metrics_are_grouped_alphabetically() {
        let metrics = vec![
            make_test_metric("node_c", 1.0, MetricType::Gauge, vec![]),
            make_test_metric("node_a", 2.0, MetricType::Gauge, vec![]),
            make_test_metric("node_b", 3.0, MetricType::Gauge, vec![]),
        ];

        let resource_metrics = convert_to_otlp_metrics(metrics, 1000, "node_metrics");
        let names: Vec<&str> = resource_metrics.scope_metrics[0]
            .metrics
            .iter()
            .map(|m| m.name.as_str())
            .collect();

        assert_eq!(names, vec!["node_a", "node_b", "node_c"]);
    }
}
