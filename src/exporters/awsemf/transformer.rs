// SPDX-License-Identifier: Apache-2.0

use crate::exporters::awsemf::AwsEmfExporterConfig;
use crate::exporters::awsemf::request_builder::TransformPayload;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use serde_json::Error as JsonError;
use serde_json::{Value, json};
use std::time::Duration;
use std::{collections::HashMap, io};
use thiserror::Error;

#[derive(Clone)]
pub struct Transformer {
    config: AwsEmfExporterConfig,
    metric_transformer: MetricTransformer,
}

impl Transformer {
    pub fn new(_environment: String, config: AwsEmfExporterConfig) -> Self {
        Self {
            config: config.clone(),
            metric_transformer: MetricTransformer::new(config),
        }
    }
}

impl TransformPayload<ResourceMetrics> for Transformer {
    fn transform(&self, resource_metrics: Vec<ResourceMetrics>) -> Result<Vec<Value>, ExportError> {
        let mut grouped_metrics: HashMap<GroupKey, GroupedMetric> = HashMap::new();

        for rm in resource_metrics {
            let resource_attrs = extract_resource_attributes(&rm);

            for sm in rm.scope_metrics {
                for metric in sm.metrics {
                    self.metric_transformer.add_to_grouped_metrics(
                        &metric,
                        &resource_attrs,
                        &mut grouped_metrics,
                    )?;
                }
            }
        }

        // Convert grouped metrics to EMF logs
        let mut emf_logs = Vec::new();
        for (_, grouped_metric) in grouped_metrics {
            let emf_log = self
                .metric_transformer
                .translate_grouped_metric_to_emf(grouped_metric)?;
            emf_logs.push(emf_log);
        }

        Ok(emf_logs)
    }
}

#[derive(Clone)]
pub struct MetricTransformer {
    config: AwsEmfExporterConfig,
}

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("Timestamp error")]
    TimestampError,
    #[error("Exporter is shut down")]
    Shutdown,
    #[error("Export timeout after {0:?}")]
    Timeout(Duration),
    #[error("Serialization error: {0}")]
    Serialization(#[from] JsonError),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Export failed: {message}")]
    ExportFailed { message: String },
    #[error("Invalid metric type: {metric_type}")]
    InvalidMetricType { metric_type: String },
}

// Group key for organizing metrics with the same labels and metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    pub namespace: String,
    pub labels: Vec<(String, String)>, // Sorted key-value pairs for consistent hashing
    pub timestamp_ms: i64,
}

// Grouped metric containing multiple metrics with the same dimensions
#[derive(Debug, Clone)]
pub struct GroupedMetric {
    pub labels: HashMap<String, String>,
    pub metrics: HashMap<String, MetricInfo>,
    pub metadata: MetricMetadata,
}

// Individual metric information
#[derive(Debug, Clone)]
pub struct MetricInfo {
    pub value: MetricValue,
    pub unit: String,
}

// Metric value types
#[derive(Debug, Clone)]
pub enum MetricValue {
    Double(f64),
    Int(i64),
    Histogram {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
    Summary {
        count: u64,
        sum: f64,
        quantiles: Vec<(f64, f64)>, // (quantile, value) pairs
    },
}

// Metadata for grouped metrics
#[derive(Debug, Clone)]
pub struct MetricMetadata {
    pub namespace: String,
    pub timestamp_ms: i64,
}

impl MetricTransformer {
    pub fn new(config: AwsEmfExporterConfig) -> Self {
        Self { config }
    }

    pub fn add_to_grouped_metrics(
        &self,
        metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
        resource_attrs: &HashMap<String, String>,
        grouped_metrics: &mut HashMap<GroupKey, GroupedMetric>,
    ) -> Result<(), ExportError> {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;

        match &metric.data {
            Some(Data::Gauge(gauge_data)) => {
                for data_point in &gauge_data.data_points {
                    self.add_data_point_to_group(
                        &metric.name,
                        &metric.unit,
                        data_point,
                        resource_attrs,
                        grouped_metrics,
                        "gauge",
                    )?;
                }
            }
            Some(Data::Sum(sum_data)) => {
                for data_point in &sum_data.data_points {
                    self.add_data_point_to_group(
                        &metric.name,
                        &metric.unit,
                        data_point,
                        resource_attrs,
                        grouped_metrics,
                        "counter",
                    )?;
                }
            }
            Some(Data::Histogram(histogram_data)) => {
                for data_point in &histogram_data.data_points {
                    self.add_histogram_to_group(
                        &metric.name,
                        &metric.unit,
                        data_point,
                        resource_attrs,
                        grouped_metrics,
                    )?;
                }
            }
            Some(Data::Summary(summary_data)) => {
                for data_point in &summary_data.data_points {
                    self.add_summary_to_group(
                        &metric.name,
                        &metric.unit,
                        data_point,
                        resource_attrs,
                        grouped_metrics,
                    )?;
                }
            }
            _ => {
                return Err(ExportError::InvalidMetricType {
                    metric_type: format!("{:?}", metric.data),
                });
            }
        }

        Ok(())
    }

    fn add_data_point_to_group(
        &self,
        metric_name: &str,
        unit: &str,
        data_point: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
        resource_attrs: &HashMap<String, String>,
        grouped_metrics: &mut HashMap<GroupKey, GroupedMetric>,
        _metric_type: &str,
    ) -> Result<(), ExportError> {
        let labels = self.extract_dimensions_from_number_data_point(data_point, resource_attrs)?;
        let timestamp_ms = data_point.time_unix_nano as i64 / 1_000_000;

        let metric_value = match &data_point.value {
            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                val,
            )) => MetricValue::Double(*val),
            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(val)) => {
                MetricValue::Int(*val)
            }
            None => {
                return Err(ExportError::ExportFailed {
                    message: "No metric value found".to_string(),
                });
            }
        };

        let group_key = self.create_group_key(&labels, timestamp_ms);

        let metric_info = MetricInfo {
            value: metric_value,
            unit: self.translate_unit(unit),
        };

        // Get or create grouped metric
        let grouped_metric = grouped_metrics
            .entry(group_key)
            .or_insert_with(|| GroupedMetric {
                labels: labels.clone(),
                metrics: HashMap::new(),
                metadata: MetricMetadata {
                    namespace: self.config.namespace.clone(),
                    timestamp_ms,
                },
            });

        // Add metric to the group
        if grouped_metric.metrics.contains_key(metric_name) {
            // Log warning about duplicate metric (in Go implementation)
            eprintln!("Warning: Duplicate metric found: {}", metric_name);
        } else {
            grouped_metric
                .metrics
                .insert(metric_name.to_string(), metric_info);
        }

        Ok(())
    }

    fn add_histogram_to_group(
        &self,
        metric_name: &str,
        unit: &str,
        data_point: &opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
        resource_attrs: &HashMap<String, String>,
        grouped_metrics: &mut HashMap<GroupKey, GroupedMetric>,
    ) -> Result<(), ExportError> {
        let labels =
            self.extract_dimensions_from_histogram_data_point(data_point, resource_attrs)?;
        let timestamp_ms = data_point.time_unix_nano as i64 / 1_000_000;

        let metric_value = MetricValue::Histogram {
            count: data_point.count,
            sum: data_point.sum.unwrap_or(0.0),
            min: data_point.min.unwrap_or(0.0),
            max: data_point.max.unwrap_or(0.0),
        };

        let group_key = self.create_group_key(&labels, timestamp_ms);

        let metric_info = MetricInfo {
            value: metric_value,
            unit: self.translate_unit(unit),
        };

        let grouped_metric = grouped_metrics
            .entry(group_key)
            .or_insert_with(|| GroupedMetric {
                labels: labels.clone(),
                metrics: HashMap::new(),
                metadata: MetricMetadata {
                    namespace: self.config.namespace.clone(),
                    timestamp_ms,
                },
            });

        grouped_metric
            .metrics
            .insert(metric_name.to_string(), metric_info);

        Ok(())
    }

    fn add_summary_to_group(
        &self,
        metric_name: &str,
        unit: &str,
        data_point: &opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
        resource_attrs: &HashMap<String, String>,
        grouped_metrics: &mut HashMap<GroupKey, GroupedMetric>,
    ) -> Result<(), ExportError> {
        let labels = self.extract_dimensions_from_summary_data_point(data_point, resource_attrs)?;
        let timestamp_ms = data_point.time_unix_nano as i64 / 1_000_000;

        let quantiles: Vec<(f64, f64)> = data_point
            .quantile_values
            .iter()
            .map(|qv| (qv.quantile, qv.value))
            .collect();

        let metric_value = MetricValue::Summary {
            count: data_point.count,
            sum: data_point.sum,
            quantiles,
        };

        let group_key = self.create_group_key(&labels, timestamp_ms);

        let metric_info = MetricInfo {
            value: metric_value,
            unit: self.translate_unit(unit),
        };

        let grouped_metric = grouped_metrics
            .entry(group_key)
            .or_insert_with(|| GroupedMetric {
                labels: labels.clone(),
                metrics: HashMap::new(),
                metadata: MetricMetadata {
                    namespace: self.config.namespace.clone(),
                    timestamp_ms,
                },
            });

        grouped_metric
            .metrics
            .insert(metric_name.to_string(), metric_info);

        Ok(())
    }

    fn create_group_key(&self, labels: &HashMap<String, String>, timestamp_ms: i64) -> GroupKey {
        let mut sorted_labels: Vec<(String, String)> =
            labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        sorted_labels.sort_by(|a, b| a.0.cmp(&b.0));

        GroupKey {
            namespace: self.config.namespace.clone(),
            labels: sorted_labels,
            timestamp_ms,
        }
    }

    pub fn translate_grouped_metric_to_emf(
        &self,
        grouped_metric: GroupedMetric,
    ) -> Result<Value, ExportError> {
        let timestamp_ms = grouped_metric.metadata.timestamp_ms;

        // Create the dimensions array (list of dimension names)
        let dimension_keys: Vec<String> = grouped_metric.labels.keys().cloned().collect();

        // Create metrics array for CloudWatch
        let mut cw_metrics = Vec::new();
        for (metric_name, metric_info) in &grouped_metric.metrics {
            cw_metrics.push(json!({
                "Name": metric_name,
                "Unit": if metric_info.unit.is_empty() { "Count" } else { &metric_info.unit }
            }));
        }

        // Create the EMF structure
        let mut emf_log = json!({
            "_aws": {
                "Timestamp": timestamp_ms,
                "CloudWatchMetrics": [{
                    "Namespace": grouped_metric.metadata.namespace,
                    "Dimensions": [dimension_keys],
                    "Metrics": cw_metrics
                }]
            }
        });

        let emf_obj = emf_log.as_object_mut().unwrap();

        // Add all labels as fields
        for (key, value) in &grouped_metric.labels {
            emf_obj.insert(key.clone(), json!(value));
        }

        // Add all metric values as fields
        for (metric_name, metric_info) in &grouped_metric.metrics {
            match &metric_info.value {
                MetricValue::Double(val) => {
                    emf_obj.insert(metric_name.clone(), json!(val));
                }
                MetricValue::Int(val) => {
                    emf_obj.insert(metric_name.clone(), json!(val));
                }
                MetricValue::Histogram {
                    count,
                    sum,
                    min,
                    max,
                } => {
                    // For histograms, emit as CloudWatch statistical set
                    emf_obj.insert(
                        metric_name.clone(),
                        json!({
                            "Count": count,
                            "Sum": sum,
                            "Min": min,
                            "Max": max
                        }),
                    );
                }
                MetricValue::Summary {
                    count,
                    sum,
                    quantiles: _,
                } => {
                    // For summaries, emit count and sum (quantiles handled separately if detailed metrics enabled)
                    emf_obj.insert(format!("{}_count", metric_name), json!(count));
                    emf_obj.insert(format!("{}_sum", metric_name), json!(sum));
                }
            }
        }

        Ok(emf_log)
    }

    fn extract_dimensions_from_number_data_point(
        &self,
        data_point: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
        resource_attrs: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>, ExportError> {
        let mut dimensions = HashMap::new();

        // Add resource attributes as dimensions
        for (key, value) in resource_attrs {
            dimensions.insert(key.clone(), value.clone());
        }

        // Add data point attributes as dimensions
        for attr in &data_point.attributes {
            if let Some(value) = &attr.value {
                let string_val = match &value.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                        i.to_string()
                    }
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(
                        b,
                    )) => b.to_string(),
                    _ => continue,
                };
                dimensions.insert(attr.key.clone(), string_val);
            }
        }

        Ok(dimensions)
    }

    fn extract_dimensions_from_histogram_data_point(
        &self,
        data_point: &opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
        resource_attrs: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>, ExportError> {
        let mut dimensions = HashMap::new();

        // Add resource attributes
        for (key, value) in resource_attrs {
            dimensions.insert(key.clone(), value.clone());
        }

        // Add data point attributes
        for attr in &data_point.attributes {
            if let Some(value) = &attr.value {
                let string_val = match &value.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                        i.to_string()
                    }
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(
                        b,
                    )) => b.to_string(),
                    _ => continue,
                };
                dimensions.insert(attr.key.clone(), string_val);
            }
        }

        Ok(dimensions)
    }

    fn extract_dimensions_from_summary_data_point(
        &self,
        data_point: &opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
        resource_attrs: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>, ExportError> {
        let mut dimensions = HashMap::new();

        // Add resource attributes
        for (key, value) in resource_attrs {
            dimensions.insert(key.clone(), value.clone());
        }

        // Add data point attributes
        for attr in &data_point.attributes {
            if let Some(value) = &attr.value {
                let string_val = match &value.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                        i.to_string()
                    }
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(
                        b,
                    )) => b.to_string(),
                    _ => continue,
                };
                dimensions.insert(attr.key.clone(), string_val);
            }
        }

        Ok(dimensions)
    }

    fn translate_unit(&self, unit: &str) -> String {
        match unit {
            "1" => "".to_string(),
            "ns" => "".to_string(), // CloudWatch doesn't support nanoseconds
            "ms" => "Milliseconds".to_string(),
            "s" => "Seconds".to_string(),
            "us" => "Microseconds".to_string(),
            "By" => "Bytes".to_string(),
            "bit" => "Bits".to_string(),
            _ => unit.to_string(),
        }
    }
}

fn extract_resource_attributes(resource_metrics: &ResourceMetrics) -> HashMap<String, String> {
    let mut attrs = HashMap::new();

    if let Some(resource) = &resource_metrics.resource {
        for attr in &resource.attributes {
            if let Some(value) = &attr.value {
                let string_val = match &value.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => s.clone(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                        i.to_string()
                    }
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ) => d.to_string(),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(
                        b,
                    )) => b.to_string(),
                    _ => continue,
                };
                attrs.insert(attr.key.clone(), string_val);
            }
        }
    }

    attrs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::awsemf::AwsEmfExporterConfigBuilder;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, Sum, Summary,
        SummaryDataPoint, metric,
    };
    use std::collections::HashMap;

    fn create_test_transformer() -> MetricTransformer {
        let config = AwsEmfExporterConfigBuilder::default()
            .with_namespace("TestNamespace")
            .with_log_group_name("test-log-group")
            .with_log_stream_name("test-log-stream")
            .build()
            .config;
        MetricTransformer::new(config)
    }

    fn create_test_attributes() -> Vec<KeyValue> {
        vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueValue::StringValue("test-service".to_string())),
                }),
            },
            KeyValue {
                key: "environment".to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueValue::StringValue("test".to_string())),
                }),
            },
        ]
    }

    #[test]
    fn test_add_gauge_metric_with_double_value() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_gauge".to_string(),
            description: "Test gauge metric".to_string(),
            unit: "bytes".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: create_test_attributes(),
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsDouble(42.5)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        assert_eq!(grouped_metrics.len(), 1);

        let grouped_metric = grouped_metrics.values().next().unwrap();
        assert_eq!(grouped_metric.metrics.len(), 1);
        assert!(grouped_metric.metrics.contains_key("test_gauge"));

        let metric_info = &grouped_metric.metrics["test_gauge"];
        match &metric_info.value {
            MetricValue::Double(val) => assert_eq!(*val, 42.5),
            _ => panic!("Expected Double value"),
        }
    }

    #[test]
    fn test_add_gauge_metric_with_int_value() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_gauge_int".to_string(),
            description: "Test gauge metric".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: create_test_attributes(),
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsInt(100)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let metric_info = &grouped_metrics.values().next().unwrap().metrics["test_gauge_int"];
        match &metric_info.value {
            MetricValue::Int(val) => assert_eq!(*val, 100),
            _ => panic!("Expected Int value"),
        }
    }

    #[test]
    fn test_add_sum_metric() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_sum".to_string(),
            description: "Test sum metric".to_string(),
            unit: "ms".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    attributes: create_test_attributes(),
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsDouble(123.45)),
                    exemplars: vec![],
                    flags: 0,
                }],
                aggregation_temporality: 1,
                is_monotonic: true,
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let metric_info = &grouped_metrics.values().next().unwrap().metrics["test_sum"];
        match &metric_info.value {
            MetricValue::Double(val) => assert_eq!(*val, 123.45),
            _ => panic!("Expected Double value"),
        }
    }

    #[test]
    fn test_add_histogram_metric() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_histogram".to_string(),
            description: "Test histogram metric".to_string(),
            unit: "s".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: create_test_attributes(),
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    count: 10,
                    sum: Some(50.0),
                    bucket_counts: vec![1, 4, 3, 2],
                    explicit_bounds: vec![1.0, 5.0, 10.0],
                    exemplars: vec![],
                    flags: 0,
                    min: Some(0.5),
                    max: Some(15.0),
                }],
                aggregation_temporality: 1,
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let metric_info = &grouped_metrics.values().next().unwrap().metrics["test_histogram"];
        match &metric_info.value {
            MetricValue::Histogram {
                count,
                sum,
                min,
                max,
            } => {
                assert_eq!(*count, 10);
                assert_eq!(*sum, 50.0);
                assert_eq!(*min, 0.5);
                assert_eq!(*max, 15.0);
            }
            _ => panic!("Expected Histogram value"),
        }
    }

    #[test]
    fn test_add_histogram_metric_with_missing_optional_fields() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_histogram_minimal".to_string(),
            description: "Test histogram metric".to_string(),
            unit: "s".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    count: 5,
                    sum: None, // Missing sum
                    bucket_counts: vec![5],
                    explicit_bounds: vec![],
                    exemplars: vec![],
                    flags: 0,
                    min: None, // Missing min
                    max: None, // Missing max
                }],
                aggregation_temporality: 1,
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let metric_info =
            &grouped_metrics.values().next().unwrap().metrics["test_histogram_minimal"];
        match &metric_info.value {
            MetricValue::Histogram {
                count,
                sum,
                min,
                max,
            } => {
                assert_eq!(*count, 5);
                assert_eq!(*sum, 0.0); // Should default to 0.0
                assert_eq!(*min, 0.0); // Should default to 0.0
                assert_eq!(*max, 0.0); // Should default to 0.0
            }
            _ => panic!("Expected Histogram value"),
        }
    }

    #[test]
    fn test_add_summary_metric() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let quantile_values = vec![
            opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile {
                quantile: 0.5,
                value: 10.0,
            },
            opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile {
                quantile: 0.95,
                value: 25.0,
            },
        ];

        let metric = Metric {
            name: "test_summary".to_string(),
            description: "Test summary metric".to_string(),
            unit: "ms".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    attributes: create_test_attributes(),
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    count: 100,
                    sum: 1500.0,
                    quantile_values,
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let metric_info = &grouped_metrics.values().next().unwrap().metrics["test_summary"];
        match &metric_info.value {
            MetricValue::Summary {
                count,
                sum,
                quantiles,
            } => {
                assert_eq!(*count, 100);
                assert_eq!(*sum, 1500.0);
                assert_eq!(quantiles.len(), 2);
                assert_eq!(quantiles[0], (0.5, 10.0));
                assert_eq!(quantiles[1], (0.95, 25.0));
            }
            _ => panic!("Expected Summary value"),
        }
    }

    #[test]
    fn test_add_summary_metric_with_empty_quantiles() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_summary_empty".to_string(),
            description: "Test summary metric".to_string(),
            unit: "ms".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    count: 0,
                    sum: 0.0,
                    quantile_values: vec![],
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let metric_info = &grouped_metrics.values().next().unwrap().metrics["test_summary_empty"];
        match &metric_info.value {
            MetricValue::Summary {
                count,
                sum,
                quantiles,
            } => {
                assert_eq!(*count, 0);
                assert_eq!(*sum, 0.0);
                assert!(quantiles.is_empty());
            }
            _ => panic!("Expected Summary value"),
        }
    }

    #[test]
    fn test_invalid_metric_type() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_invalid".to_string(),
            description: "Test invalid metric".to_string(),
            unit: "".to_string(),
            metadata: vec![],
            data: None, // Invalid - no data
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_err());
        match result.unwrap_err() {
            ExportError::InvalidMetricType { metric_type } => {
                assert!(metric_type.contains("None"));
            }
            _ => panic!("Expected InvalidMetricType error"),
        }
    }

    #[test]
    fn test_number_data_point_missing_value() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_missing_value".to_string(),
            description: "Test missing value".to_string(),
            unit: "".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: None, // Missing value
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_err());
        match result.unwrap_err() {
            ExportError::ExportFailed { message } => {
                assert_eq!(message, "No metric value found");
            }
            _ => panic!("Expected ExportFailed error"),
        }
    }

    #[test]
    fn test_empty_data_points() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_empty".to_string(),
            description: "Test empty data points".to_string(),
            unit: "".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![], // Empty data points
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        // Should succeed but create no grouped metrics
        assert!(result.is_ok());
        assert!(grouped_metrics.is_empty());
    }

    #[test]
    fn test_multiple_data_points_same_metric() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_multiple".to_string(),
            description: "Test multiple data points".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![
                    NumberDataPoint {
                        attributes: vec![KeyValue {
                            key: "label".to_string(),
                            value: Some(AnyValue {
                                value: Some(AnyValueValue::StringValue("value1".to_string())),
                            }),
                        }],
                        start_time_unix_nano: 1000000000,
                        time_unix_nano: 2000000000,
                        value: Some(NumberValue::AsInt(10)),
                        exemplars: vec![],
                        flags: 0,
                    },
                    NumberDataPoint {
                        attributes: vec![KeyValue {
                            key: "label".to_string(),
                            value: Some(AnyValue {
                                value: Some(AnyValueValue::StringValue("value2".to_string())),
                            }),
                        }],
                        start_time_unix_nano: 1000000000,
                        time_unix_nano: 2000000000,
                        value: Some(NumberValue::AsInt(20)),
                        exemplars: vec![],
                        flags: 0,
                    },
                ],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        // Should create two different groups due to different labels
        assert_eq!(grouped_metrics.len(), 2);
    }

    #[test]
    fn test_grouping_by_labels_and_timestamp() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        // Add first metric
        let metric1 = Metric {
            name: "test_metric_1".to_string(),
            description: "Test metric 1".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "service".to_string(),
                        value: Some(AnyValue {
                            value: Some(AnyValueValue::StringValue("api".to_string())),
                        }),
                    }],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsInt(10)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        // Add second metric with same labels and timestamp
        let metric2 = Metric {
            name: "test_metric_2".to_string(),
            description: "Test metric 2".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "service".to_string(),
                        value: Some(AnyValue {
                            value: Some(AnyValueValue::StringValue("api".to_string())),
                        }),
                    }],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000, // Same timestamp
                    value: Some(NumberValue::AsInt(20)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result1 =
            transformer.add_to_grouped_metrics(&metric1, &resource_attrs, &mut grouped_metrics);
        let result2 =
            transformer.add_to_grouped_metrics(&metric2, &resource_attrs, &mut grouped_metrics);

        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Should group into single group due to same labels and timestamp
        assert_eq!(grouped_metrics.len(), 1);
        let grouped_metric = grouped_metrics.values().next().unwrap();
        assert_eq!(grouped_metric.metrics.len(), 2);
        assert!(grouped_metric.metrics.contains_key("test_metric_1"));
        assert!(grouped_metric.metrics.contains_key("test_metric_2"));
    }

    #[test]
    fn test_different_timestamps_create_separate_groups() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        // Add metrics with same labels but different timestamps
        let metric1 = Metric {
            name: "test_metric".to_string(),
            description: "Test metric".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsInt(10)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let metric2 = Metric {
            name: "test_metric".to_string(),
            description: "Test metric".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 3000000000, // Different timestamp
                    value: Some(NumberValue::AsInt(20)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result1 =
            transformer.add_to_grouped_metrics(&metric1, &resource_attrs, &mut grouped_metrics);
        let result2 =
            transformer.add_to_grouped_metrics(&metric2, &resource_attrs, &mut grouped_metrics);

        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Should create separate groups due to different timestamps
        assert_eq!(grouped_metrics.len(), 2);
    }

    #[test]
    fn test_duplicate_metric_name_in_same_group() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "duplicate_metric".to_string(),
            description: "Test duplicate".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![
                    NumberDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 1000000000,
                        time_unix_nano: 2000000000,
                        value: Some(NumberValue::AsInt(10)),
                        exemplars: vec![],
                        flags: 0,
                    },
                    NumberDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 1000000000,
                        time_unix_nano: 2000000000, // Same timestamp and labels
                        value: Some(NumberValue::AsInt(20)),
                        exemplars: vec![],
                        flags: 0,
                    },
                ],
            })),
        };

        // This should trigger the duplicate warning but still succeed
        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        assert_eq!(grouped_metrics.len(), 1);
        let grouped_metric = grouped_metrics.values().next().unwrap();
        // Should only contain the first metric (duplicate is not added)
        assert_eq!(grouped_metric.metrics.len(), 1);
    }

    #[test]
    fn test_zero_and_negative_timestamps() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_zero_timestamp".to_string(),
            description: "Test zero timestamp".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 0, // Zero timestamp
                    value: Some(NumberValue::AsInt(10)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let grouped_metric = grouped_metrics.values().next().unwrap();
        assert_eq!(grouped_metric.metadata.timestamp_ms, 0);
    }

    #[test]
    fn test_large_timestamp_values() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let large_timestamp = u64::MAX;
        let metric = Metric {
            name: "test_large_timestamp".to_string(),
            description: "Test large timestamp".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: large_timestamp,
                    value: Some(NumberValue::AsInt(10)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let grouped_metric = grouped_metrics.values().next().unwrap();
        assert_eq!(
            grouped_metric.metadata.timestamp_ms,
            (large_timestamp as i64) / 1_000_000
        );
    }

    #[test]
    fn test_with_resource_attributes() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();

        let mut resource_attrs = HashMap::new();
        resource_attrs.insert("service.name".to_string(), "my-service".to_string());
        resource_attrs.insert("version".to_string(), "1.0.0".to_string());

        let metric = Metric {
            name: "test_with_resources".to_string(),
            description: "Test with resource attributes".to_string(),
            unit: "count".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsInt(10)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        // The resource attributes should be incorporated into the labels through the extraction methods
        assert_eq!(grouped_metrics.len(), 1);
    }

    #[test]
    fn test_unit_translation() {
        let transformer = create_test_transformer();
        let mut grouped_metrics = HashMap::new();
        let resource_attrs = HashMap::new();

        let metric = Metric {
            name: "test_unit".to_string(),
            description: "Test unit translation".to_string(),
            unit: "By".to_string(), // This should be translated by translate_unit
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsDouble(1024.0)),
                    exemplars: vec![],
                    flags: 0,
                }],
            })),
        };

        let result =
            transformer.add_to_grouped_metrics(&metric, &resource_attrs, &mut grouped_metrics);

        assert!(result.is_ok());
        let grouped_metric = grouped_metrics.values().next().unwrap();
        let metric_info = &grouped_metric.metrics["test_unit"];
        // The unit should be processed by translate_unit method
        assert!(!metric_info.unit.is_empty());
    }
}
