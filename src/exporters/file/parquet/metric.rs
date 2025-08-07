use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;

use super::common::{MapOrJson, ToRecordBatch, map_or_json_to_string};
use crate::exporters::file::FileExporterError;

// Static schema created once and reused for all metric record batches
static METRIC_SCHEMA: std::sync::LazyLock<Arc<Schema>> = std::sync::LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("type_", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("value_int", DataType::Int64, true), // nullable for integer values
        Field::new("value_double", DataType::Float64, true), // nullable for double values
        Field::new("value_type", DataType::Utf8, false), // "int" | "double" | "histogram" | "summary"
        Field::new("attributes", DataType::Utf8, false), // metric-level attributes
        Field::new("complex_value", DataType::Utf8, true), // JSON for histograms/summaries (nullable)
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, false),
    ]))
});

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct MetricRow {
    pub timestamp: u64,
    pub name: String,
    pub description: String,
    pub unit: String,
    pub type_: String,
    pub service_name: String,
    pub value_int: Option<i64>,        // For integer metric values
    pub value_double: Option<f64>,     // For double metric values
    pub value_type: String,            // "int" | "double" | "histogram" | "summary"
    pub attributes: MapOrJson,         // Data point attributes
    pub complex_value: Option<String>, // JSON for histogram/summary data
    pub resource_attributes: MapOrJson,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: MapOrJson,
}

impl ToRecordBatch for MetricRow {
    fn to_record_batch(rows: Vec<Self>) -> Result<RecordBatch, FileExporterError> {
        // Pre-allocate all the collections
        let mut timestamps = Vec::with_capacity(rows.len());
        let mut names = Vec::with_capacity(rows.len());
        let mut descriptions = Vec::with_capacity(rows.len());
        let mut units = Vec::with_capacity(rows.len());
        let mut types = Vec::with_capacity(rows.len());
        let mut service_names = Vec::with_capacity(rows.len());
        let mut value_ints = Vec::with_capacity(rows.len());
        let mut value_doubles = Vec::with_capacity(rows.len());
        let mut value_types = Vec::with_capacity(rows.len());
        let mut attributes = Vec::with_capacity(rows.len());
        let mut complex_values = Vec::with_capacity(rows.len());
        let mut resource_attributes = Vec::with_capacity(rows.len());
        let mut scope_names = Vec::with_capacity(rows.len());
        let mut scope_versions = Vec::with_capacity(rows.len());
        let mut scope_attributes = Vec::with_capacity(rows.len());

        // Move data from rows, consuming it
        for row in rows {
            timestamps.push(row.timestamp);
            names.push(row.name);
            descriptions.push(row.description);
            units.push(row.unit);
            types.push(row.type_);
            service_names.push(row.service_name);
            value_ints.push(row.value_int);
            value_doubles.push(row.value_double);
            value_types.push(row.value_type);
            attributes.push(map_or_json_to_string(&row.attributes));
            complex_values.push(row.complex_value);
            resource_attributes.push(map_or_json_to_string(&row.resource_attributes));
            scope_names.push(row.scope_name);
            scope_versions.push(row.scope_version);
            scope_attributes.push(map_or_json_to_string(&row.scope_attributes));
        }

        // Build arrays from the consumed data
        let timestamp = arrow::array::UInt64Array::from(timestamps);
        let name = arrow::array::StringArray::from(names);
        let description = arrow::array::StringArray::from(descriptions);
        let unit = arrow::array::StringArray::from(units);
        let type_ = arrow::array::StringArray::from(types);
        let service_name = arrow::array::StringArray::from(service_names);
        let value_int = arrow::array::Int64Array::from(value_ints);
        let value_double = arrow::array::Float64Array::from(value_doubles);
        let value_type = arrow::array::StringArray::from(value_types);
        let attributes_array = arrow::array::StringArray::from(attributes);
        let complex_value = arrow::array::StringArray::from(complex_values);
        let resource_attributes = arrow::array::StringArray::from(resource_attributes);
        let scope_name = arrow::array::StringArray::from(scope_names);
        let scope_version = arrow::array::StringArray::from(scope_versions);
        let scope_attributes = arrow::array::StringArray::from(scope_attributes);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamp),
            Arc::new(name),
            Arc::new(description),
            Arc::new(unit),
            Arc::new(type_),
            Arc::new(service_name),
            Arc::new(value_int),
            Arc::new(value_double),
            Arc::new(value_type),
            Arc::new(attributes_array),
            Arc::new(complex_value),
            Arc::new(resource_attributes),
            Arc::new(scope_name),
            Arc::new(scope_version),
            Arc::new(scope_attributes),
        ];

        // Use the static schema instead of creating a new one
        RecordBatch::try_new(METRIC_SCHEMA.clone(), columns)
            .map_err(|e| FileExporterError::Export(e.to_string()))
    }
}

// Helper function to extract numeric values from NumberDataPoint
fn extract_number_value(
    dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
) -> (Option<i64>, Option<f64>, String) {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;

    match &dp.value {
        Some(Value::AsInt(i)) => (Some(*i), None, "int".to_string()),
        Some(Value::AsDouble(d)) => (None, Some(*d), "double".to_string()),
        None => (None, None, "null".to_string()),
    }
}

impl MetricRow {
    pub fn from_resource_metrics(
        resource_metrics: &ResourceMetrics,
    ) -> Result<Vec<MetricRow>, FileExporterError> {
        // retain core imports only
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;

        fn attrs_to_map(attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue]) -> MapOrJson {
            let mut map = std::collections::HashMap::new();
            for attr in attrs {
                if let Some(any_value) = &attr.value {
                    let value_str = match &any_value.value {
                        Some(AnyValue::StringValue(s)) => s.clone(),
                        Some(AnyValue::BoolValue(b)) => b.to_string(),
                        Some(AnyValue::IntValue(i)) => i.to_string(),
                        Some(AnyValue::DoubleValue(d)) => d.to_string(),
                        _ => "".to_string(),
                    };
                    map.insert(attr.key.clone(), value_str);
                }
            }
            MapOrJson::Map(map)
        }

        // Resource-level attributes ----------------------------
        let resource_attrs = resource_metrics
            .resource
            .as_ref()
            .map(|r| attrs_to_map(&r.attributes))
            .unwrap_or(MapOrJson::Map(Default::default()));

        // Extract service.name from resource attributes
        let service_name = if let MapOrJson::Map(map) = &resource_attrs {
            map.get("service.name").cloned().unwrap_or_default()
        } else {
            String::new()
        };

        let mut rows = Vec::new();

        for scope_metrics in &resource_metrics.scope_metrics {
            let scope_name = scope_metrics
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            let scope_version = scope_metrics
                .scope
                .as_ref()
                .map(|s| s.version.clone())
                .unwrap_or_default();
            let scope_attributes = scope_metrics
                .scope
                .as_ref()
                .map(|s| attrs_to_map(&s.attributes))
                .unwrap_or(MapOrJson::Map(Default::default()));

            for metric in &scope_metrics.metrics {
                match metric.data.as_ref() {
                    Some(Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            let (value_int, value_double, value_type) = extract_number_value(dp);
                            rows.push(MetricRow {
                                timestamp: dp.time_unix_nano,
                                name: metric.name.clone(),
                                description: metric.description.clone(),
                                unit: metric.unit.clone(),
                                type_: "gauge".to_string(),
                                service_name: service_name.clone(),
                                value_int,
                                value_double,
                                value_type,
                                attributes: attrs_to_map(&dp.attributes),
                                complex_value: None,
                                resource_attributes: resource_attrs.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                            });
                        }
                    }
                    Some(Data::Sum(s)) => {
                        for dp in &s.data_points {
                            let (value_int, value_double, value_type) = extract_number_value(dp);
                            rows.push(MetricRow {
                                timestamp: dp.time_unix_nano,
                                name: metric.name.clone(),
                                description: metric.description.clone(),
                                unit: metric.unit.clone(),
                                type_: "sum".to_string(),
                                service_name: service_name.clone(),
                                value_int,
                                value_double,
                                value_type,
                                attributes: attrs_to_map(&dp.attributes),
                                complex_value: None,
                                resource_attributes: resource_attrs.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                            });
                        }
                    }
                    Some(Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            rows.push(MetricRow {
                                timestamp: dp.time_unix_nano,
                                name: metric.name.clone(),
                                description: metric.description.clone(),
                                unit: metric.unit.clone(),
                                type_: "histogram".to_string(),
                                service_name: service_name.clone(),
                                value_int: None,
                                value_double: Some(dp.sum.unwrap_or(0.0)), // Store histogram sum as the main value
                                value_type: "histogram".to_string(),
                                attributes: attrs_to_map(&dp.attributes),
                                complex_value: Some(histogram_datapoint_to_json(dp)),
                                resource_attributes: resource_attrs.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                            });
                        }
                    }
                    Some(Data::ExponentialHistogram(eh)) => {
                        for dp in &eh.data_points {
                            rows.push(MetricRow {
                                timestamp: dp.time_unix_nano,
                                name: metric.name.clone(),
                                description: metric.description.clone(),
                                unit: metric.unit.clone(),
                                type_: "exponential_histogram".to_string(),
                                service_name: service_name.clone(),
                                value_int: None,
                                value_double: dp.sum, // Store histogram sum as the main value
                                value_type: "exponential_histogram".to_string(),
                                attributes: attrs_to_map(&dp.attributes),
                                complex_value: Some(exp_hist_datapoint_to_json(dp)),
                                resource_attributes: resource_attrs.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                            });
                        }
                    }
                    Some(Data::Summary(su)) => {
                        for dp in &su.data_points {
                            rows.push(MetricRow {
                                timestamp: dp.time_unix_nano,
                                name: metric.name.clone(),
                                description: metric.description.clone(),
                                unit: metric.unit.clone(),
                                type_: "summary".to_string(),
                                service_name: service_name.clone(),
                                value_int: None,
                                value_double: Some(dp.sum), // Store summary sum as the main value
                                value_type: "summary".to_string(),
                                attributes: attrs_to_map(&dp.attributes),
                                complex_value: Some(summary_datapoint_to_json(dp)),
                                resource_attributes: resource_attrs.clone(),
                                scope_name: scope_name.clone(),
                                scope_version: scope_version.clone(),
                                scope_attributes: scope_attributes.clone(),
                            });
                        }
                    }
                    None => {
                        // Unknown metric type, skip or handle as needed
                    }
                }
            }
        }

        Ok(rows)
    }
}

// ---- helper functions --------------------------------------------

fn histogram_datapoint_to_json(
    dp: &opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
) -> String {
    let v = serde_json::json!({
        "count": dp.count,
        "sum": dp.sum,
        "bucket_counts": dp.bucket_counts,
        "explicit_bounds": dp.explicit_bounds,
    });
    v.to_string()
}

fn exp_hist_datapoint_to_json(
    dp: &opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint,
) -> String {
    let v = serde_json::json!({
        "count": dp.count,
        "sum": dp.sum.unwrap_or(0.0),
        "scale": dp.scale,
        "zero_count": dp.zero_count,
        "positive": {
            "offset": dp.positive.as_ref().map(|p| p.offset).unwrap_or(0),
            "bucket_counts": dp.positive.as_ref().map(|p| &p.bucket_counts).unwrap_or(&vec![]),
        },
        "negative": {
            "offset": dp.negative.as_ref().map(|n| n.offset).unwrap_or(0),
            "bucket_counts": dp.negative.as_ref().map(|n| &n.bucket_counts).unwrap_or(&vec![]),
        }
    });
    v.to_string()
}

fn summary_datapoint_to_json(
    dp: &opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
) -> String {
    let quantiles: Vec<_> = dp
        .quantile_values
        .iter()
        .map(|qv| {
            serde_json::json!({
                "quantile": qv.quantile,
                "value": qv.value,
            })
        })
        .collect();
    let v = serde_json::json!({
        "count": dp.count,
        "sum": dp.sum,
        "quantile_values": quantiles,
    });
    v.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    #[test]
    fn test_from_resource_metrics_multiple_gauge_datapoints() {
        // Create two data points
        let dp1 = NumberDataPoint {
            time_unix_nano: 111,
            value: Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(42),
            ),
            ..Default::default()
        };
        let dp2 = NumberDataPoint {
            time_unix_nano: 222,
            value: Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(3.14),
            ),
            ..Default::default()
        };
        let gauge = Gauge {
            data_points: vec![dp1.clone(), dp2.clone()],
            ..Default::default()
        };
        let metric = Metric {
            name: "test_metric".to_string(),
            description: "desc".to_string(),
            unit: "unit".to_string(),
            data: Some(Data::Gauge(gauge)),
            ..Default::default()
        };
        let scope_metrics = ScopeMetrics {
            scope: Some(InstrumentationScope {
                name: "myscope".to_string(),
                version: "1.0".to_string(),
                ..Default::default()
            }),
            metrics: vec![metric],
            ..Default::default()
        };
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "svc".to_string(),
                        ),
                    ),
                }),
            }],
            ..Default::default()
        };
        let resource_metrics = ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![scope_metrics],
            ..Default::default()
        };

        let rows = MetricRow::from_resource_metrics(&resource_metrics).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, 111);
        assert_eq!(rows[1].timestamp, 222);
        assert_eq!(rows[0].name, "test_metric");
        assert_eq!(rows[0].type_, "gauge");
        assert_eq!(rows[0].service_name, "svc");
        assert_eq!(rows[0].scope_name, "myscope");
        assert_eq!(rows[0].scope_version, "1.0");
        // Check numeric value extraction
        assert_eq!(rows[0].value_int, Some(42));
        assert_eq!(rows[0].value_double, None);
        assert_eq!(rows[0].value_type, "int");

        assert_eq!(rows[1].value_int, None);
        assert_eq!(rows[1].value_double, Some(3.14));
        assert_eq!(rows[1].value_type, "double");
    }

    #[test]
    fn test_numeric_value_extraction() {
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;

        // Test integer value
        let dp_int = NumberDataPoint {
            time_unix_nano: 1000,
            value: Some(Value::AsInt(123)),
            ..Default::default()
        };
        let (value_int, value_double, value_type) = super::extract_number_value(&dp_int);
        assert_eq!(value_int, Some(123));
        assert_eq!(value_double, None);
        assert_eq!(value_type, "int");

        // Test double value
        let dp_double = NumberDataPoint {
            time_unix_nano: 2000,
            value: Some(Value::AsDouble(45.67)),
            ..Default::default()
        };
        let (value_int, value_double, value_type) = super::extract_number_value(&dp_double);
        assert_eq!(value_int, None);
        assert_eq!(value_double, Some(45.67));
        assert_eq!(value_type, "double");

        // Test null value
        let dp_null = NumberDataPoint {
            time_unix_nano: 3000,
            value: None,
            ..Default::default()
        };
        let (value_int, value_double, value_type) = super::extract_number_value(&dp_null);
        assert_eq!(value_int, None);
        assert_eq!(value_double, None);
        assert_eq!(value_type, "null");
    }
}
