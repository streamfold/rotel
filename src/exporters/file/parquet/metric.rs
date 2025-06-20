use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;

use super::common::{MapOrJson, ToRecordBatch, map_or_json_to_string};
use crate::exporters::file::FileExporterError;
use crate::{build_string_array, build_u64_array};

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct MetricRow {
    pub timestamp: u64,
    pub name: String,
    pub description: String,
    pub unit: String,
    pub type_: String,
    pub service_name: String,
    pub value: MapOrJson,
    pub resource_attributes: MapOrJson,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: MapOrJson,
}

impl ToRecordBatch for MetricRow {
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, FileExporterError> {
        let timestamp = build_u64_array!(rows, timestamp);
        let name = build_string_array!(rows, name);
        let description = build_string_array!(rows, description);
        let unit = build_string_array!(rows, unit);
        let type_ = build_string_array!(rows, type_);
        let service_name = build_string_array!(rows, service_name);
        let value = arrow::array::StringArray::from(
            rows.iter()
                .map(|r| map_or_json_to_string(&r.value))
                .collect::<Vec<_>>(),
        );
        let resource_attributes = arrow::array::StringArray::from(
            rows.iter()
                .map(|r| map_or_json_to_string(&r.resource_attributes))
                .collect::<Vec<_>>(),
        );
        let scope_name = build_string_array!(rows, scope_name);
        let scope_version = build_string_array!(rows, scope_version);
        let scope_attributes = arrow::array::StringArray::from(
            rows.iter()
                .map(|r| map_or_json_to_string(&r.scope_attributes))
                .collect::<Vec<_>>(),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, false),
            Field::new("unit", DataType::Utf8, false),
            Field::new("type_", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("resource_attributes", DataType::Utf8, false),
            Field::new("scope_name", DataType::Utf8, false),
            Field::new("scope_version", DataType::Utf8, false),
            Field::new("scope_attributes", DataType::Utf8, false),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamp),
            Arc::new(name),
            Arc::new(description),
            Arc::new(unit),
            Arc::new(type_),
            Arc::new(service_name),
            Arc::new(value),
            Arc::new(resource_attributes),
            Arc::new(scope_name),
            Arc::new(scope_version),
            Arc::new(scope_attributes),
        ];

        RecordBatch::try_new(schema, columns).map_err(|e| FileExporterError::Export(e.to_string()))
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
                let type_ = match metric.data.as_ref() {
                    Some(Data::Gauge(_)) => "gauge",
                    Some(Data::Sum(_)) => "sum",
                    Some(Data::Histogram(_)) => "histogram",
                    Some(Data::ExponentialHistogram(_)) => "exponential_histogram",
                    Some(Data::Summary(_)) => "summary",
                    None => "unknown",
                };

                // We treat the *first* data point as representative for timestamp & value.
                let (timestamp, value_json) = match &metric.data {
                    Some(Data::Gauge(g)) => g
                        .data_points
                        .first()
                        .map(|dp| (dp.time_unix_nano, number_datapoint_to_json(dp)))
                        .unwrap_or((0, "null".to_string())),
                    Some(Data::Sum(s)) => s
                        .data_points
                        .first()
                        .map(|dp| (dp.time_unix_nano, number_datapoint_to_json(dp)))
                        .unwrap_or((0, "null".to_string())),
                    Some(Data::Histogram(h)) => h
                        .data_points
                        .first()
                        .map(|dp| (dp.time_unix_nano, histogram_datapoint_to_json(dp)))
                        .unwrap_or((0, "{}".to_string())),
                    Some(Data::ExponentialHistogram(eh)) => eh
                        .data_points
                        .first()
                        .map(|dp| (dp.time_unix_nano, exp_hist_datapoint_to_json(dp)))
                        .unwrap_or((0, "{}".to_string())),
                    Some(Data::Summary(su)) => su
                        .data_points
                        .first()
                        .map(|dp| (dp.time_unix_nano, summary_datapoint_to_json(dp)))
                        .unwrap_or((0, "{}".to_string())),
                    None => (0, "{}".to_string()),
                };

                rows.push(MetricRow {
                    timestamp,
                    name: metric.name.clone(),
                    description: metric.description.clone(),
                    unit: metric.unit.clone(),
                    type_: type_.to_string(),
                    service_name: service_name.clone(),
                    value: MapOrJson::Json(value_json),
                    resource_attributes: resource_attrs.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                    scope_attributes: scope_attributes.clone(),
                });
            }
        }

        Ok(rows)
    }
}

// ---- helper functions --------------------------------------------

fn number_datapoint_to_json(
    dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
) -> String {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::*;
    let v = dp
        .value
        .as_ref()
        .map(|val| match val {
            AsInt(i) => serde_json::json!(*i),
            AsDouble(d) => serde_json::json!(*d),
        })
        .unwrap_or(serde_json::json!(null));
    v.to_string()
}

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
