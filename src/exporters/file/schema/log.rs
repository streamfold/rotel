use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use opentelemetry_proto::tonic::logs::v1::ResourceLogs;

use super::common::{MapOrJson, ToRecordBatch, map_or_json_to_string};
use crate::exporters::file::FileExporterError;
use crate::{build_string_array, build_u8_array, build_u64_array};

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct LogRecordRow {
    pub timestamp: u64,
    pub trace_id: String,
    pub span_id: String,
    pub trace_flags: u8,
    pub severity_text: String,
    pub severity_number: u8,
    pub service_name: String,
    pub body: String,
    pub resource_schema_url: String,
    pub resource_attributes: MapOrJson,
    pub scope_schema_url: String,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: MapOrJson,
    pub log_attributes: MapOrJson,
}

impl ToRecordBatch for LogRecordRow {
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, FileExporterError> {
        let timestamp = build_u64_array!(rows, timestamp);
        let trace_id = build_string_array!(rows, trace_id);
        let span_id = build_string_array!(rows, span_id);
        let trace_flags = build_u8_array!(rows, trace_flags);
        let severity_text = build_string_array!(rows, severity_text);
        let severity_number = build_u8_array!(rows, severity_number);
        let service_name = build_string_array!(rows, service_name);
        let body = build_string_array!(rows, body);
        let resource_schema_url = build_string_array!(rows, resource_schema_url);
        let resource_attributes = arrow::array::StringArray::from(
            rows.iter()
                .map(|r| map_or_json_to_string(&r.resource_attributes))
                .collect::<Vec<_>>(),
        );
        let scope_schema_url = build_string_array!(rows, scope_schema_url);
        let scope_name = build_string_array!(rows, scope_name);
        let scope_version = build_string_array!(rows, scope_version);
        let scope_attributes = arrow::array::StringArray::from(
            rows.iter()
                .map(|r| map_or_json_to_string(&r.scope_attributes))
                .collect::<Vec<_>>(),
        );
        let log_attributes = arrow::array::StringArray::from(
            rows.iter()
                .map(|r| map_or_json_to_string(&r.log_attributes))
                .collect::<Vec<_>>(),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("trace_flags", DataType::UInt8, false),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("severity_number", DataType::UInt8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("resource_schema_url", DataType::Utf8, false),
            Field::new("resource_attributes", DataType::Utf8, false),
            Field::new("scope_schema_url", DataType::Utf8, false),
            Field::new("scope_name", DataType::Utf8, false),
            Field::new("scope_version", DataType::Utf8, false),
            Field::new("scope_attributes", DataType::Utf8, false),
            Field::new("log_attributes", DataType::Utf8, false),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamp),
            Arc::new(trace_id),
            Arc::new(span_id),
            Arc::new(trace_flags),
            Arc::new(severity_text),
            Arc::new(severity_number),
            Arc::new(service_name),
            Arc::new(body),
            Arc::new(resource_schema_url),
            Arc::new(resource_attributes),
            Arc::new(scope_schema_url),
            Arc::new(scope_name),
            Arc::new(scope_version),
            Arc::new(scope_attributes),
            Arc::new(log_attributes),
        ];

        RecordBatch::try_new(schema, columns).map_err(|e| FileExporterError::Export(e.to_string()))
    }
}

impl LogRecordRow {
    pub fn from_resource_logs(
        resource_logs: &ResourceLogs,
    ) -> Result<Vec<LogRecordRow>, FileExporterError> {
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

        // Resource-level attributes & service name
        let resource_attrs = resource_logs
            .resource
            .as_ref()
            .map(|r| attrs_to_map(&r.attributes))
            .unwrap_or(MapOrJson::Map(Default::default()));

        let service_name = if let MapOrJson::Map(map) = &resource_attrs {
            map.get("service.name").cloned().unwrap_or_default()
        } else {
            String::new()
        };

        let mut rows = Vec::new();

        for scope_logs in &resource_logs.scope_logs {
            let scope_name = scope_logs
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            let scope_version = scope_logs
                .scope
                .as_ref()
                .map(|s| s.version.clone())
                .unwrap_or_default();
            let scope_attrs = scope_logs
                .scope
                .as_ref()
                .map(|s| attrs_to_map(&s.attributes))
                .unwrap_or(MapOrJson::Map(Default::default()));
            let scope_schema_url = scope_logs.schema_url.clone();

            for log_record in &scope_logs.log_records {
                let body_str = match &log_record.body {
                    None => String::new(),
                    Some(any_value) => match &any_value.value {
                        Some(AnyValue::StringValue(s)) => s.clone(),
                        Some(AnyValue::BoolValue(b)) => b.to_string(),
                        Some(AnyValue::IntValue(i)) => i.to_string(),
                        Some(AnyValue::DoubleValue(d)) => d.to_string(),
                        _ => "".to_string(),
                    },
                };

                rows.push(LogRecordRow {
                    timestamp: log_record.time_unix_nano,
                    trace_id: hex::encode(&log_record.trace_id),
                    span_id: hex::encode(&log_record.span_id),
                    trace_flags: log_record.flags as u8,
                    severity_text: log_record.severity_text.clone(),
                    severity_number: log_record.severity_number as u8,
                    service_name: service_name.clone(),
                    body: body_str,
                    resource_schema_url: resource_logs.schema_url.clone(),
                    resource_attributes: resource_attrs.clone(),
                    scope_schema_url: scope_schema_url.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                    scope_attributes: scope_attrs.clone(),
                    log_attributes: attrs_to_map(&log_record.attributes),
                });
            }
        }

        Ok(rows)
    }
}
