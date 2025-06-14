use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

use super::common::{map_or_json_to_string, vec_u64_to_list_array, vec_string_to_list_array, vec_maporjson_to_list_array, MapOrJson, ToRecordBatch};
use crate::exporters::file::FileExporterError;
use crate::{build_string_array, build_u64_array, build_i64_array};

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct SpanRow {
    pub timestamp: u64,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub trace_state: String,
    pub span_name: String,
    pub span_kind: String,
    pub service_name: String,
    pub resource_attributes: MapOrJson,
    pub scope_name: String,
    pub scope_version: String,
    pub span_attributes: MapOrJson,
    pub duration: i64,
    pub status_code: String,
    pub status_message: String,
    pub events_timestamp: Vec<u64>,
    pub events_name: Vec<String>,
    pub events_attributes: Vec<MapOrJson>,
    pub links_trace_id: Vec<String>,
    pub links_span_id: Vec<String>,
    pub links_trace_state: Vec<String>,
    pub links_attributes: Vec<MapOrJson>,
}

impl ToRecordBatch for SpanRow {
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, FileExporterError> {
        // Primitive columns --------------------------------------------------
        let timestamp = build_u64_array!(rows, timestamp);
        let trace_id = build_string_array!(rows, trace_id);
        let span_id = build_string_array!(rows, span_id);
        let parent_span_id = build_string_array!(rows, parent_span_id);
        let trace_state = build_string_array!(rows, trace_state);
        let span_name = build_string_array!(rows, span_name);
        let span_kind = build_string_array!(rows, span_kind);
        let service_name = build_string_array!(rows, service_name);
        let resource_attributes = StringArray::from(rows.iter().map(|r| map_or_json_to_string(&r.resource_attributes)).collect::<Vec<_>>());
        let scope_name = build_string_array!(rows, scope_name);
        let scope_version = build_string_array!(rows, scope_version);
        let span_attributes = StringArray::from(rows.iter().map(|r| map_or_json_to_string(&r.span_attributes)).collect::<Vec<_>>());
        let duration = build_i64_array!(rows, duration);
        let status_code = build_string_array!(rows, status_code);
        let status_message = build_string_array!(rows, status_message);

        // Repeated / list columns -------------------------------------------
        let events_timestamp = vec_u64_to_list_array(&rows.iter().map(|r| r.events_timestamp.clone()).collect::<Vec<_>>());
        let events_name = vec_string_to_list_array(&rows.iter().map(|r| r.events_name.clone()).collect::<Vec<_>>());
        let events_attributes = vec_maporjson_to_list_array(&rows.iter().map(|r| r.events_attributes.clone()).collect::<Vec<_>>());
        let links_trace_id = vec_string_to_list_array(&rows.iter().map(|r| r.links_trace_id.clone()).collect::<Vec<_>>());
        let links_span_id = vec_string_to_list_array(&rows.iter().map(|r| r.links_span_id.clone()).collect::<Vec<_>>());
        let links_trace_state = vec_string_to_list_array(&rows.iter().map(|r| r.links_trace_state.clone()).collect::<Vec<_>>());
        let links_attributes = vec_maporjson_to_list_array(&rows.iter().map(|r| r.links_attributes.clone()).collect::<Vec<_>>());

        // Schema -------------------------------------------------------------
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::UInt64, false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, false),
            Field::new("trace_state", DataType::Utf8, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("resource_attributes", DataType::Utf8, false),
            Field::new("scope_name", DataType::Utf8, false),
            Field::new("scope_version", DataType::Utf8, false),
            Field::new("span_attributes", DataType::Utf8, false),
            Field::new("duration", DataType::Int64, false),
            Field::new("status_code", DataType::Utf8, false),
            Field::new("status_message", DataType::Utf8, false),
            Field::new("events_timestamp", DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))), false),
            Field::new("events_name", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("events_attributes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("links_trace_id", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("links_span_id", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("links_trace_state", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("links_attributes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
        ]));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamp),
            Arc::new(trace_id),
            Arc::new(span_id),
            Arc::new(parent_span_id),
            Arc::new(trace_state),
            Arc::new(span_name),
            Arc::new(span_kind),
            Arc::new(service_name),
            Arc::new(resource_attributes),
            Arc::new(scope_name),
            Arc::new(scope_version),
            Arc::new(span_attributes),
            Arc::new(duration),
            Arc::new(status_code),
            Arc::new(status_message),
            Arc::new(events_timestamp),
            Arc::new(events_name),
            Arc::new(events_attributes),
            Arc::new(links_trace_id),
            Arc::new(links_span_id),
            Arc::new(links_trace_state),
            Arc::new(links_attributes),
        ];

        RecordBatch::try_new(schema, columns).map_err(|e| FileExporterError::Export(e.to_string()))
    }
}

impl SpanRow {
    pub fn from_resource_spans(resource_spans: &ResourceSpans) -> Result<Vec<SpanRow>, FileExporterError> {
        let mut rows = Vec::new();

        // Convert resource attributes into a map ---------------------------------
        let resource_attributes: MapOrJson = {
            let mut map = HashMap::new();
            if let Some(resource) = &resource_spans.resource {
                for attr in &resource.attributes {
                    if let Some(any_value) = &attr.value {
                        let value_str = match &any_value.value {
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(_)) => "[]".to_string(),
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(_)) => "{}".to_string(),
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(_)) => "".to_string(),
                            None => "".to_string(),
                        };
                        map.insert(attr.key.clone(), value_str);
                    }
                }
            }
            MapOrJson::Map(map)
        };

        // Extract service.name from resource attributes, default to "unknown" ---
        let service_name = if let MapOrJson::Map(map) = &resource_attributes {
            map.get("service.name").cloned().unwrap_or_else(|| "unknown".to_string())
        } else {
            "unknown".to_string()
        };

        // Helper to convert attribute list to MapOrJson --------------------------
        fn attrs_to_map(attrs: &Vec<opentelemetry_proto::tonic::common::v1::KeyValue>) -> MapOrJson {
            let mut map = HashMap::new();
            for attr in attrs {
                if let Some(any_value) = &attr.value {
                    let value_str = match &any_value.value {
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => s.clone(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => b.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => i.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => d.to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(_)) => "[]".to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(_)) => "{}".to_string(),
                        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(_)) => "".to_string(),
                        None => "".to_string(),
                    };
                    map.insert(attr.key.clone(), value_str);
                }
            }
            MapOrJson::Map(map)
        }

        // Helper to convert span kind to string ----------------------------------
        fn span_kind_to_string(kind: i32) -> String {
            match kind {
                0 => "Unspecified".to_string(),
                1 => "Internal".to_string(),
                2 => "Server".to_string(),
                3 => "Client".to_string(),
                4 => "Producer".to_string(),
                5 => "Consumer".to_string(),
                _ => "Unspecified".to_string(),
            }
        }

        // Helper to convert status code to string --------------------------------
        fn status_code(span: &opentelemetry_proto::tonic::trace::v1::Span) -> String {
            match &span.status {
                None => "Unset".to_string(),
                Some(s) => match s.code {
                    0 => "Unset".to_string(),
                    1 => "Ok".to_string(),
                    2 => "Error".to_string(),
                    _ => "Unset".to_string(),
                },
            }
        }

        fn status_message(span: &opentelemetry_proto::tonic::trace::v1::Span) -> String {
            match &span.status {
                None => "".to_string(),
                Some(s) => s.message.clone(),
            }
        }

        // -----------------------------------------------------------------------
        for scope_spans in &resource_spans.scope_spans {
            let scope_name = scope_spans
                .scope
                .as_ref()
                .map_or("".to_string(), |s| s.name.clone());
            let scope_version = scope_spans
                .scope
                .as_ref()
                .map_or("".to_string(), |s| s.version.clone());

            let _scope_attributes = attrs_to_map(&scope_spans
                .scope
                .as_ref()
                .map_or(vec![], |s| s.attributes.clone()));

            for span in &scope_spans.spans {
                // Build the row --------------------------------------------------
                let mut row = SpanRow {
                    timestamp: span.start_time_unix_nano,
                    trace_id: hex::encode(span.trace_id.clone()),
                    span_id: hex::encode(span.span_id.clone()),
                    parent_span_id: hex::encode(span.parent_span_id.clone()),
                    trace_state: span.trace_state.clone(),
                    span_name: span.name.clone(),
                    span_kind: span_kind_to_string(span.kind),
                    service_name: service_name.clone(),
                    resource_attributes: resource_attributes.clone(),
                    scope_name: scope_name.clone(),
                    scope_version: scope_version.clone(),
                    span_attributes: attrs_to_map(&span.attributes),
                    duration: (span.end_time_unix_nano as i64 - span.start_time_unix_nano as i64),
                    status_code: status_code(span),
                    status_message: status_message(span),
                    events_timestamp: Vec::new(),
                    events_name: Vec::new(),
                    events_attributes: Vec::new(),
                    links_trace_id: Vec::new(),
                    links_span_id: Vec::new(),
                    links_trace_state: Vec::new(),
                    links_attributes: Vec::new(),
                };

                // Populate events ----------------------------------------------
                for event in &span.events {
                    row.events_timestamp.push(event.time_unix_nano);
                    row.events_name.push(event.name.clone());
                    row.events_attributes.push(attrs_to_map(&event.attributes));
                }

                // Populate links -----------------------------------------------
                for link in &span.links {
                    row.links_trace_id.push(hex::encode(link.trace_id.clone()));
                    row.links_span_id.push(hex::encode(link.span_id.clone()));
                    row.links_trace_state.push(link.trace_state.clone());
                    row.links_attributes.push(attrs_to_map(&link.attributes));
                }

                rows.push(row);
            }
        }

        Ok(rows)
    }
} 