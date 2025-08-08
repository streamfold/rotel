use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

use super::common::{
    MapOrJson, ToRecordBatch, map_or_json_to_string, vec_maporjson_to_list_array,
    vec_string_to_list_array, vec_u64_to_list_array, attrs_to_map,
};
use crate::exporters::file::FileExporterError;

// Static schema created once and reused for all span record batches
static SPAN_SCHEMA: std::sync::LazyLock<Arc<Schema>> = std::sync::LazyLock::new(|| {
    Arc::new(Schema::new(vec![
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
        Field::new(
            "events_timestamp",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            false,
        ),
        Field::new(
            "events_name",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "events_attributes",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "links_trace_id",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "links_span_id",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "links_trace_state",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "links_attributes",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]))
});

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
    fn to_record_batch(rows: Vec<Self>) -> Result<RecordBatch, FileExporterError> {
        // Pre-allocate all the collections
        let mut timestamps = Vec::with_capacity(rows.len());
        let mut trace_ids = Vec::with_capacity(rows.len());
        let mut span_ids = Vec::with_capacity(rows.len());
        let mut parent_span_ids = Vec::with_capacity(rows.len());
        let mut trace_states = Vec::with_capacity(rows.len());
        let mut span_names = Vec::with_capacity(rows.len());
        let mut span_kinds = Vec::with_capacity(rows.len());
        let mut service_names = Vec::with_capacity(rows.len());
        let mut resource_attributes = Vec::with_capacity(rows.len());
        let mut scope_names = Vec::with_capacity(rows.len());
        let mut scope_versions = Vec::with_capacity(rows.len());
        let mut span_attributes = Vec::with_capacity(rows.len());
        let mut durations = Vec::with_capacity(rows.len());
        let mut status_codes = Vec::with_capacity(rows.len());
        let mut status_messages = Vec::with_capacity(rows.len());
        let mut events_timestamps = Vec::with_capacity(rows.len());
        let mut events_names = Vec::with_capacity(rows.len());
        let mut events_attrs = Vec::with_capacity(rows.len());
        let mut links_trace_ids = Vec::with_capacity(rows.len());
        let mut links_span_ids = Vec::with_capacity(rows.len());
        let mut links_trace_states = Vec::with_capacity(rows.len());
        let mut links_attrs = Vec::with_capacity(rows.len());

        // Move data from rows, consuming it
        for row in rows {
            timestamps.push(row.timestamp);
            trace_ids.push(row.trace_id);
            span_ids.push(row.span_id);
            parent_span_ids.push(row.parent_span_id);
            trace_states.push(row.trace_state);
            span_names.push(row.span_name);
            span_kinds.push(row.span_kind);
            service_names.push(row.service_name);
            resource_attributes.push(map_or_json_to_string(&row.resource_attributes));
            scope_names.push(row.scope_name);
            scope_versions.push(row.scope_version);
            span_attributes.push(map_or_json_to_string(&row.span_attributes));
            durations.push(row.duration);
            status_codes.push(row.status_code);
            status_messages.push(row.status_message);
            events_timestamps.push(row.events_timestamp);
            events_names.push(row.events_name);
            events_attrs.push(row.events_attributes);
            links_trace_ids.push(row.links_trace_id);
            links_span_ids.push(row.links_span_id);
            links_trace_states.push(row.links_trace_state);
            links_attrs.push(row.links_attributes);
        }

        // Build arrays from the consumed data
        let timestamp = arrow::array::UInt64Array::from(timestamps);
        let trace_id = arrow::array::StringArray::from(trace_ids);
        let span_id = arrow::array::StringArray::from(span_ids);
        let parent_span_id = arrow::array::StringArray::from(parent_span_ids);
        let trace_state = arrow::array::StringArray::from(trace_states);
        let span_name = arrow::array::StringArray::from(span_names);
        let span_kind = arrow::array::StringArray::from(span_kinds);
        let service_name = arrow::array::StringArray::from(service_names);
        let resource_attributes = StringArray::from(resource_attributes);
        let scope_name = arrow::array::StringArray::from(scope_names);
        let scope_version = arrow::array::StringArray::from(scope_versions);
        let span_attributes = StringArray::from(span_attributes);
        let duration = arrow::array::Int64Array::from(durations);
        let status_code = arrow::array::StringArray::from(status_codes);
        let status_message = arrow::array::StringArray::from(status_messages);
        let events_timestamp = vec_u64_to_list_array(events_timestamps);
        let events_name = vec_string_to_list_array(events_names);
        let events_attributes = vec_maporjson_to_list_array(events_attrs);
        let links_trace_id = vec_string_to_list_array(links_trace_ids);
        let links_span_id = vec_string_to_list_array(links_span_ids);
        let links_trace_state = vec_string_to_list_array(links_trace_states);
        let links_attributes = vec_maporjson_to_list_array(links_attrs);

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

        // Use the static schema instead of creating a new one
        RecordBatch::try_new(SPAN_SCHEMA.clone(), columns)
            .map_err(|e| FileExporterError::Export(e.to_string()))
    }
}

impl SpanRow {
    pub fn from_resource_spans(
        resource_spans: &ResourceSpans,
    ) -> Result<Vec<SpanRow>, FileExporterError> {
        let mut rows = Vec::new();

        // Convert resource attributes into a map ---------------------------------
        let resource_attributes: MapOrJson = resource_spans
            .resource
            .as_ref()
            .map(|r| attrs_to_map(&r.attributes))
            .unwrap_or(MapOrJson::Map(HashMap::new()));

        // Extract service.name from resource attributes, default to "unknown" ---
        let service_name = if let MapOrJson::Map(map) = &resource_attributes {
            map.get("service.name")
                .cloned()
                .unwrap_or_else(|| "unknown".to_string())
        } else {
            "unknown".to_string()
        };


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

            let _scope_attributes = attrs_to_map(
                &scope_spans
                    .scope
                    .as_ref()
                    .map_or(vec![], |s| s.attributes.clone()),
            );

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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    #[test]
    fn test_from_resource_spans_basic() {
        let resource_spans = ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "test-scope".to_string(),
                    version: "1.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    parent_span_id: vec![],
                    name: "test-span".to_string(),
                    kind: 2, // Server
                    start_time_unix_nano: 1234567890,
                    end_time_unix_nano: 1234567900,
                    attributes: vec![KeyValue {
                        key: "span.key".to_string(),
                        value: Some(AnyValue {
                            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("span.value".to_string())),
                        }),
                    }],
                    status: Some(Status {
                        code: 1, // Ok
                        message: "success".to_string(),
                    }),
                    trace_state: "test=1".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            schema_url: "test-schema".to_string(),
        };

        let result = SpanRow::from_resource_spans(&resource_spans).unwrap();
        assert_eq!(result.len(), 1);

        let row = &result[0];
        assert_eq!(row.timestamp, 1234567890);
        assert_eq!(row.trace_id, "0102030405060708090a0b0c0d0e0f10");
        assert_eq!(row.span_id, "0102030405060708");
        assert_eq!(row.parent_span_id, "");
        assert_eq!(row.span_name, "test-span");
        assert_eq!(row.span_kind, "Server");
        assert_eq!(row.service_name, "test-service");
        assert_eq!(row.scope_name, "test-scope");
        assert_eq!(row.scope_version, "1.0");
        assert_eq!(row.duration, 10);
        assert_eq!(row.status_code, "Ok");
        assert_eq!(row.status_message, "success");
        assert_eq!(row.trace_state, "test=1");
    }

    #[test]
    fn test_from_resource_spans_empty() {
        let resource_spans = ResourceSpans {
            resource: None,
            scope_spans: vec![],
            schema_url: String::new(),
        };

        let result = SpanRow::from_resource_spans(&resource_spans).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_to_record_batch() {
        let rows = vec![SpanRow {
            timestamp: 1234567890,
            trace_id: "0102030405060708090a0b0c0d0e0f10".to_string(),
            span_id: "0102030405060708".to_string(),
            parent_span_id: "".to_string(),
            trace_state: "test=1".to_string(),
            span_name: "test-span".to_string(),
            span_kind: "Server".to_string(),
            service_name: "test-service".to_string(),
            resource_attributes: MapOrJson::Map(Default::default()),
            scope_name: "test-scope".to_string(),
            scope_version: "1.0".to_string(),
            span_attributes: MapOrJson::Map(Default::default()),
            duration: 10,
            status_code: "Ok".to_string(),
            status_message: "success".to_string(),
            events_timestamp: vec![],
            events_name: vec![],
            events_attributes: vec![],
            links_trace_id: vec![],
            links_span_id: vec![],
            links_trace_state: vec![],
            links_attributes: vec![],
        }];

        let batch = SpanRow::to_record_batch(rows).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 22);
    }
}
