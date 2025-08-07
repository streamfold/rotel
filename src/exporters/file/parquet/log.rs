use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use opentelemetry_proto::tonic::logs::v1::ResourceLogs;

use super::common::{MapOrJson, ToRecordBatch, map_or_json_to_string};
use crate::exporters::file::FileExporterError;

// Static schema created once and reused for all log record batches
static LOG_SCHEMA: std::sync::LazyLock<Arc<Schema>> = std::sync::LazyLock::new(|| {
    Arc::new(Schema::new(vec![
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
    ]))
});

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
    fn to_record_batch(rows: Vec<Self>) -> Result<RecordBatch, FileExporterError> {
        // Pre-allocate all the collections
        let mut timestamps = Vec::with_capacity(rows.len());
        let mut trace_ids = Vec::with_capacity(rows.len());
        let mut span_ids = Vec::with_capacity(rows.len());
        let mut trace_flags = Vec::with_capacity(rows.len());
        let mut severity_texts = Vec::with_capacity(rows.len());
        let mut severity_numbers = Vec::with_capacity(rows.len());
        let mut service_names = Vec::with_capacity(rows.len());
        let mut bodies = Vec::with_capacity(rows.len());
        let mut resource_schema_urls = Vec::with_capacity(rows.len());
        let mut resource_attributes = Vec::with_capacity(rows.len());
        let mut scope_schema_urls = Vec::with_capacity(rows.len());
        let mut scope_names = Vec::with_capacity(rows.len());
        let mut scope_versions = Vec::with_capacity(rows.len());
        let mut scope_attributes = Vec::with_capacity(rows.len());
        let mut log_attributes = Vec::with_capacity(rows.len());

        // Move data from rows, consuming it
        for row in rows {
            timestamps.push(row.timestamp);
            trace_ids.push(row.trace_id);
            span_ids.push(row.span_id);
            trace_flags.push(row.trace_flags);
            severity_texts.push(row.severity_text);
            severity_numbers.push(row.severity_number);
            service_names.push(row.service_name);
            bodies.push(row.body);
            resource_schema_urls.push(row.resource_schema_url);
            resource_attributes.push(map_or_json_to_string(&row.resource_attributes));
            scope_schema_urls.push(row.scope_schema_url);
            scope_names.push(row.scope_name);
            scope_versions.push(row.scope_version);
            scope_attributes.push(map_or_json_to_string(&row.scope_attributes));
            log_attributes.push(map_or_json_to_string(&row.log_attributes));
        }

        // Build arrays from the consumed data
        let timestamp = arrow::array::UInt64Array::from(timestamps);
        let trace_id = arrow::array::StringArray::from(trace_ids);
        let span_id = arrow::array::StringArray::from(span_ids);
        let trace_flags = arrow::array::UInt8Array::from(trace_flags);
        let severity_text = arrow::array::StringArray::from(severity_texts);
        let severity_number = arrow::array::UInt8Array::from(severity_numbers);
        let service_name = arrow::array::StringArray::from(service_names);
        let body = arrow::array::StringArray::from(bodies);
        let resource_schema_url = arrow::array::StringArray::from(resource_schema_urls);
        let resource_attributes = arrow::array::StringArray::from(resource_attributes);
        let scope_schema_url = arrow::array::StringArray::from(scope_schema_urls);
        let scope_name = arrow::array::StringArray::from(scope_names);
        let scope_version = arrow::array::StringArray::from(scope_versions);
        let scope_attributes = arrow::array::StringArray::from(scope_attributes);
        let log_attributes = arrow::array::StringArray::from(log_attributes);

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

        // Use the static schema instead of creating a new one
        RecordBatch::try_new(LOG_SCHEMA.clone(), columns)
            .map_err(|e| FileExporterError::Export(e.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    #[test]
    fn test_from_resource_logs_basic() {
        let resource_logs = ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "test-scope".to_string(),
                    version: "1.0".to_string(),
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1234567890,
                    trace_id: vec![1, 2, 3, 4],
                    span_id: vec![5, 6, 7, 8],
                    flags: 1,
                    severity_text: "INFO".to_string(),
                    severity_number: 9,
                    body: Some(AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("test message".to_string())),
                    }),
                    attributes: vec![KeyValue {
                        key: "log.key".to_string(),
                        value: Some(AnyValue {
                            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("log.value".to_string())),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            schema_url: "test-schema".to_string(),
        };

        let result = LogRecordRow::from_resource_logs(&resource_logs).unwrap();
        assert_eq!(result.len(), 1);

        let row = &result[0];
        assert_eq!(row.timestamp, 1234567890);
        assert_eq!(row.trace_id, "01020304");
        assert_eq!(row.span_id, "05060708");
        assert_eq!(row.trace_flags, 1);
        assert_eq!(row.severity_text, "INFO");
        assert_eq!(row.severity_number, 9);
        assert_eq!(row.service_name, "test-service");
        assert_eq!(row.body, "test message");
        assert_eq!(row.resource_schema_url, "test-schema");
        assert_eq!(row.scope_name, "test-scope");
        assert_eq!(row.scope_version, "1.0");
    }

    #[test]
    fn test_from_resource_logs_empty() {
        let resource_logs = ResourceLogs {
            resource: None,
            scope_logs: vec![],
            schema_url: String::new(),
        };

        let result = LogRecordRow::from_resource_logs(&resource_logs).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_to_record_batch() {
        let rows = vec![LogRecordRow {
            timestamp: 1234567890,
            trace_id: "01020304".to_string(),
            span_id: "05060708".to_string(),
            trace_flags: 1,
            severity_text: "INFO".to_string(),
            severity_number: 9,
            service_name: "test-service".to_string(),
            body: "test message".to_string(),
            resource_schema_url: "test-schema".to_string(),
            resource_attributes: MapOrJson::Map(Default::default()),
            scope_schema_url: String::new(),
            scope_name: "test-scope".to_string(),
            scope_version: "1.0".to_string(),
            scope_attributes: MapOrJson::Map(Default::default()),
            log_attributes: MapOrJson::Map(Default::default()),
        }];

        let batch = LogRecordRow::to_record_batch(rows).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 15);
    }
}
