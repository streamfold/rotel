use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

use crate::exporters::file::FileExporterError;

/// Unified representation used in Arrow columns where the data could be a
/// key/value map (flattened) or an already-encoded JSON string.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum MapOrJson {
    Map(HashMap<String, String>),
    Json(String),
}

/// Trait that converts a vec of row structs into an Arrow `RecordBatch` ready
/// for Parquet/Arrow IPC writing.
pub trait ToRecordBatch {
    fn to_record_batch(rows: Vec<Self>) -> Result<RecordBatch, FileExporterError>
    where
        Self: Sized;
}

// ---------------------------------------------------------------------------
// Helper functions to reduce boiler-plate when creating Arrow arrays
// ---------------------------------------------------------------------------

/// Convert `MapOrJson` to its serialized string representation.
pub(crate) fn map_or_json_to_string(m: &MapOrJson) -> String {
    match m {
        MapOrJson::Map(map) => serde_json::to_string(map).unwrap_or_default(),
        MapOrJson::Json(s) => s.clone(),
    }
}

/// Helper that builds a `ListArray<UInt64Array>` from a `Vec<Vec<u64>>` field.
pub(crate) fn vec_u64_to_list_array(data: Vec<Vec<u64>>) -> ListArray {
    let offsets = OffsetBuffer::from_lengths(data.iter().map(|v| v.len()));
    let values = arrow::array::UInt64Array::from_iter(data.into_iter().flatten());
    ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, true)),
        offsets,
        Arc::new(values),
        None,
    )
}

/// Helper that builds a `ListArray<StringArray>` from a `Vec<Vec<String>>` field.
pub(crate) fn vec_string_to_list_array(data: Vec<Vec<String>>) -> ListArray {
    let offsets = OffsetBuffer::from_lengths(data.iter().map(|v| v.len()));
    let values = StringArray::from(data.into_iter().flatten().collect::<Vec<_>>());
    ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets,
        Arc::new(values),
        None,
    )
}

/// Helper that builds a `ListArray<StringArray>` from a `Vec<Vec<MapOrJson>>` field.
pub(crate) fn vec_maporjson_to_list_array(data: Vec<Vec<MapOrJson>>) -> ListArray {
    let offsets = OffsetBuffer::from_lengths(data.iter().map(|v| v.len()));
    let values = StringArray::from(
        data.into_iter()
            .flatten()
            .map(|m| map_or_json_to_string(&m))
            .collect::<Vec<_>>(),
    );
    ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets,
        Arc::new(values),
        None,
    )
}

/// Convert OpenTelemetry attributes to a MapOrJson representation.
/// This function handles all OpenTelemetry AnyValue types:
/// - String, Bool, Int, Double: converted to string representation
/// - ArrayValue: serialized as JSON array
/// - KvlistValue: serialized as JSON object
/// - BytesValue: encoded as base64 string
pub(crate) fn attrs_to_map(
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> MapOrJson {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;

    let mut map = HashMap::new();
    for attr in attrs {
        if let Some(any_value) = &attr.value {
            let value_str = match &any_value.value {
                Some(AnyValue::StringValue(s)) => s.clone(),
                Some(AnyValue::BoolValue(b)) => b.to_string(),
                Some(AnyValue::IntValue(i)) => i.to_string(),
                Some(AnyValue::DoubleValue(d)) => d.to_string(),
                // Array and Kvlist are simply converted to JSON for now
                Some(AnyValue::ArrayValue(arr)) => serde_json::to_string(arr).unwrap(),
                Some(AnyValue::KvlistValue(kvlist)) => serde_json::to_string(kvlist).unwrap(),
                Some(AnyValue::BytesValue(bytes)) => BASE64_STANDARD.encode(bytes),
                None => "".to_string(),
            };
            map.insert(attr.key.clone(), value_str);
        }
    }
    MapOrJson::Map(map)
}
