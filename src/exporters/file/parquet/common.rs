use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;

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
// Helper macros & functions to reduce boiler-plate when creating Arrow arrays
// ---------------------------------------------------------------------------

/// Build a `StringArray` from a field on every row.
#[macro_export]
macro_rules! build_string_array {
    ($rows:expr, $field:ident) => {
        arrow::array::StringArray::from($rows.into_iter().map(|r| r.$field).collect::<Vec<_>>())
    };
}

/// Build a `UInt64Array` from an integer field.
#[macro_export]
macro_rules! build_u64_array {
    ($rows:expr, $field:ident) => {
        arrow::array::UInt64Array::from($rows.into_iter().map(|r| r.$field).collect::<Vec<_>>())
    };
}

/// Build a `Int64Array` from an integer field.
#[macro_export]
macro_rules! build_i64_array {
    ($rows:expr, $field:ident) => {
        arrow::array::Int64Array::from($rows.into_iter().map(|r| r.$field).collect::<Vec<_>>())
    };
}

/// Build a `UInt8Array` from a `u8` field.
#[macro_export]
macro_rules! build_u8_array {
    ($rows:expr, $field:ident) => {
        arrow::array::UInt8Array::from($rows.into_iter().map(|r| r.$field).collect::<Vec<_>>())
    };
}

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
