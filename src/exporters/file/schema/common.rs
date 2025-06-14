use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array, ListArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use arrow::buffer::OffsetBuffer;

use crate::exporters::file::FileExporterError;

/// Unified representation used in Arrow columns where the data could be a
/// key/value map (flattened) or an already-encoded JSON string.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MapOrJson {
    Map(HashMap<String, String>),
    Json(String),
}

/// Trait that converts a slice of row structs into an Arrow `RecordBatch` ready
/// for Parquet/Arrow IPC writing.
pub trait ToRecordBatch {
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, FileExporterError>
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
        arrow::array::StringArray::from(
            $rows.iter().map(|r| r.$field.clone()).collect::<Vec<_>>()
        )
    };
}

/// Build a `UInt64Array` from an integer field.
#[macro_export]
macro_rules! build_u64_array {
    ($rows:expr, $field:ident) => {
        arrow::array::UInt64Array::from(
            $rows.iter().map(|r| r.$field).collect::<Vec<_>>()
        )
    };
}

/// Build a `Int64Array` from an integer field.
#[macro_export]
macro_rules! build_i64_array {
    ($rows:expr, $field:ident) => {
        arrow::array::Int64Array::from(
            $rows.iter().map(|r| r.$field).collect::<Vec<_>>()
        )
    };
}

/// Build a `UInt8Array` from a `u8` field.
#[macro_export]
macro_rules! build_u8_array {
    ($rows:expr, $field:ident) => {
        arrow::array::UInt8Array::from(
            $rows.iter().map(|r| r.$field).collect::<Vec<_>>()
        )
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
pub(crate) fn vec_u64_to_list_array(data: &[Vec<u64>]) -> ListArray {
    let value_arrays: Vec<UInt64Array> = data.iter().map(|v| UInt64Array::from(v.clone())).collect();
    let values = arrow::array::UInt64Array::from_iter(
        value_arrays.iter().flat_map(|arr| arr.values().iter().copied()),
    );
    let offsets = OffsetBuffer::from_lengths(data.iter().map(|v| v.len()));
    ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, true)),
        offsets,
        Arc::new(values),
        None,
    )
}

/// Helper that builds a `ListArray<StringArray>` from a `Vec<Vec<String>>` field.
pub(crate) fn vec_string_to_list_array(data: &[Vec<String>]) -> ListArray {
    let value_arrays: Vec<StringArray> = data.iter().map(|v| StringArray::from(v.clone())).collect();
    let values = arrow::array::StringArray::from_iter(
        value_arrays.iter().flat_map(|arr| arr.iter().map(|s| s.map(|s| s.to_string()))),
    );
    let offsets = OffsetBuffer::from_lengths(data.iter().map(|v| v.len()));
    ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets,
        Arc::new(values),
        None,
    )
}

/// Helper that builds a `ListArray<StringArray>` from a `Vec<Vec<MapOrJson>>` field.
pub(crate) fn vec_maporjson_to_list_array(data: &[Vec<MapOrJson>]) -> ListArray {
    let value_arrays: Vec<StringArray> = data
        .iter()
        .map(|v| StringArray::from(v.iter().map(map_or_json_to_string).collect::<Vec<_>>()))
        .collect();
    let values = arrow::array::StringArray::from_iter(
        value_arrays.iter().flat_map(|arr| arr.iter().map(|s| s.map(|s| s.to_string()))),
    );
    let offsets = OffsetBuffer::from_lengths(data.iter().map(|v| v.len()));
    ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets,
        Arc::new(values),
        None,
    )
} 