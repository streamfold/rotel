use std::sync::Arc;
use arrow::array::{
    Array, Int64Array, Float64Array, StringArray, BooleanArray,
    ListArray, MapArray, Date32Array, TimestampNanosecondArray,
    NullArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use crate::exporters::file::{Result, FileExporterError};

/// DataConverter provides conversion between JSON and Arrow RecordBatch.
///
/// All conversion errors are returned as `FileExporterError::InvalidData` with detailed context.
/// Recovery: Validate input data types and structure before conversion.
///
pub struct DataConverter {
    /// The inferred schema from the data
    schema: Arc<Schema>,
}

impl DataConverter {
    /// Creates a new DataConverter with the given schema
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }

    /// Infers schema and converts JSON data to Arrow RecordBatch.
    ///
    /// # Errors
    ///
    /// Returns `FileExporterError::InvalidData` for malformed or unsupported data.
    /// Recovery: Ensure input is a JSON array of objects with supported types.
    pub fn convert_json_to_record_batch(data: &[u8]) -> Result<RecordBatch> {
        // Parse JSON data
        let json_value: Value = serde_json::from_slice(data)
            .map_err(|e| FileExporterError::InvalidData(format!("Failed to parse JSON: {}", e)))?;

        // For now, we'll handle a simple array of objects
        let array = json_value.as_array()
            .ok_or_else(|| FileExporterError::InvalidData("Expected JSON array".to_string()))?;

        if array.is_empty() {
            return Err(FileExporterError::InvalidData("Empty data array".to_string()));
        }

        // Create schema based on the first object
        let first_obj = array[0].as_object()
            .ok_or_else(|| FileExporterError::InvalidData("Expected JSON objects".to_string()))?;

        let mut fields = Vec::new();
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();

        for (key, value) in first_obj {
            let (field, array) = Self::convert_field(key, value, array)?;
            fields.push(field);
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| FileExporterError::Export(format!("Failed to create RecordBatch: {}", e)))
    }

    /// Converts a JSON field to Arrow field and array
    fn convert_field(key: &str, value: &Value, array: &[Value]) -> Result<(Field, Arc<dyn Array>)> {
        match value {
            Value::Null => {
                let field = Field::new(key, DataType::Null, true);
                let array = Arc::new(NullArray::new(array.len()));
                Ok((field, array))
            }
            Value::Bool(_) => {
                let field = Field::new(key, DataType::Boolean, true);
                let mut values = Vec::with_capacity(array.len());
                for obj in array {
                    let obj = obj.as_object()
                        .ok_or_else(|| FileExporterError::InvalidData("Expected JSON objects".to_string()))?;
                    values.push(obj.get(key)
                        .and_then(|v| v.as_bool())
                        .ok_or_else(|| FileExporterError::InvalidData(format!("Invalid boolean value for field {}", key)))?);
                }
                let array = Arc::new(BooleanArray::from(values));
                Ok((field, array))
            }
            Value::Number(n) if n.is_i64() => {
                let field = Field::new(key, DataType::Int64, true);
                let mut values = Vec::with_capacity(array.len());
                for obj in array {
                    let obj = obj.as_object()
                        .ok_or_else(|| FileExporterError::InvalidData("Expected JSON objects".to_string()))?;
                    values.push(obj.get(key)
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| FileExporterError::InvalidData(format!("Invalid integer value for field {}", key)))?);
                }
                let array = Arc::new(Int64Array::from(values));
                Ok((field, array))
            }
            Value::Number(n) if n.is_f64() => {
                let field = Field::new(key, DataType::Float64, true);
                let mut values = Vec::with_capacity(array.len());
                for obj in array {
                    let obj = obj.as_object()
                        .ok_or_else(|| FileExporterError::InvalidData("Expected JSON objects".to_string()))?;
                    values.push(obj.get(key)
                        .and_then(|v| v.as_f64())
                        .ok_or_else(|| FileExporterError::InvalidData(format!("Invalid float value for field {}", key)))?);
                }
                let array = Arc::new(Float64Array::from(values));
                Ok((field, array))
            }
            Value::String(_) => {
                let field = Field::new(key, DataType::Utf8, true);
                let mut values = Vec::with_capacity(array.len());
                for obj in array {
                    let obj = obj.as_object()
                        .ok_or_else(|| FileExporterError::InvalidData("Expected JSON objects".to_string()))?;
                    values.push(obj.get(key)
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| FileExporterError::InvalidData(format!("Invalid string value for field {}", key)))?);
                }
                let array = Arc::new(StringArray::from(values));
                Ok((field, array))
            }
            Value::Array(_) => {
                // For arrays, we'll use a list of strings for now
                // TODO: Support nested array types
                let field = Field::new(key, DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true);
                let mut values = Vec::with_capacity(array.len());
                for obj in array {
                    let obj = obj.as_object()
                        .ok_or_else(|| FileExporterError::InvalidData("Expected JSON objects".to_string()))?;
                    let arr = obj.get(key)
                        .and_then(|v| v.as_array())
                        .ok_or_else(|| FileExporterError::InvalidData(format!("Invalid array value for field {}", key)))?;
                    let strings: Vec<&str> = arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect();
                    values.push(strings);
                }
                let array = Arc::new(ListArray::from_string_array(StringArray::from(values)));
                Ok((field, array))
            }
            Value::Object(_) => {
                // For objects, we'll use a map of string to string for now
                // TODO: Support nested object types
                let field = Field::new(key, DataType::Map(
                    Arc::new(Field::new("entries", DataType::Struct(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ]), false)),
                    false,
                ), true);
                let mut keys = Vec::new();
                let mut values = Vec::new();
                for obj in array {
                    let obj = obj.as_object()
                        .ok_or_else(|| FileExporterError::InvalidData("Expected JSON objects".to_string()))?;
                    let map = obj.get(key)
                        .and_then(|v| v.as_object())
                        .ok_or_else(|| FileExporterError::InvalidData(format!("Invalid object value for field {}", key)))?;
                    for (k, v) in map {
                        keys.push(k.as_str());
                        values.push(v.as_str().unwrap_or(""));
                    }
                }
                let array = Arc::new(MapArray::from_string_arrays(
                    StringArray::from(keys),
                    StringArray::from(values),
                ));
                Ok((field, array))
            }
            _ => Err(FileExporterError::InvalidData(format!("Unsupported data type for field {}", key))),
        }
    }

    /// Converts a timestamp string to Arrow timestamp
    fn convert_timestamp(timestamp: &str) -> Result<i64> {
        let dt = DateTime::parse_from_rfc3339(timestamp)
            .map_err(|e| FileExporterError::InvalidData(format!("Invalid timestamp format: {}", e)))?;
        Ok(dt.timestamp_nanos())
    }

    /// Converts a date string to Arrow date
    fn convert_date(date: &str) -> Result<i32> {
        let dt = NaiveDateTime::parse_from_str(date, "%Y-%m-%d")
            .map_err(|e| FileExporterError::InvalidData(format!("Invalid date format: {}", e)))?;
        Ok(dt.date().num_days_from_ce() - 719163) // Convert to days since epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::DataType;

    #[test]
    fn test_convert_primitive_types() {
        let data = r#"[
            {"int": 42, "float": 3.14, "string": "test", "bool": true},
            {"int": 43, "float": 3.15, "string": "test2", "bool": false}
        ]"#.as_bytes();

        let batch = DataConverter::convert_json_to_record_batch(data).unwrap();
        let schema = batch.schema();

        assert_eq!(schema.field_with_name("int").unwrap().data_type(), &DataType::Int64);
        assert_eq!(schema.field_with_name("float").unwrap().data_type(), &DataType::Float64);
        assert_eq!(schema.field_with_name("string").unwrap().data_type(), &DataType::Utf8);
        assert_eq!(schema.field_with_name("bool").unwrap().data_type(), &DataType::Boolean);

        let int_array = batch.column_by_name("int").unwrap();
        let float_array = batch.column_by_name("float").unwrap();
        let string_array = batch.column_by_name("string").unwrap();
        let bool_array = batch.column_by_name("bool").unwrap();

        assert_eq!(int_array.as_any().downcast_ref::<Int64Array>().unwrap().value(0), 42);
        assert_eq!(float_array.as_any().downcast_ref::<Float64Array>().unwrap().value(0), 3.14);
        assert_eq!(string_array.as_any().downcast_ref::<StringArray>().unwrap().value(0), "test");
        assert_eq!(bool_array.as_any().downcast_ref::<BooleanArray>().unwrap().value(0), true);
    }

    #[test]
    fn test_convert_array_type() {
        let data = r#"[
            {"tags": ["tag1", "tag2"]},
            {"tags": ["tag3", "tag4"]}
        ]"#.as_bytes();

        let batch = DataConverter::convert_json_to_record_batch(data).unwrap();
        let schema = batch.schema();
        let field = schema.field_with_name("tags").unwrap();

        assert!(matches!(field.data_type(), DataType::List(_)));
    }

    #[test]
    fn test_convert_map_type() {
        let data = r#"[
            {"metadata": {"key1": "value1", "key2": "value2"}},
            {"metadata": {"key3": "value3", "key4": "value4"}}
        ]"#.as_bytes();

        let batch = DataConverter::convert_json_to_record_batch(data).unwrap();
        let schema = batch.schema();
        let field = schema.field_with_name("metadata").unwrap();

        assert!(matches!(field.data_type(), DataType::Map(_, _)));
    }

    #[test]
    fn test_convert_invalid_json() {
        let data = r#"invalid json"#.as_bytes();
        let result = DataConverter::convert_json_to_record_batch(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_empty_array() {
        let data = r#"[]"#.as_bytes();
        let result = DataConverter::convert_json_to_record_batch(data);
        assert!(result.is_err());
    }
} 