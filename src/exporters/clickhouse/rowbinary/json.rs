use serde::{Serialize, Serializer};
use std::borrow::Cow;

use crate::otlp::cvattr::ConvertedAttrValue;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use serde_json::json;

/// Default maximum recursion depth for nested structures to prevent stack overflow
pub const DEFAULT_NESTED_KV_MAX_DEPTH: usize = 10;

/// JSON type representation for ClickHouse rowbinary format.
/// Uses `Cow` for strings and object keys to efficiently handle both
/// borrowed and owned data with a single type.
#[derive(Debug)]
pub enum JsonType<'a> {
    Int(i64),
    Str(Cow<'a, str>),
    Double(f64),
    Bool(bool),
    Array(Vec<JsonType<'a>>),
    Object(Vec<(Cow<'a, str>, JsonType<'a>)>),
}

impl<'a> JsonType<'a> {
    /// Create a borrowed string variant
    #[inline]
    pub fn str_borrowed(s: &'a str) -> Self {
        JsonType::Str(Cow::Borrowed(s))
    }

    /// Create an owned string variant
    #[inline]
    pub fn str_owned(s: String) -> Self {
        JsonType::Str(Cow::Owned(s))
    }
}

impl<'a> From<&'a ConvertedAttrValue> for JsonType<'a> {
    fn from(value: &'a ConvertedAttrValue) -> Self {
        match value {
            ConvertedAttrValue::Int(i) => JsonType::Int(*i),
            ConvertedAttrValue::String(s) => JsonType::str_borrowed(s.as_str()),
            ConvertedAttrValue::Double(d) => JsonType::Double(*d),
        }
    }
}

impl From<ConvertedAttrValue> for JsonType<'static> {
    fn from(value: ConvertedAttrValue) -> Self {
        match value {
            ConvertedAttrValue::Int(i) => JsonType::Int(i),
            ConvertedAttrValue::String(s) => JsonType::str_owned(s),
            ConvertedAttrValue::Double(d) => JsonType::Double(d),
        }
    }
}

/// Convert AnyValue to JsonType with configurable nesting behavior.
/// - `nested_kv_max_depth = None` or `Some(0)`: flat mode (backwards compatible, fastest)
/// - `nested_kv_max_depth = Some(n)` where n > 0: recursive conversion up to depth n
///
/// Flat mode is the hot path - it handles nested complex types by serializing to JSON strings.
/// Nested mode recursively converts KvlistValue to Object and preserves ArrayValue structure.
#[inline]
pub fn anyvalue_to_jsontype<'a>(value: &'a AnyValue, nested_kv_max_depth: Option<usize>) -> JsonType<'a> {
    match nested_kv_max_depth {
        // Fast path: flat mode (backwards compatible)
        None | Some(0) => anyvalue_to_jsontype_flat(value),
        // Nested mode: recursive conversion
        Some(max_depth) => anyvalue_to_jsontype_nested(value, 0, max_depth),
    }
}

/// Fast path: convert without recursing into nested structures.
/// Arrays containing KvlistValue or nested ArrayValue are serialized as JSON strings.
#[inline]
fn anyvalue_to_jsontype_flat<'a>(value: &'a AnyValue) -> JsonType<'a> {
    match &value.value {
        Some(Value::IntValue(i)) => JsonType::Int(*i),
        Some(Value::DoubleValue(d)) => JsonType::Double(*d),
        Some(Value::StringValue(s)) => JsonType::str_borrowed(s.as_str()),
        Some(Value::BoolValue(b)) => JsonType::Bool(*b),
        Some(Value::ArrayValue(a)) => {
            let values = a.values.iter()
                .map(|v| match &v.value {
                    Some(Value::IntValue(i)) => JsonType::Int(*i),
                    Some(Value::DoubleValue(d)) => JsonType::Double(*d),
                    Some(Value::StringValue(s)) => JsonType::str_borrowed(s.as_str()),
                    Some(Value::BoolValue(b)) => JsonType::Bool(*b),
                    // Complex nested types become JSON strings
                    Some(Value::ArrayValue(arr)) => JsonType::str_owned(json!(arr).to_string()),
                    Some(Value::KvlistValue(kv)) => JsonType::str_owned(json!(kv).to_string()),
                    Some(Value::BytesValue(b)) => JsonType::str_owned(hex::encode(b)),
                    None => JsonType::str_borrowed(""),
                })
                .collect();
            JsonType::Array(values)
        }
        Some(Value::KvlistValue(_)) => JsonType::str_borrowed(""),
        Some(Value::BytesValue(b)) => JsonType::str_owned(hex::encode(b)),
        None => JsonType::str_borrowed(""),
    }
}

/// Nested mode: recursively convert with depth tracking.
fn anyvalue_to_jsontype_nested<'a>(value: &'a AnyValue, depth: usize, max_depth: usize) -> JsonType<'a> {
    if depth > max_depth {
        return JsonType::str_owned(json!(value).to_string());
    }

    match &value.value {
        Some(Value::IntValue(i)) => JsonType::Int(*i),
        Some(Value::DoubleValue(d)) => JsonType::Double(*d),
        Some(Value::StringValue(s)) => JsonType::str_borrowed(s.as_str()),
        Some(Value::BoolValue(b)) => JsonType::Bool(*b),
        Some(Value::ArrayValue(a)) => {
            let values = a.values.iter()
                .map(|v| anyvalue_to_jsontype_nested(v, depth + 1, max_depth))
                .collect();
            JsonType::Array(values)
        }
        Some(Value::KvlistValue(kv)) => {
            let entries = kv.values.iter()
                .filter_map(|entry| {
                    entry.value.as_ref().map(|v| (
                        Cow::Borrowed(entry.key.as_str()),
                        anyvalue_to_jsontype_nested(v, depth + 1, max_depth),
                    ))
                })
                .collect();
            JsonType::Object(entries)
        }
        Some(Value::BytesValue(b)) => JsonType::str_owned(hex::encode(b)),
        None => JsonType::str_borrowed(""),
    }
}

/// Convert owned AnyValue to JsonType with configurable nesting behavior.
#[inline]
pub fn anyvalue_to_jsontype_owned(value: AnyValue, nested_kv_max_depth: Option<usize>) -> JsonType<'static> {
    match nested_kv_max_depth {
        None | Some(0) => anyvalue_to_jsontype_flat_owned(value),
        Some(max_depth) => anyvalue_to_jsontype_nested_owned(value, 0, max_depth),
    }
}

/// Fast path (owned): convert without recursing into nested structures.
#[inline]
fn anyvalue_to_jsontype_flat_owned(value: AnyValue) -> JsonType<'static> {
    match value.value {
        Some(Value::IntValue(i)) => JsonType::Int(i),
        Some(Value::DoubleValue(d)) => JsonType::Double(d),
        Some(Value::StringValue(s)) => JsonType::str_owned(s),
        Some(Value::BoolValue(b)) => JsonType::Bool(b),
        Some(Value::ArrayValue(a)) => {
            let values = a.values.into_iter()
                .map(|v| match v.value {
                    Some(Value::IntValue(i)) => JsonType::Int(i),
                    Some(Value::DoubleValue(d)) => JsonType::Double(d),
                    Some(Value::StringValue(s)) => JsonType::str_owned(s),
                    Some(Value::BoolValue(b)) => JsonType::Bool(b),
                    Some(Value::ArrayValue(arr)) => JsonType::str_owned(json!(arr).to_string()),
                    Some(Value::KvlistValue(kv)) => JsonType::str_owned(json!(kv).to_string()),
                    Some(Value::BytesValue(b)) => JsonType::str_owned(hex::encode(&b)),
                    None => JsonType::str_owned(String::new()),
                })
                .collect();
            JsonType::Array(values)
        }
        Some(Value::KvlistValue(_)) => JsonType::str_owned(String::new()),
        Some(Value::BytesValue(b)) => JsonType::str_owned(hex::encode(&b)),
        None => JsonType::str_owned(String::new()),
    }
}

/// Nested mode (owned): recursively convert with depth tracking.
fn anyvalue_to_jsontype_nested_owned(value: AnyValue, depth: usize, max_depth: usize) -> JsonType<'static> {
    if depth > max_depth {
        return JsonType::str_owned(json!(value).to_string());
    }

    match value.value {
        Some(Value::IntValue(i)) => JsonType::Int(i),
        Some(Value::DoubleValue(d)) => JsonType::Double(d),
        Some(Value::StringValue(s)) => JsonType::str_owned(s),
        Some(Value::BoolValue(b)) => JsonType::Bool(b),
        Some(Value::ArrayValue(a)) => {
            let values = a.values.into_iter()
                .map(|v| anyvalue_to_jsontype_nested_owned(v, depth + 1, max_depth))
                .collect();
            JsonType::Array(values)
        }
        Some(Value::KvlistValue(kv)) => {
            let entries = kv.values.into_iter()
                .filter_map(|entry| {
                    entry.value.map(|v| (
                        Cow::Owned(entry.key),
                        anyvalue_to_jsontype_nested_owned(v, depth + 1, max_depth),
                    ))
                })
                .collect();
            JsonType::Object(entries)
        }
        Some(Value::BytesValue(b)) => JsonType::str_owned(hex::encode(&b)),
        None => JsonType::str_owned(String::new()),
    }
}

impl<'a> From<&'a AnyValue> for JsonType<'a> {
    /// Default conversion uses flat mode for backwards compatibility.
    fn from(value: &'a AnyValue) -> Self {
        anyvalue_to_jsontype(value, None)
    }
}

impl From<AnyValue> for JsonType<'static> {
    /// Default conversion uses flat mode for backwards compatibility.
    fn from(value: AnyValue) -> Self {
        anyvalue_to_jsontype_owned(value, None)
    }
}

// See the docs here for interpretation of the code values:
// https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding
impl<'a> Serialize for JsonType<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            JsonType::Int(i) => {
                let jsonint = JsonInt64 {
                    code: 0x0a,
                    value: *i,
                };
                jsonint.serialize(serializer)
            }
            JsonType::Str(s) => {
                let jsonstr = JsonStr {
                    code: 0x15,
                    value: s.as_ref(),
                };
                jsonstr.serialize(serializer)
            }
            JsonType::Double(d) => {
                let jsondouble = JsonFloat64 {
                    code: 0x0e,
                    value: *d,
                };
                jsondouble.serialize(serializer)
            }
            JsonType::Bool(b) => {
                let jsonbool = JsonBool {
                    code: 0x2d,
                    value: *b,
                };
                jsonbool.serialize(serializer)
            }
            JsonType::Array(a) => {
                // We always use the Dynamic type here because it is simpler to serialize. Clickhouse
                // will use Nullable(T) in responses if all types are the same, but there doesn't seem
                // to be a cost savings in wire size.
                let jsonarray = JsonArrayDynamic {
                    array_code: 0x1e,
                    dynamic_code: 0x2b,
                    max_types: 0x20,
                    values: a,
                };
                jsonarray.serialize(serializer)
            }
            JsonType::Object(entries) => {
                // Object type code 0x1f for named tuples/objects
                let jsonobject = JsonObject {
                    object_code: 0x1f,
                    entries,
                };
                jsonobject.serialize(serializer)
            }
        }
    }
}

#[derive(Serialize)]
struct JsonStr<'a> {
    code: u8,
    value: &'a str,
}

#[derive(Serialize)]
struct JsonInt64 {
    code: u8,
    value: i64,
}

#[derive(Serialize)]
struct JsonFloat64 {
    code: u8,
    value: f64,
}

#[derive(Serialize)]
struct JsonBool {
    code: u8,
    value: bool,
}

#[derive(Serialize)]
struct JsonArrayDynamic<'a> {
    array_code: u8,
    dynamic_code: u8,
    max_types: u8,
    values: &'a Vec<JsonType<'a>>,
}

/// Serializer for Object type (named tuple in ClickHouse JSON format)
/// Format: object_code (0x1f) | num_entries | [key_len | key_bytes | value]...
struct JsonObject<'a> {
    object_code: u8,
    entries: &'a Vec<(Cow<'a, str>, JsonType<'a>)>,
}

impl<'a> Serialize for JsonObject<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeTuple;
        // Object format: code | num_keys | [key_len | key_bytes | value_type | value]...
        let num_entries = self.entries.len();
        let mut seq = serializer.serialize_tuple(2 + num_entries * 2)?;
        seq.serialize_element(&self.object_code)?;
        seq.serialize_element(&(num_entries as u8))?;
        for (key, value) in self.entries.iter() {
            // Serialize key as length-prefixed string (Cow dereferences to &str)
            seq.serialize_element(&JsonStr {
                code: 0x00, // No type code for object keys, just length + bytes
                value: key.as_ref(),
            })?;
            // Serialize the value with its type
            seq.serialize_element(value)?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::exporters::clickhouse::rowbinary::serialize_into;

    use super::*;

    /// Test that verifies the byte patterns for JsonType::Int serialization.
    /// This test prevents regressions in the wire format by ensuring the serialized
    /// bytes match the expected pattern: [type_code(0x0a), value_bytes(little_endian)]
    #[test]
    fn test_jsontype_int_serialization() {
        let json_int = JsonType::Int(42);
        let serialized = serialize_to_bytes(json_int);

        // Expected bytes: type code 0x0a + 42 as i64 little endian
        let expected: Vec<u8> = vec![
            0x0a, // JsonInt type code
            0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 42 as i64 little endian
        ];

        assert_eq!(
            serialized, expected,
            "JsonType::Int(42) serialization bytes mismatch"
        );
    }

    /// Test that verifies the byte patterns for JsonType::Int with negative values.
    #[test]
    fn test_jsontype_int_negative_serialization() {
        let json_int = JsonType::Int(-123);
        let serialized = serialize_to_bytes(json_int);

        // Expected bytes: type code 0x0a + (-123) as i64 little endian
        let expected: Vec<u8> = vec![
            0x0a, // JsonInt type code
            0x85, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // -123 as i64 little endian
        ];

        assert_eq!(
            serialized, expected,
            "JsonType::Int(-123) serialization bytes mismatch"
        );
    }

    /// Test that verifies the byte patterns for JsonType::Str serialization (borrowed).
    /// This test prevents regressions in the wire format by ensuring the serialized
    /// bytes match the expected pattern: [type_code(0x15), string_length, string_bytes]
    #[test]
    fn test_jsontype_str_serialization() {
        let json_str = JsonType::str_borrowed("hello");
        let serialized = serialize_to_bytes(json_str);

        // Expected bytes: type code 0x15 + string length (5 as u8) + "hello" bytes
        let expected: Vec<u8> = vec![
            0x15, // JsonStr type code
            0x05, // length 5 as u8
            b'h', b'e', b'l', b'l', b'o', // "hello" as bytes
        ];

        assert_eq!(
            serialized, expected,
            "JsonType::str_borrowed(\"hello\") serialization bytes mismatch"
        );
    }

    /// Test that verifies the byte patterns for JsonType::Str with empty string.
    #[test]
    fn test_jsontype_str_empty_serialization() {
        let json_str = JsonType::str_borrowed("");
        let serialized = serialize_to_bytes(json_str);

        // Expected bytes: type code 0x15 + string length (0 as u8)
        let expected: Vec<u8> = vec![
            0x15, // JsonStr type code
            0x00, // length 0 as u8
        ];

        assert_eq!(
            serialized, expected,
            "JsonType::str_borrowed(\"\") serialization bytes mismatch"
        );
    }

    /// Test that verifies the byte patterns for JsonType::Str (owned) serialization.
    /// This should produce the same byte pattern as borrowed Str.
    #[test]
    fn test_jsontype_str_owned_serialization() {
        let json_str_owned = JsonType::str_owned("world".to_string());
        let serialized = serialize_to_bytes(json_str_owned);

        // Expected bytes: type code 0x15 + string length (5 as u8) + "world" bytes
        let expected: Vec<u8> = vec![
            0x15, // JsonStr type code
            0x05, // length 5 as u8
            b'w', b'o', b'r', b'l', b'd', // "world" as bytes
        ];

        assert_eq!(
            serialized, expected,
            "JsonType::str_owned(\"world\") serialization bytes mismatch"
        );
    }

    /// Test that verifies the byte patterns for JsonType::Double serialization.
    /// This test prevents regressions in the wire format by ensuring the serialized
    /// bytes match the expected pattern: [type_code(0x0e), value_bytes(little_endian)]
    #[test]
    fn test_jsontype_double_serialization() {
        let json_double = JsonType::Double(std::f64::consts::PI);
        let serialized = serialize_to_bytes(json_double);

        // Expected bytes: type code 0x0e + 3.14159 as f64 little endian
        let pi_bytes = std::f64::consts::PI.to_le_bytes();
        let mut expected = vec![0x0e]; // JsonDouble type code
        expected.extend_from_slice(&pi_bytes);

        assert_eq!(
            serialized, expected,
            "JsonType::Double(3.14159) serialization bytes mismatch"
        );
    }

    /// Test that verifies the byte patterns for JsonType::Double with special values.
    #[test]
    fn test_jsontype_double_special_values_serialization() {
        // Test positive infinity
        let json_double_inf = JsonType::Double(f64::INFINITY);
        let serialized_inf = serialize_to_bytes(json_double_inf);
        let mut expected_inf = vec![0x0e]; // JsonDouble type code
        expected_inf.extend_from_slice(&f64::INFINITY.to_le_bytes());
        assert_eq!(
            serialized_inf, expected_inf,
            "JsonType::Double(INFINITY) serialization bytes mismatch"
        );

        // Test negative infinity
        let json_double_neg_inf = JsonType::Double(f64::NEG_INFINITY);
        let serialized_neg_inf = serialize_to_bytes(json_double_neg_inf);
        let mut expected_neg_inf = vec![0x0e]; // JsonDouble type code
        expected_neg_inf.extend_from_slice(&f64::NEG_INFINITY.to_le_bytes());
        assert_eq!(
            serialized_neg_inf, expected_neg_inf,
            "JsonType::Double(NEG_INFINITY) serialization bytes mismatch"
        );

        // Test zero
        let json_double_zero = JsonType::Double(0.0);
        let serialized_zero = serialize_to_bytes(json_double_zero);
        let mut expected_zero = vec![0x0e]; // JsonDouble type code
        expected_zero.extend_from_slice(&0.0_f64.to_le_bytes());
        assert_eq!(
            serialized_zero, expected_zero,
            "JsonType::Double(0.0) serialization bytes mismatch"
        );
    }

    #[test]
    fn test_jsontype_array_serialization() {
        // Same types
        let json_array = JsonType::Array(vec![JsonType::Int(3), JsonType::Int(5)]);
        let serialized_array = serialize_to_bytes(json_array);

        let expected: Vec<u8> = vec![
            0x1e, 0x2b, 0x20, 0x02, 0x0a, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
            0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_eq!(serialized_array, expected);

        // Different types
        let json_array = JsonType::Array(vec![JsonType::Int(3), JsonType::str_borrowed("hello")]);
        let serialized_array = serialize_to_bytes(json_array);

        let expected: Vec<u8> = vec![
            0x1e, 0x2b, 0x20, 0x02, 0x0a, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15,
            0x05, b'h', b'e', b'l', b'l', b'o',
        ];

        assert_eq!(serialized_array, expected);
    }

    /// Test that verifies the byte patterns for JsonType::Bool serialization.
    #[test]
    fn test_jsontype_bool_serialization() {
        let json_bool_true = JsonType::Bool(true);
        let serialized_true = serialize_to_bytes(json_bool_true);

        // Expected bytes: type code 0x2d + true as u8 (0x01)
        let expected_true: Vec<u8> = vec![
            0x2d, // JsonBool type code
            0x01, // true as u8
        ];

        assert_eq!(
            serialized_true, expected_true,
            "JsonType::Bool(true) serialization bytes mismatch"
        );

        let json_bool_false = JsonType::Bool(false);
        let serialized_false = serialize_to_bytes(json_bool_false);

        // Expected bytes: type code 0x2d + false as u8 (0x00)
        let expected_false: Vec<u8> = vec![
            0x2d, // JsonBool type code
            0x00, // false as u8
        ];

        assert_eq!(
            serialized_false, expected_false,
            "JsonType::Bool(false) serialization bytes mismatch"
        );
    }

    /// Test flat mode (backwards compatible) - KvlistValue and nested ArrayValue become JSON strings
    #[test]
    fn test_anyvalue_arrayvalue_flat_mode() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValue, KeyValueList};

        let mut kv_list = KeyValueList::default();
        kv_list.values.push(KeyValue {
            key: "nested_key".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("nested_value".to_string())),
            }),
        });

        let array_value = ArrayValue {
            values: vec![
                AnyValue { value: Some(Value::IntValue(42)) },
                AnyValue { value: Some(Value::StringValue("test_string".to_string())) },
                AnyValue { value: Some(Value::KvlistValue(kv_list)) },
                AnyValue { value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue { value: Some(Value::IntValue(100)) }],
                })) },
            ],
        };

        let any_value = AnyValue { value: Some(Value::ArrayValue(array_value)) };

        // Default From uses flat mode (None)
        let json_type: JsonType = (&any_value).into();

        match json_type {
            JsonType::Array(values) => {
                assert_eq!(values.len(), 4);

                // Simple types are converted directly
                match &values[0] {
                    JsonType::Int(i) => assert_eq!(*i, 42),
                    _ => panic!("Expected Int(42) at index 0"),
                }
                match &values[1] {
                    JsonType::Str(s) => assert_eq!(s.as_ref(), "test_string"),
                    _ => panic!("Expected Str at index 1"),
                }

                // KvlistValue becomes a JSON string in flat mode
                match &values[2] {
                    JsonType::Str(s) => {
                        assert!(s.as_ref().contains("nested_key"));
                        assert!(s.as_ref().contains("nested_value"));
                    }
                    _ => panic!("Expected Str (JSON string) at index 2 in flat mode, got {:?}", &values[2]),
                }

                // Nested ArrayValue becomes a JSON string in flat mode
                match &values[3] {
                    JsonType::Str(s) => {
                        assert!(s.as_ref().contains("100"));
                    }
                    _ => panic!("Expected Str (JSON string) at index 3 in flat mode, got {:?}", &values[3]),
                }
            }
            _ => panic!("Expected JsonType::Array"),
        }
    }

    /// Test nested mode - KvlistValue becomes Object, nested ArrayValue stays Array
    #[test]
    fn test_anyvalue_arrayvalue_nested_mode() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValue, KeyValueList};

        let mut kv_list = KeyValueList::default();
        kv_list.values.push(KeyValue {
            key: "nested_key".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("nested_value".to_string())),
            }),
        });

        let array_value = ArrayValue {
            values: vec![
                AnyValue { value: Some(Value::IntValue(42)) },
                AnyValue { value: Some(Value::StringValue("test_string".to_string())) },
                AnyValue { value: Some(Value::DoubleValue(3.14)) },
                AnyValue { value: Some(Value::BoolValue(true)) },
                AnyValue { value: Some(Value::KvlistValue(kv_list)) },
                AnyValue { value: Some(Value::BytesValue(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f])) },
                AnyValue { value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue { value: Some(Value::IntValue(100)) }],
                })) },
            ],
        };

        let any_value = AnyValue { value: Some(Value::ArrayValue(array_value)) };

        // Use nested mode with max depth 10
        let json_type = anyvalue_to_jsontype(&any_value, Some(10));

        match json_type {
            JsonType::Array(values) => {
                assert_eq!(values.len(), 7);

                match &values[0] {
                    JsonType::Int(i) => assert_eq!(*i, 42),
                    _ => panic!("Expected Int(42) at index 0"),
                }
                match &values[1] {
                    JsonType::Str(s) => assert_eq!(s.as_ref(), "test_string"),
                    _ => panic!("Expected Str at index 1"),
                }
                match &values[2] {
                    JsonType::Double(d) => assert_eq!(*d, 3.14),
                    _ => panic!("Expected Double at index 2"),
                }
                match &values[3] {
                    JsonType::Bool(b) => assert!(b),
                    _ => panic!("Expected Bool at index 3"),
                }

                // KvlistValue becomes Object in nested mode
                match &values[4] {
                    JsonType::Object(entries) => {
                        assert_eq!(entries.len(), 1);
                        assert_eq!(entries[0].0.as_ref(), "nested_key");
                        match &entries[0].1 {
                            JsonType::Str(s) => assert_eq!(s.as_ref(), "nested_value"),
                            _ => panic!("Expected nested_key to have Str value"),
                        }
                    }
                    _ => panic!("Expected Object at index 4 in nested mode, got {:?}", &values[4]),
                }

                match &values[5] {
                    JsonType::Str(s) => assert_eq!(s.as_ref(), "48656c6c6f"),
                    _ => panic!("Expected Str (hex) at index 5"),
                }

                // Nested ArrayValue stays as Array in nested mode
                match &values[6] {
                    JsonType::Array(nested) => {
                        assert_eq!(nested.len(), 1);
                        match &nested[0] {
                            JsonType::Int(i) => assert_eq!(*i, 100),
                            _ => panic!("Expected Int(100) in nested array"),
                        }
                    }
                    _ => panic!("Expected Array at index 6 in nested mode, got {:?}", &values[6]),
                }
            }
            _ => panic!("Expected JsonType::Array"),
        }
    }

    fn serialize_to_bytes(value: JsonType) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(512);
        serialize_into(&mut buf, &value).unwrap();
        buf.to_vec()
    }

    /// Debug test to verify and print actual serialized bytes for manual inspection.
    /// This test can be used to understand the exact byte patterns produced.
    #[test]
    #[ignore] // Use `cargo test -- --ignored` to run this debug test
    fn debug_jsontype_serialization_bytes() {
        let test_cases = vec![
            ("Int(42)", JsonType::Int(42)),
            ("Str(borrowed)", JsonType::str_borrowed("hello")),
            ("Str(owned)", JsonType::str_owned("world".to_string())),
            ("Double(3.14)", JsonType::Double(std::f64::consts::PI)),
        ];

        for (name, json_type) in test_cases {
            let bytes = serialize_to_bytes(json_type);
            println!("{}: {:02x?}", name, bytes);
            println!("{}: {} bytes total", name, bytes.len());

            // Print as Rust array literal for easy copy-paste into tests
            print!("{}: vec![", name);
            for (i, byte) in bytes.iter().enumerate() {
                if i > 0 {
                    print!(", ");
                }
                print!("0x{:02x}", byte);
            }
            println!("];");
            println!();
        }
    }
}
