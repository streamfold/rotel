use serde::{Serialize, Serializer};

use crate::otlp::cvattr::ConvertedAttrValue;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use serde_json::json;

#[derive(Debug)]
pub enum JsonType<'a> {
    Int(i64),
    Str(&'a str),
    StrOwned(String),
    Double(f64),
    Bool(bool),
    Array(Vec<JsonType<'a>>),
}

impl<'a> From<&'a ConvertedAttrValue> for JsonType<'a> {
    fn from(value: &'a ConvertedAttrValue) -> Self {
        match value {
            ConvertedAttrValue::Int(i) => JsonType::Int(*i),
            ConvertedAttrValue::String(s) => JsonType::Str(s.as_str()),
            ConvertedAttrValue::Double(d) => JsonType::Double(*d),
        }
    }
}

impl From<ConvertedAttrValue> for JsonType<'static> {
    fn from(value: ConvertedAttrValue) -> Self {
        match value {
            ConvertedAttrValue::Int(i) => JsonType::Int(i),
            ConvertedAttrValue::String(s) => JsonType::StrOwned(s),
            ConvertedAttrValue::Double(d) => JsonType::Double(d),
        }
    }
}

impl<'a> From<&'a AnyValue> for JsonType<'a> {
    fn from(value: &'a AnyValue) -> Self {
        match &value.value {
            Some(Value::IntValue(i)) => JsonType::Int(*i),
            Some(Value::DoubleValue(d)) => JsonType::Double(*d),
            Some(Value::StringValue(s)) => JsonType::Str(s.as_str()),
            Some(Value::BoolValue(b)) => JsonType::Bool(*b),
            Some(Value::ArrayValue(a)) => {
                // We support arrays with all simple values, no recursive nesting
                let values = a
                    .values
                    .iter()
                    .map(|v| match &v.value {
                        Some(Value::IntValue(i)) => JsonType::Int(*i),
                        Some(Value::DoubleValue(d)) => JsonType::Double(*d),
                        Some(Value::StringValue(s)) => JsonType::Str(s.as_str()),
                        Some(Value::BoolValue(b)) => JsonType::Bool(*b),
                        Some(Value::ArrayValue(a)) => JsonType::StrOwned(json!(a).to_string()),
                        Some(Value::KvlistValue(kv)) => JsonType::StrOwned(json!(kv).to_string()),
                        Some(Value::BytesValue(b)) => JsonType::StrOwned(hex::encode(b)),
                        None => JsonType::Str(""),
                    })
                    .collect();
                JsonType::Array(values)
            }
            Some(Value::KvlistValue(_kv)) => {
                // KvlistValue should be flattened before reaching this point
                // This case should not occur in normal operation
                JsonType::Str("")
            }
            Some(Value::BytesValue(b)) => JsonType::StrOwned(hex::encode(b)),
            None => JsonType::Str(""),
        }
    }
}

impl From<AnyValue> for JsonType<'static> {
    fn from(value: AnyValue) -> Self {
        match value.value {
            Some(Value::IntValue(i)) => JsonType::Int(i),
            Some(Value::DoubleValue(d)) => JsonType::Double(d),
            Some(Value::StringValue(s)) => JsonType::StrOwned(s),
            Some(Value::BoolValue(b)) => JsonType::Bool(b),
            Some(Value::ArrayValue(a)) => {
                // We support arrays with all simple values, no recursive nesting
                let values = a
                    .values
                    .into_iter()
                    .map(|v| match v.value {
                        Some(Value::IntValue(i)) => JsonType::Int(i),
                        Some(Value::DoubleValue(d)) => JsonType::Double(d),
                        Some(Value::StringValue(s)) => JsonType::StrOwned(s),
                        Some(Value::BoolValue(b)) => JsonType::Bool(b),
                        Some(Value::ArrayValue(a)) => JsonType::StrOwned(json!(a).to_string()),
                        Some(Value::KvlistValue(kv)) => JsonType::StrOwned(json!(kv).to_string()),
                        Some(Value::BytesValue(b)) => JsonType::StrOwned(hex::encode(b)),
                        None => JsonType::Str(""),
                    })
                    .collect();
                JsonType::Array(values)
            }
            Some(Value::KvlistValue(_kv)) => {
                // KvlistValue should be flattened before reaching this point
                // This case should not occur in normal operation
                JsonType::StrOwned(String::new())
            }
            Some(Value::BytesValue(b)) => JsonType::StrOwned(hex::encode(&b)),
            None => JsonType::StrOwned(String::new()),
        }
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
                    value: s,
                };
                jsonstr.serialize(serializer)
            }
            JsonType::StrOwned(s) => {
                let jsonstr = JsonStr {
                    code: 0x15,
                    value: &s,
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
                // We always use the Dyanmic type here because it is simpler to serialize. Clickhouse
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

    /// Test that verifies the byte patterns for JsonType::Str serialization.
    /// This test prevents regressions in the wire format by ensuring the serialized
    /// bytes match the expected pattern: [type_code(0x15), string_length, string_bytes]
    #[test]
    fn test_jsontype_str_serialization() {
        let json_str = JsonType::Str("hello");
        let serialized = serialize_to_bytes(json_str);

        // Expected bytes: type code 0x15 + string length (5 as u8) + "hello" bytes
        let expected: Vec<u8> = vec![
            0x15, // JsonStr type code
            0x05, // length 5 as u8
            b'h', b'e', b'l', b'l', b'o', // "hello" as bytes
        ];

        assert_eq!(
            serialized, expected,
            "JsonType::Str(\"hello\") serialization bytes mismatch"
        );
    }

    /// Test that verifies the byte patterns for JsonType::Str with empty string.
    #[test]
    fn test_jsontype_str_empty_serialization() {
        let json_str = JsonType::Str("");
        let serialized = serialize_to_bytes(json_str);

        // Expected bytes: type code 0x15 + string length (0 as u8)
        let expected: Vec<u8> = vec![
            0x15, // JsonStr type code
            0x00, // length 0 as u8
        ];

        assert_eq!(
            serialized, expected,
            "JsonType::Str(\"\") serialization bytes mismatch"
        );
    }

    /// Test that verifies the byte patterns for JsonType::StrOwned serialization.
    /// This should produce the same byte pattern as JsonType::Str.
    #[test]
    fn test_jsontype_str_owned_serialization() {
        let json_str_owned = JsonType::StrOwned("world".to_string());
        let serialized = serialize_to_bytes(json_str_owned);

        // Expected bytes: type code 0x15 + string length (5 as u8) + "world" bytes
        let expected: Vec<u8> = vec![
            0x15, // JsonStr type code
            0x05, // length 5 as u8
            b'w', b'o', b'r', b'l', b'd', // "world" as bytes
        ];

        assert_eq!(
            serialized, expected,
            "JsonType::StrOwned(\"world\") serialization bytes mismatch"
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
        let json_array = JsonType::Array(vec![JsonType::Int(3), JsonType::Str("hello")]);
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

    #[test]
    fn test_anyvalue_arrayvalue_conversion() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValue, KeyValueList};

        // Create an ArrayValue with mixed types including KvlistValue
        let mut kv_list = KeyValueList::default();
        kv_list.values.push(KeyValue {
            key: "nested_key".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("nested_value".to_string())),
            }),
        });

        let array_value = ArrayValue {
            values: vec![
                AnyValue {
                    value: Some(Value::IntValue(42)),
                },
                AnyValue {
                    value: Some(Value::StringValue("test_string".to_string())),
                },
                AnyValue {
                    value: Some(Value::DoubleValue(3.14)),
                },
                AnyValue {
                    value: Some(Value::BoolValue(true)),
                },
                AnyValue {
                    value: Some(Value::KvlistValue(kv_list)),
                },
                AnyValue {
                    value: Some(Value::BytesValue(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f])), // "Hello"
                },
                AnyValue {
                    value: Some(Value::ArrayValue(ArrayValue {
                        values: vec![AnyValue {
                            value: Some(Value::IntValue(100)),
                        }],
                    })),
                },
            ],
        };

        let any_value = AnyValue {
            value: Some(Value::ArrayValue(array_value)),
        };

        let json_type: JsonType = (&any_value).into();

        match json_type {
            JsonType::Array(values) => {
                assert_eq!(values.len(), 7);

                // Check Int value
                match &values[0] {
                    JsonType::Int(i) => assert_eq!(*i, 42),
                    _ => panic!("Expected Int(42) at index 0"),
                }

                // Check String value
                match &values[1] {
                    JsonType::Str(s) => assert_eq!(*s, "test_string"),
                    _ => panic!("Expected Str(\"test_string\") at index 1"),
                }

                // Check Double value
                match &values[2] {
                    JsonType::Double(d) => assert_eq!(*d, 3.14),
                    _ => panic!("Expected Double(3.14) at index 2"),
                }

                // Check Bool value
                match &values[3] {
                    JsonType::Bool(b) => assert!(b),
                    _ => panic!("Expected Bool(true) at index 3"),
                }

                // Check KvlistValue (converted to JSON string)
                match &values[4] {
                    JsonType::StrOwned(s) => {
                        // The exact JSON string format may vary, but it should contain the key-value pair
                        assert!(s.contains("nested_key"));
                        assert!(s.contains("nested_value"));
                    }
                    _ => panic!("Expected StrOwned with JSON string at index 4"),
                }

                // Check BytesValue (hex encoded)
                match &values[5] {
                    JsonType::StrOwned(s) => assert_eq!(s, "48656c6c6f"), // "Hello" in hex
                    _ => panic!("Expected StrOwned with hex string at index 5"),
                }

                // Check nested ArrayValue (converted to JSON string)
                match &values[6] {
                    JsonType::StrOwned(s) => {
                        // Should be a JSON representation of the nested array
                        assert!(s.contains("100"));
                    }
                    _ => panic!("Expected StrOwned with JSON array string at index 6"),
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
            ("Str(\"hello\")", JsonType::Str("hello")),
            (
                "StrOwned(\"world\")",
                JsonType::StrOwned("world".to_string()),
            ),
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
