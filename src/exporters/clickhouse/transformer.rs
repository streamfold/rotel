use crate::exporters::clickhouse::Compression;
use crate::exporters::clickhouse::rowbinary::json::JsonType;
use crate::exporters::clickhouse::schema::MapOrJson;

use opentelemetry_proto::tonic::common::v1::KeyValue;
use std::borrow::Cow;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Transformer {
    pub(crate) compression: Compression,
    use_json: bool,
    /// Maximum depth for nested KV list conversion.
    /// - `None` or `Some(0)`: flat mode (backwards compatible, nested KV serialized as JSON strings)
    /// - `Some(n)` where n > 0: recursive conversion up to depth n
    nested_kv_max_depth: usize,
    /// Whether the logs table has the extended EventName column.
    pub(crate) logs_extended: bool,
}

impl Transformer {
    pub fn new(compression: Compression, use_json: bool) -> Self {
        Self {
            compression,
            use_json,
            nested_kv_max_depth: 0, // Default: backwards compatible flat mode
            logs_extended: false,
        }
    }

    pub fn with_nested_kv_max_depth(mut self, max_depth: usize) -> Self {
        self.nested_kv_max_depth = max_depth;
        self
    }

    pub fn with_logs_extended(mut self, extended: bool) -> Self {
        self.logs_extended = extended;
        self
    }
}

impl Transformer {
    pub(crate) fn transform_attrs_kv<'a>(&self, attrs: &'a [KeyValue]) -> MapOrJson<'a> {
        match self.use_json {
            true => MapOrJson::Json(self.build_json_attrs_kv(attrs)),
            false => MapOrJson::Map(self.build_map_attrs_kv(attrs)),
        }
    }

    fn build_map_attrs_kv(&self, attrs: &[KeyValue]) -> Vec<(String, String)> {
        let mut result = Vec::new();
        self.flatten_keyvalues_map(attrs, String::new(), &mut result);
        result
    }

    fn flatten_keyvalues_map(
        &self,
        attrs: &[KeyValue],
        prefix: String,
        result: &mut Vec<(String, String)>,
    ) {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        for kv in attrs {
            let full_key = if prefix.is_empty() {
                kv.key.clone()
            } else {
                format!("{}.{}", prefix, kv.key)
            };

            if let Some(any_value) = &kv.value {
                match &any_value.value {
                    Some(Value::KvlistValue(kvlist)) if self.nested_kv_max_depth == 0 => {
                        // Flat mode: recursively expand KvlistValue into dotted paths
                        self.flatten_keyvalues_map(&kvlist.values, full_key, result);
                    }
                    Some(val) => {
                        result.push((full_key, Self::anyvalue_to_string(val)));
                    }
                    None => {}
                }
            }
        }
    }

    fn build_json_attrs_kv<'a>(
        &self,
        attrs: &'a [KeyValue],
    ) -> HashMap<Cow<'a, str>, JsonType<'a>> {
        let mut hm = HashMap::new();
        self.flatten_keyvalues_borrowed(attrs, String::new(), &mut hm);
        hm
    }

    fn flatten_keyvalues_borrowed<'a>(
        &self,
        attrs: &'a [KeyValue],
        prefix: String,
        result: &mut HashMap<Cow<'a, str>, JsonType<'a>>,
    ) {
        use crate::exporters::clickhouse::rowbinary::json::anyvalue_to_jsontype;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        for kv in attrs {
            if let Some(any_value) = &kv.value {
                match &any_value.value {
                    Some(Value::KvlistValue(kvlist)) if self.nested_kv_max_depth == 0 => {
                        // Flat mode: recursively expand KvlistValue into dotted paths
                        let full_key = if prefix.is_empty() {
                            kv.key.clone()
                        } else {
                            format!("{}.{}", prefix, kv.key)
                        };
                        self.flatten_keyvalues_borrowed(&kvlist.values, full_key, result);
                    }
                    Some(_) => {
                        // Nested mode (max_depth > 0): let anyvalue_to_jsontype handle
                        // KvlistValue → JsonType::Object conversion with depth tracking.
                        // Also handles all other value types.
                        let key = if prefix.is_empty() {
                            Cow::Borrowed(kv.key.as_str())
                        } else {
                            Cow::Owned(format!("{}.{}", prefix, kv.key))
                        };
                        result.insert(
                            key,
                            anyvalue_to_jsontype(any_value, Some(self.nested_kv_max_depth)),
                        );
                    }
                    None => {}
                }
            }
        }
    }

    pub(crate) fn transform_attrs_kv_owned(&self, attrs: &[KeyValue]) -> MapOrJson<'static> {
        match self.use_json {
            true => MapOrJson::JsonOwned(self.build_json_attrs_kv_owned(attrs)),
            false => MapOrJson::Map(self.build_map_attrs_kv(attrs)),
        }
    }

    fn build_json_attrs_kv_owned(&self, attrs: &[KeyValue]) -> HashMap<String, JsonType<'static>> {
        let mut hm = HashMap::new();
        self.flatten_keyvalues_owned(attrs, String::new(), &mut hm);
        hm
    }

    fn flatten_keyvalues_owned(
        &self,
        attrs: &[KeyValue],
        prefix: String,
        result: &mut HashMap<String, JsonType<'static>>,
    ) {
        use crate::exporters::clickhouse::rowbinary::json::anyvalue_to_jsontype_owned;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        for kv in attrs {
            let full_key = if prefix.is_empty() {
                kv.key.clone()
            } else {
                format!("{}.{}", prefix, kv.key)
            };

            if let Some(any_value) = &kv.value {
                match &any_value.value {
                    Some(Value::KvlistValue(kvlist)) if self.nested_kv_max_depth == 0 => {
                        // Flat mode: recursively expand KvlistValue into dotted paths
                        self.flatten_keyvalues_owned(&kvlist.values, full_key, result);
                    }
                    Some(_) => {
                        // Nested mode (max_depth > 0): let anyvalue_to_jsontype_owned
                        // handle KvlistValue → Object conversion with depth tracking.
                        result.insert(
                            full_key,
                            anyvalue_to_jsontype_owned(
                                any_value.clone(),
                                Some(self.nested_kv_max_depth),
                            ),
                        );
                    }
                    None => {}
                }
            }
        }
    }

    fn anyvalue_to_string(
        val: &opentelemetry_proto::tonic::common::v1::any_value::Value,
    ) -> String {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use serde_json::json;

        match val {
            Value::StringValue(s) => s.clone(),
            Value::BoolValue(b) => b.to_string(),
            Value::IntValue(i) => i.to_string(),
            Value::DoubleValue(d) => json!(d).to_string(),
            Value::ArrayValue(a) => json!(a).to_string(),
            Value::KvlistValue(kv) => json!(kv).to_string(),
            Value::BytesValue(b) => hex::encode(b),
        }
    }
}

pub(crate) fn find_str_attribute_kv<'a>(attr: &str, attributes: &'a [KeyValue]) -> Cow<'a, str> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;

    attributes
        .iter()
        .find(|kv| kv.key == attr)
        .and_then(|kv| {
            kv.value.as_ref().and_then(|v| {
                v.value.as_ref().map(|val| match val {
                    Value::StringValue(s) => Cow::Borrowed(s.as_str()),
                    _ => Cow::Borrowed(""),
                })
            })
        })
        .unwrap_or(Cow::Borrowed(""))
}

pub(crate) fn encode_id<'a>(id: &[u8], out: &'a mut [u8]) -> &'a str {
    match hex::encode_to_slice(id, out) {
        Ok(_) => {
            // We can be pretty sure the encoded string is utf8 safe
            std::str::from_utf8(out).unwrap_or_default()
        }
        Err(_) => {
            // Trace and Span IDs are required to have a certain length (8 or 16 bytes), the only
            // case this should fail is on an empty ID, like parent_span_id on a root span.
            ""
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_attrs_lifetime_correctness() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true);

        // Create some test attributes
        let attrs = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("test-service".to_string())),
                }),
            },
            KeyValue {
                key: "http.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(200)),
                }),
            },
            KeyValue {
                key: "duration".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::DoubleValue(1.23)),
                }),
            },
        ];

        // This should work without cloning the values - the returned MapOrJson
        // should maintain references to the original data where possible
        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                assert_eq!(map.len(), 3);
                assert!(map.contains_key("service.name"));
                assert!(map.contains_key("http.status_code"));
                assert!(map.contains_key("duration"));
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_transform_attrs_map_variant() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, false);

        let attrs = vec![
            KeyValue {
                key: "key1".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("value1".to_string())),
                }),
            },
            KeyValue {
                key: "key2".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(42)),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Map(vec) => {
                assert_eq!(vec.len(), 2);
                assert!(vec.contains(&("key1".to_string(), "value1".to_string())));
                assert!(vec.contains(&("key2".to_string(), "42".to_string())));
            }
            _ => panic!("Expected Map variant"),
        }
    }

    #[test]
    fn test_transform_attrs_owned_lifetime_correctness() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true);

        // Create some test attributes
        let attrs = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("test-service".to_string())),
                }),
            },
            KeyValue {
                key: "http.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(200)),
                }),
            },
            KeyValue {
                key: "duration".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::DoubleValue(1.23)),
                }),
            },
        ];

        // This should work with owned data - the returned MapOrJson
        // should contain owned data that doesn't reference the input
        let result = transformer.transform_attrs_kv_owned(&attrs);

        match result {
            MapOrJson::JsonOwned(map) => {
                assert_eq!(map.len(), 3);
                assert!(map.contains_key("service.name"));
                assert!(map.contains_key("http.status_code"));
                assert!(map.contains_key("duration"));

                // Verify the values are correct and owned
                match &map["service.name"] {
                    JsonType::Str(s) => assert_eq!(s.as_ref(), "test-service"),
                    _ => panic!("Expected Str variant"),
                }
                match &map["http.status_code"] {
                    JsonType::Int(i) => assert_eq!(*i, 200),
                    _ => panic!("Expected Int variant"),
                }
                match &map["duration"] {
                    JsonType::Double(d) => assert_eq!(*d, 1.23),
                    _ => panic!("Expected Double variant"),
                }
            }
            _ => panic!("Expected JsonOwned variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_json_variant() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true);

        let attrs = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("test-service".to_string())),
                }),
            },
            KeyValue {
                key: "http.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(200)),
                }),
            },
            KeyValue {
                key: "duration".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::DoubleValue(1.23)),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                assert_eq!(map.len(), 3);
                assert!(map.contains_key("service.name"));
                assert!(map.contains_key("http.status_code"));
                assert!(map.contains_key("duration"));
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_map_variant() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, false);

        let attrs = vec![
            KeyValue {
                key: "key1".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("value1".to_string())),
                }),
            },
            KeyValue {
                key: "key2".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(42)),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Map(vec) => {
                assert_eq!(vec.len(), 2);
                assert!(vec.contains(&("key1".to_string(), "value1".to_string())));
                assert!(vec.contains(&("key2".to_string(), "42".to_string())));
            }
            _ => panic!("Expected Map variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_owned() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true);

        let attrs = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("test-service".to_string())),
                }),
            },
            KeyValue {
                key: "http.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(200)),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv_owned(&attrs);

        match result {
            MapOrJson::JsonOwned(map) => {
                assert_eq!(map.len(), 2);
                assert!(map.contains_key("service.name"));
                assert!(map.contains_key("http.status_code"));
            }
            _ => panic!("Expected JsonOwned variant"),
        }
    }

    #[test]
    fn test_find_str_attribute_kv() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let attrs = vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("my-service".to_string())),
                }),
            },
            KeyValue {
                key: "http.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(200)),
                }),
            },
        ];

        let result = find_str_attribute_kv("service.name", &attrs);
        assert_eq!(result, "my-service");

        let result = find_str_attribute_kv("http.status_code", &attrs);
        assert_eq!(result, "");

        let result = find_str_attribute_kv("nonexistent", &attrs);
        assert_eq!(result, "");
    }

    #[test]
    fn test_transform_attrs_kv_flatten_kvlist() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true);

        let attrs = vec![
            KeyValue {
                key: "simple".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("value".to_string())),
                }),
            },
            KeyValue {
                key: "metadata".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::KvlistValue(KeyValueList {
                        values: vec![
                            KeyValue {
                                key: "region".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("us-east-1".to_string())),
                                }),
                            },
                            KeyValue {
                                key: "zone".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("a".to_string())),
                                }),
                            },
                        ],
                    })),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                assert_eq!(map.len(), 3);
                assert!(map.contains_key("simple"));
                assert!(map.contains_key("metadata.region"));
                assert!(map.contains_key("metadata.zone"));
                assert!(!map.contains_key("metadata"));
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_flatten_nested_kvlist() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true);

        let attrs = vec![KeyValue {
            key: "outer".to_string(),
            value: Some(AnyValue {
                value: Some(Value::KvlistValue(KeyValueList {
                    values: vec![
                        KeyValue {
                            key: "middle".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::KvlistValue(KeyValueList {
                                    values: vec![KeyValue {
                                        key: "inner".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("deep".to_string())),
                                        }),
                                    }],
                                })),
                            }),
                        },
                        KeyValue {
                            key: "sibling".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::IntValue(42)),
                            }),
                        },
                    ],
                })),
            }),
        }];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                assert_eq!(map.len(), 2);
                assert!(map.contains_key("outer.middle.inner"));
                assert!(map.contains_key("outer.sibling"));
                assert!(!map.contains_key("outer"));
                assert!(!map.contains_key("outer.middle"));
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_owned_flatten_kvlist() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true);

        let attrs = vec![
            KeyValue {
                key: "simple".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("value".to_string())),
                }),
            },
            KeyValue {
                key: "metadata".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::KvlistValue(KeyValueList {
                        values: vec![
                            KeyValue {
                                key: "region".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("us-east-1".to_string())),
                                }),
                            },
                            KeyValue {
                                key: "zone".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::IntValue(1)),
                                }),
                            },
                        ],
                    })),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv_owned(&attrs);

        match result {
            MapOrJson::JsonOwned(map) => {
                assert_eq!(map.len(), 3);
                assert!(map.contains_key("simple"));
                assert!(map.contains_key("metadata.region"));
                assert!(map.contains_key("metadata.zone"));
                assert!(!map.contains_key("metadata"));
            }
            _ => panic!("Expected JsonOwned variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_map_variant_flatten_kvlist() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, false);

        let attrs = vec![
            KeyValue {
                key: "simple".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("value".to_string())),
                }),
            },
            KeyValue {
                key: "metadata".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::KvlistValue(KeyValueList {
                        values: vec![
                            KeyValue {
                                key: "region".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("us-east-1".to_string())),
                                }),
                            },
                            KeyValue {
                                key: "zone".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::IntValue(1)),
                                }),
                            },
                        ],
                    })),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Map(vec) => {
                assert_eq!(vec.len(), 3);
                assert!(vec.contains(&("simple".to_string(), "value".to_string())));
                assert!(vec.contains(&("metadata.region".to_string(), "us-east-1".to_string())));
                assert!(vec.contains(&("metadata.zone".to_string(), "1".to_string())));
            }
            _ => panic!("Expected Map variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_map_variant_nested_kvlist() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, false);

        let attrs = vec![KeyValue {
            key: "outer".to_string(),
            value: Some(AnyValue {
                value: Some(Value::KvlistValue(KeyValueList {
                    values: vec![KeyValue {
                        key: "middle".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![KeyValue {
                                    key: "inner".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(Value::StringValue("deep".to_string())),
                                    }),
                                }],
                            })),
                        }),
                    }],
                })),
            }),
        }];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Map(vec) => {
                assert_eq!(vec.len(), 1);
                assert!(vec.contains(&("outer.middle.inner".to_string(), "deep".to_string())));
            }
            _ => panic!("Expected Map variant"),
        }
    }

    /// Test 1: Nested mode preserves KvlistValue as a JSON Object
    ///
    /// When nested_kv_max_depth > 0, a KvlistValue attribute sitting alongside
    /// scalar siblings at the same level should be preserved as a JSON Object —
    /// NOT flattened into dotted paths.
    ///
    /// Before the fix, flatten_keyvalues_* would unconditionally recurse into
    /// KvlistValue entries regardless of nested_kv_max_depth, producing dotted
    /// keys (e.g. "user_message.role", "user_message.content"). These dotted
    /// keys conflicted with the depth-tracking logic, causing the nested
    /// attribute to be silently lost in ClickHouse output.
    ///
    /// After the fix, KvlistValue entries are only flattened in flat mode
    /// (nested_kv_max_depth == 0). In nested mode, they are passed through to
    /// anyvalue_to_jsontype which correctly converts them to JSON Objects.
    #[test]
    fn test_nested_mode_preserves_kvlist_alongside_scalar_siblings() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // nested_kv_max_depth > 0: nested mode enabled
        let transformer = Transformer::new(Compression::None, true).with_nested_kv_max_depth(3);

        // A KvlistValue ("user_message") alongside a scalar ("is_active")
        let attrs = vec![
            KeyValue {
                key: "is_active".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::BoolValue(true)),
                }),
            },
            KeyValue {
                key: "user_message".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::KvlistValue(KeyValueList {
                        values: vec![
                            KeyValue {
                                key: "role".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("user".to_string())),
                                }),
                            },
                            KeyValue {
                                key: "content".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("Hello".to_string())),
                                }),
                            },
                        ],
                    })),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                // Must have exactly 2 top-level keys — NOT 3 dotted keys
                assert_eq!(
                    map.len(),
                    2,
                    "Expected 2 keys (scalar + object), got {}: {:?}",
                    map.len(),
                    map.keys().collect::<Vec<_>>()
                );

                // Scalar sibling preserved
                assert!(map.contains_key("is_active"));

                // KvlistValue kept as a single key with an Object value
                assert!(
                    map.contains_key("user_message"),
                    "KvlistValue attribute was dropped — this is the bug"
                );

                // Bug symptom: dotted paths should NOT exist
                assert!(
                    !map.contains_key("user_message.role"),
                    "KvlistValue was incorrectly flattened into dotted paths"
                );
                assert!(!map.contains_key("user_message.content"));

                // Verify the Object contains the correct inner entries
                match &map[&Cow::Borrowed("user_message")] {
                    JsonType::Object(entries) => {
                        assert_eq!(entries.len(), 2);
                        let role = entries
                            .iter()
                            .find(|(k, _)| k == "role")
                            .expect("missing 'role' in nested object");
                        match &role.1 {
                            JsonType::Str(s) => assert_eq!(s, "user"),
                            other => panic!("Expected Str for role, got {:?}", other),
                        }
                        let content = entries
                            .iter()
                            .find(|(k, _)| k == "content")
                            .expect("missing 'content' in nested object");
                        match &content.1 {
                            JsonType::Str(s) => assert_eq!(s, "Hello"),
                            other => panic!("Expected Str for content, got {:?}", other),
                        }
                    }
                    other => panic!("Expected user_message to be Object, got {:?}", other),
                }
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    /// Test 2: Same fix for the owned variant (flatten_keyvalues_owned)
    ///
    /// Verifies the same bug fix in transform_attrs_kv_owned, which uses
    /// flatten_keyvalues_owned internally. The owned path is used when
    /// attributes need to outlive the source protobuf data.
    #[test]
    fn test_nested_mode_owned_preserves_kvlist_alongside_scalar_siblings() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true).with_nested_kv_max_depth(3);

        let attrs = vec![
            KeyValue {
                key: "is_active".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::BoolValue(false)),
                }),
            },
            KeyValue {
                key: "response_message".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::KvlistValue(KeyValueList {
                        values: vec![KeyValue {
                            key: "role".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("assistant".to_string())),
                            }),
                        }],
                    })),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv_owned(&attrs);

        match result {
            MapOrJson::JsonOwned(map) => {
                assert_eq!(
                    map.len(),
                    2,
                    "Expected 2 keys, got {}: {:?}",
                    map.len(),
                    map.keys().collect::<Vec<_>>()
                );
                assert!(map.contains_key("is_active"));
                assert!(
                    map.contains_key("response_message"),
                    "KvlistValue attribute was dropped"
                );
                assert!(
                    !map.contains_key("response_message.role"),
                    "KvlistValue was incorrectly flattened into dotted paths"
                );

                match &map["response_message"] {
                    JsonType::Object(entries) => {
                        assert_eq!(entries.len(), 1);
                        let role = entries
                            .iter()
                            .find(|(k, _)| k == "role")
                            .expect("missing 'role' in nested object");
                        match &role.1 {
                            JsonType::Str(s) => assert_eq!(s, "assistant"),
                            other => panic!("Expected Str for role, got {:?}", other),
                        }
                    }
                    other => panic!("Expected response_message to be Object, got {:?}", other),
                }
            }
            _ => panic!("Expected JsonOwned variant"),
        }
    }

    /// Test 3: Flat mode backwards compatibility
    ///
    /// Verifies that flat mode (nested_kv_max_depth == 0, the default) still
    /// flattens KvlistValue into dotted paths as before. This ensures the fix
    /// doesn't break existing behavior for users not using nested mode.
    #[test]
    fn test_flat_mode_still_flattens_kvlist_into_dotted_paths() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // nested_kv_max_depth == 0 (default): flat mode
        let transformer = Transformer::new(Compression::None, true);

        let attrs = vec![
            KeyValue {
                key: "is_active".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::BoolValue(true)),
                }),
            },
            KeyValue {
                key: "user_message".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::KvlistValue(KeyValueList {
                        values: vec![
                            KeyValue {
                                key: "role".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("user".to_string())),
                                }),
                            },
                            KeyValue {
                                key: "content".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("Hello".to_string())),
                                }),
                            },
                        ],
                    })),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                // Flat mode: KvlistValue IS expanded into dotted paths
                assert_eq!(
                    map.len(),
                    3,
                    "Flat mode should produce 3 dotted keys, got {}: {:?}",
                    map.len(),
                    map.keys().collect::<Vec<_>>()
                );
                assert!(map.contains_key("is_active"));
                assert!(map.contains_key("user_message.role"));
                assert!(map.contains_key("user_message.content"));
                // The un-flattened key should NOT exist in flat mode
                assert!(!map.contains_key("user_message"));
            }
            _ => panic!("Expected JSON variant"),
        }
    }
}
