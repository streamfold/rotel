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
                    Some(Value::KvlistValue(kvlist)) => {
                        // Recursively flatten nested key-value lists
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
        use crate::exporters::clickhouse::rowbinary::json::anyvalue_to_jsontype;
        let mut hm = HashMap::new();
        for kv in attrs {
            if let Some(any_value) = &kv.value {
                if any_value.value.is_some() {
                    hm.insert(
                        Cow::Borrowed(kv.key.as_str()),
                        anyvalue_to_jsontype(any_value, Some(self.nested_kv_max_depth)),
                    );
                }
            }
        }
        hm
    }

    pub(crate) fn transform_attrs_kv_owned(&self, attrs: &[KeyValue]) -> MapOrJson<'static> {
        match self.use_json {
            true => MapOrJson::JsonOwned(self.build_json_attrs_kv_owned(attrs)),
            false => MapOrJson::Map(self.build_map_attrs_kv(attrs)),
        }
    }

    fn build_json_attrs_kv_owned(&self, attrs: &[KeyValue]) -> HashMap<String, JsonType<'static>> {
        use crate::exporters::clickhouse::rowbinary::json::anyvalue_to_jsontype_owned;
        let mut hm = HashMap::new();
        for kv in attrs {
            if let Some(any_value) = &kv.value {
                if any_value.value.is_some() {
                    hm.insert(
                        kv.key.clone(),
                        anyvalue_to_jsontype_owned(
                            any_value.clone(),
                            Some(self.nested_kv_max_depth),
                        ),
                    );
                }
            }
        }
        hm
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
                assert_eq!(map.len(), 2);
                assert!(map.contains_key("simple"));
                // metadata is preserved as a nested Object, not flattened
                match map.get("metadata") {
                    Some(JsonType::Object(entries)) => {
                        assert_eq!(entries.len(), 2);
                        assert!(entries.iter().any(|(k, _)| k.as_ref() == "region"));
                        assert!(entries.iter().any(|(k, _)| k.as_ref() == "zone"));
                    }
                    other => panic!("Expected metadata to be JsonType::Object, got {:?}", other),
                }
                assert!(!map.contains_key("metadata.region"));
                assert!(!map.contains_key("metadata.zone"));
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_nested_kvlist() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // Use nested_kv_max_depth=2 so all three levels are converted to Objects.
        let transformer = Transformer::new(Compression::None, true).with_nested_kv_max_depth(2);

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
                assert_eq!(map.len(), 1);
                // outer is preserved as a nested Object
                match map.get("outer") {
                    Some(JsonType::Object(outer_entries)) => {
                        assert_eq!(outer_entries.len(), 2);
                        // middle is itself a nested Object
                        let middle = outer_entries.iter().find(|(k, _)| k.as_ref() == "middle");
                        match middle {
                            Some((_, JsonType::Object(middle_entries))) => {
                                assert_eq!(middle_entries.len(), 1);
                                assert!(middle_entries.iter().any(|(k, _)| k.as_ref() == "inner"));
                            }
                            other => {
                                panic!("Expected middle to be JsonType::Object, got {:?}", other)
                            }
                        }
                        let sibling = outer_entries.iter().find(|(k, _)| k.as_ref() == "sibling");
                        match sibling {
                            Some((_, JsonType::Int(42))) => {}
                            other => {
                                panic!("Expected sibling to be JsonType::Int(42), got {:?}", other)
                            }
                        }
                    }
                    other => panic!("Expected outer to be JsonType::Object, got {:?}", other),
                }
                assert!(!map.contains_key("outer.middle.inner"));
                assert!(!map.contains_key("outer.sibling"));
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_transform_attrs_kv_owned_kvlist() {
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
                assert_eq!(map.len(), 2);
                assert!(map.contains_key("simple"));
                // metadata is preserved as a nested Object, not flattened
                match map.get("metadata") {
                    Some(JsonType::Object(entries)) => {
                        assert_eq!(entries.len(), 2);
                        assert!(entries.iter().any(|(k, _)| k.as_ref() == "region"));
                        assert!(entries.iter().any(|(k, _)| k.as_ref() == "zone"));
                    }
                    other => panic!("Expected metadata to be JsonType::Object, got {:?}", other),
                }
                assert!(!map.contains_key("metadata.region"));
                assert!(!map.contains_key("metadata.zone"));
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
}
