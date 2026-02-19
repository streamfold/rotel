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
    nested_kv_max_depth: Option<usize>,
}

impl Transformer {
    pub fn new(compression: Compression, use_json: bool) -> Self {
        Self {
            compression,
            use_json,
            nested_kv_max_depth: None, // Default: backwards compatible flat mode
        }
    }

    pub fn with_nested_kv_max_depth(mut self, max_depth: Option<usize>) -> Self {
        self.nested_kv_max_depth = max_depth;
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
                    Some(Value::KvlistValue(kvlist)) => {
                        let full_key = if prefix.is_empty() {
                            kv.key.clone()
                        } else {
                            format!("{}.{}", prefix, kv.key)
                        };
                        self.flatten_keyvalues_borrowed(&kvlist.values, full_key, result);
                    }
                    Some(_) => {
                        // Optimize for common case: avoid clone when prefix is empty
                        let key = if prefix.is_empty() {
                            Cow::Borrowed(kv.key.as_str())
                        } else {
                            Cow::Owned(format!("{}.{}", prefix, kv.key))
                        };
                        result.insert(
                            key,
                            anyvalue_to_jsontype(any_value, self.nested_kv_max_depth),
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
                    Some(Value::KvlistValue(kvlist)) => {
                        // Recursively flatten nested key-value lists
                        self.flatten_keyvalues_owned(&kvlist.values, full_key, result);
                    }
                    Some(_) => {
                        result.insert(
                            full_key,
                            anyvalue_to_jsontype_owned(any_value.clone(), self.nested_kv_max_depth),
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
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;

    #[test]
    fn test_transform_attrs_lifetime_correctness() {

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

    // =========================================================================
    // GenAI-style nested attribute tests
    // =========================================================================
    //
    // These tests exercise the nesting pattern used by OpenTelemetry GenAI
    // semantic conventions, where attributes like gen_ai.input.messages use:
    //   ArrayValue [ KvlistValue { role, parts: ArrayValue [ KvlistValue { type, content } ] } ]

    /// Helper: build a GenAI message KvlistValue with role and text parts.
    fn genai_message(role: &str, parts: Vec<(&str, &str)>) -> AnyValue {
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let part_values: Vec<AnyValue> = parts
            .into_iter()
            .map(|(part_type, content)| {
                AnyValue {
                    value: Some(Value::KvlistValue(KeyValueList {
                        values: vec![
                            KeyValue {
                                key: "type".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue(part_type.to_string())),
                                }),
                            },
                            KeyValue {
                                key: "content".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue(content.to_string())),
                                }),
                            },
                        ],
                    })),
                }
            })
            .collect();

        AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "role".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(role.to_string())),
                        }),
                    },
                    KeyValue {
                        key: "parts".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::ArrayValue(
                                opentelemetry_proto::tonic::common::v1::ArrayValue {
                                    values: part_values,
                                },
                            )),
                        }),
                    },
                ],
            })),
        }
    }

    /// Helper: build a gen_ai.input.messages attribute as ArrayValue of message KvlistValues.
    fn genai_messages_attr(key: &str, messages: Vec<AnyValue>) -> KeyValue {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::ArrayValue(
                    opentelemetry_proto::tonic::common::v1::ArrayValue {
                        values: messages,
                    },
                )),
            }),
        }
    }

    #[test]
    fn test_genai_input_messages_flat_mode() {
        // In flat mode (default), the top-level ArrayValue stays as an Array,
        // but nested KvlistValues inside the array become JSON strings.
        let transformer = Transformer::new(Compression::None, true);

        let attrs = vec![
            genai_messages_attr(
                "gen_ai.input.messages",
                vec![
                    genai_message("user", vec![("text", "What is the weather?")]),
                    genai_message("assistant", vec![("text", "It's sunny today.")]),
                ],
            ),
            KeyValue {
                key: "gen_ai.system".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("openai".to_string())),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                // gen_ai.system is a simple string
                assert!(map.contains_key("gen_ai.system"));

                // gen_ai.input.messages should be present as an array
                assert!(
                    map.contains_key("gen_ai.input.messages"),
                    "Expected gen_ai.input.messages key"
                );

                match map.get("gen_ai.input.messages").unwrap() {
                    JsonType::Array(values) => {
                        assert_eq!(values.len(), 2, "Expected 2 messages");

                        // In flat mode, KvlistValue elements become JSON strings
                        for val in values {
                            match val {
                                JsonType::Str(s) => {
                                    // Should contain role and parts info serialized as JSON
                                    let json_str = s.as_ref();
                                    assert!(
                                        json_str.contains("role") || json_str.contains("type"),
                                        "Expected JSON with message fields, got: {}",
                                        json_str
                                    );
                                }
                                _ => panic!(
                                    "Expected Str (JSON string) in flat mode for KvlistValue, got {:?}",
                                    val
                                ),
                            }
                        }
                    }
                    other => panic!(
                        "Expected Array for gen_ai.input.messages, got {:?}",
                        other
                    ),
                }
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_genai_input_messages_nested_mode() {
        // In nested mode, ArrayValue stays Array and KvlistValues become Objects.
        let transformer = Transformer::new(Compression::None, true)
            .with_nested_kv_max_depth(Some(10));

        let attrs = vec![genai_messages_attr(
            "gen_ai.input.messages",
            vec![
                genai_message("user", vec![("text", "What is the weather?")]),
                genai_message("assistant", vec![("text", "It's sunny today.")]),
            ],
        )];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                assert!(map.contains_key("gen_ai.input.messages"));

                match map.get("gen_ai.input.messages").unwrap() {
                    JsonType::Array(messages) => {
                        assert_eq!(messages.len(), 2, "Expected 2 messages");

                        // First message: user
                        match &messages[0] {
                            JsonType::Object(entries) => {
                                assert_eq!(entries.len(), 2, "Expected role + parts");

                                // Check role
                                let role_entry = entries
                                    .iter()
                                    .find(|(k, _)| k.as_ref() == "role")
                                    .expect("Expected 'role' key");
                                match &role_entry.1 {
                                    JsonType::Str(s) => assert_eq!(s.as_ref(), "user"),
                                    _ => panic!("Expected role to be Str"),
                                }

                                // Check parts is an array
                                let parts_entry = entries
                                    .iter()
                                    .find(|(k, _)| k.as_ref() == "parts")
                                    .expect("Expected 'parts' key");
                                match &parts_entry.1 {
                                    JsonType::Array(parts) => {
                                        assert_eq!(parts.len(), 1, "Expected 1 part");
                                        match &parts[0] {
                                            JsonType::Object(part_entries) => {
                                                let type_entry = part_entries
                                                    .iter()
                                                    .find(|(k, _)| k.as_ref() == "type")
                                                    .expect("Expected 'type' key");
                                                match &type_entry.1 {
                                                    JsonType::Str(s) => {
                                                        assert_eq!(s.as_ref(), "text")
                                                    }
                                                    _ => panic!("Expected type to be Str"),
                                                }

                                                let content_entry = part_entries
                                                    .iter()
                                                    .find(|(k, _)| k.as_ref() == "content")
                                                    .expect("Expected 'content' key");
                                                match &content_entry.1 {
                                                    JsonType::Str(s) => assert_eq!(
                                                        s.as_ref(),
                                                        "What is the weather?"
                                                    ),
                                                    _ => panic!("Expected content to be Str"),
                                                }
                                            }
                                            _ => panic!("Expected Object for part"),
                                        }
                                    }
                                    _ => panic!("Expected Array for parts"),
                                }
                            }
                            _ => panic!("Expected Object for message, got {:?}", &messages[0]),
                        }

                        // Second message: assistant
                        match &messages[1] {
                            JsonType::Object(entries) => {
                                let role_entry = entries
                                    .iter()
                                    .find(|(k, _)| k.as_ref() == "role")
                                    .expect("Expected 'role' key");
                                match &role_entry.1 {
                                    JsonType::Str(s) => assert_eq!(s.as_ref(), "assistant"),
                                    _ => panic!("Expected role to be Str"),
                                }
                            }
                            _ => panic!("Expected Object for message"),
                        }
                    }
                    other => panic!(
                        "Expected Array for gen_ai.input.messages, got {:?}",
                        other
                    ),
                }
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_genai_input_messages_nested_mode_owned() {
        // Same as nested mode test but using the owned path.
        let transformer = Transformer::new(Compression::None, true)
            .with_nested_kv_max_depth(Some(10));

        let attrs = vec![genai_messages_attr(
            "gen_ai.input.messages",
            vec![genai_message("user", vec![("text", "Hello!")])],
        )];

        let result = transformer.transform_attrs_kv_owned(&attrs);

        match result {
            MapOrJson::JsonOwned(map) => {
                assert!(map.contains_key("gen_ai.input.messages"));

                match map.get("gen_ai.input.messages").unwrap() {
                    JsonType::Array(messages) => {
                        assert_eq!(messages.len(), 1);

                        match &messages[0] {
                            JsonType::Object(entries) => {
                                let role_entry = entries
                                    .iter()
                                    .find(|(k, _)| k.as_ref() == "role")
                                    .expect("Expected 'role' key");
                                match &role_entry.1 {
                                    JsonType::Str(s) => assert_eq!(s.as_ref(), "user"),
                                    _ => panic!("Expected role to be Str"),
                                }

                                let parts_entry = entries
                                    .iter()
                                    .find(|(k, _)| k.as_ref() == "parts")
                                    .expect("Expected 'parts' key");
                                match &parts_entry.1 {
                                    JsonType::Array(parts) => {
                                        assert_eq!(parts.len(), 1);
                                        match &parts[0] {
                                            JsonType::Object(part_entries) => {
                                                let content_entry = part_entries
                                                    .iter()
                                                    .find(|(k, _)| k.as_ref() == "content")
                                                    .expect("Expected 'content' key");
                                                match &content_entry.1 {
                                                    JsonType::Str(s) => {
                                                        assert_eq!(s.as_ref(), "Hello!")
                                                    }
                                                    _ => panic!("Expected content to be Str"),
                                                }
                                            }
                                            _ => panic!("Expected Object for part"),
                                        }
                                    }
                                    _ => panic!("Expected Array for parts"),
                                }
                            }
                            _ => panic!("Expected Object for message"),
                        }
                    }
                    other => panic!("Expected Array, got {:?}", other),
                }
            }
            _ => panic!("Expected JsonOwned variant"),
        }
    }

    #[test]
    fn test_genai_tool_call_parts_nested_mode() {
        // Test GenAI tool_call message part structure:
        // KvlistValue { type: "tool_call", id: "call_123", name: "get_weather", arguments: "{}" }
        use opentelemetry_proto::tonic::common::v1::KeyValueList;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let transformer = Transformer::new(Compression::None, true)
            .with_nested_kv_max_depth(Some(10));

        let tool_call_part = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "type".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("tool_call".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "id".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("call_abc123".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("get_weather".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "arguments".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(
                                r#"{"location":"San Francisco"}"#.to_string(),
                            )),
                        }),
                    },
                ],
            })),
        };

        // Build: assistant message with tool_call part
        let assistant_msg = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "role".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("assistant".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "parts".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::ArrayValue(
                                opentelemetry_proto::tonic::common::v1::ArrayValue {
                                    values: vec![tool_call_part],
                                },
                            )),
                        }),
                    },
                ],
            })),
        };

        let attrs = vec![genai_messages_attr(
            "gen_ai.output.messages",
            vec![assistant_msg],
        )];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                assert!(map.contains_key("gen_ai.output.messages"));

                match map.get("gen_ai.output.messages").unwrap() {
                    JsonType::Array(messages) => {
                        assert_eq!(messages.len(), 1);

                        match &messages[0] {
                            JsonType::Object(entries) => {
                                // Check parts contains tool_call
                                let parts_entry = entries
                                    .iter()
                                    .find(|(k, _)| k.as_ref() == "parts")
                                    .expect("Expected 'parts' key");
                                match &parts_entry.1 {
                                    JsonType::Array(parts) => {
                                        assert_eq!(parts.len(), 1);
                                        match &parts[0] {
                                            JsonType::Object(part_entries) => {
                                                // Verify all tool_call fields
                                                let fields: Vec<&str> = part_entries
                                                    .iter()
                                                    .map(|(k, _)| k.as_ref())
                                                    .collect();
                                                assert!(fields.contains(&"type"));
                                                assert!(fields.contains(&"id"));
                                                assert!(fields.contains(&"name"));
                                                assert!(fields.contains(&"arguments"));

                                                let name = part_entries
                                                    .iter()
                                                    .find(|(k, _)| k.as_ref() == "name")
                                                    .unwrap();
                                                match &name.1 {
                                                    JsonType::Str(s) => {
                                                        assert_eq!(s.as_ref(), "get_weather")
                                                    }
                                                    _ => panic!("Expected Str for name"),
                                                }
                                            }
                                            _ => panic!("Expected Object for tool_call part"),
                                        }
                                    }
                                    _ => panic!("Expected Array for parts"),
                                }
                            }
                            _ => panic!("Expected Object for message"),
                        }
                    }
                    other => panic!("Expected Array, got {:?}", other),
                }
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_genai_depth_limit_truncation() {
        // Test that depth limit correctly truncates deeply nested structures.
        // With max_depth=1, only 1 level of nesting is preserved.
        let transformer = Transformer::new(Compression::None, true)
            .with_nested_kv_max_depth(Some(1));

        let attrs = vec![genai_messages_attr(
            "gen_ai.input.messages",
            vec![genai_message("user", vec![("text", "Hello")])],
        )];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                assert!(map.contains_key("gen_ai.input.messages"));

                // At depth 0: ArrayValue → Array (each element at depth 1)
                // At depth 1: KvlistValue → Object (each value at depth 2)
                // At depth 2 > max_depth(1): values become JSON strings
                match map.get("gen_ai.input.messages").unwrap() {
                    JsonType::Array(messages) => {
                        assert_eq!(messages.len(), 1);

                        match &messages[0] {
                            JsonType::Object(entries) => {
                                // role at depth 2 → JSON string fallback
                                let role_entry = entries
                                    .iter()
                                    .find(|(k, _)| k.as_ref() == "role");
                                assert!(role_entry.is_some(), "Expected 'role' key");

                                // parts at depth 2 → should be a JSON string (depth exceeded)
                                let parts_entry = entries
                                    .iter()
                                    .find(|(k, _)| k.as_ref() == "parts");
                                assert!(parts_entry.is_some(), "Expected 'parts' key");

                                // The parts value at depth 2 should be a JSON string fallback
                                match &parts_entry.unwrap().1 {
                                    JsonType::Str(s) => {
                                        // Depth exceeded, so the ArrayValue is serialized as JSON
                                        assert!(
                                            s.as_ref().contains("text")
                                                || s.as_ref().contains("Hello"),
                                            "Expected JSON fallback containing message data, got: {}",
                                            s.as_ref()
                                        );
                                    }
                                    _ => {
                                        // At depth 2 with max_depth 1, it falls back to JSON string
                                        // but role (StringValue) would still resolve as Str
                                    }
                                }
                            }
                            JsonType::Str(s) => {
                                // If depth 1 is exceeded for the message itself
                                assert!(
                                    s.as_ref().contains("role") || s.as_ref().contains("user"),
                                    "Expected JSON string for truncated message"
                                );
                            }
                            _ => panic!(
                                "Expected Object or Str for depth-limited message, got {:?}",
                                &messages[0]
                            ),
                        }
                    }
                    other => panic!("Expected Array, got {:?}", other),
                }
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_genai_mixed_attributes() {
        // Test a realistic span with both simple and GenAI nested attributes.
        let transformer = Transformer::new(Compression::None, true)
            .with_nested_kv_max_depth(Some(10));

        let attrs = vec![
            KeyValue {
                key: "gen_ai.system".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("openai".to_string())),
                }),
            },
            KeyValue {
                key: "gen_ai.request.model".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("gpt-4".to_string())),
                }),
            },
            KeyValue {
                key: "gen_ai.request.max_tokens".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(4096)),
                }),
            },
            KeyValue {
                key: "gen_ai.request.temperature".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::DoubleValue(0.7)),
                }),
            },
            genai_messages_attr(
                "gen_ai.input.messages",
                vec![genai_message("user", vec![("text", "Tell me a joke")])],
            ),
            genai_messages_attr(
                "gen_ai.output.messages",
                vec![genai_message(
                    "assistant",
                    vec![("text", "Why did the chicken cross the road?")],
                )],
            ),
            KeyValue {
                key: "gen_ai.usage.input_tokens".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(10)),
                }),
            },
            KeyValue {
                key: "gen_ai.usage.output_tokens".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(15)),
                }),
            },
        ];

        let result = transformer.transform_attrs_kv(&attrs);

        match result {
            MapOrJson::Json(map) => {
                // All 8 attributes should be present
                assert_eq!(map.len(), 8, "Expected 8 attributes, got: {:?}", map.keys().collect::<Vec<_>>());

                // Simple string attributes
                match map.get("gen_ai.system").unwrap() {
                    JsonType::Str(s) => assert_eq!(s.as_ref(), "openai"),
                    _ => panic!("Expected Str"),
                }
                match map.get("gen_ai.request.model").unwrap() {
                    JsonType::Str(s) => assert_eq!(s.as_ref(), "gpt-4"),
                    _ => panic!("Expected Str"),
                }

                // Integer attributes
                match map.get("gen_ai.request.max_tokens").unwrap() {
                    JsonType::Int(i) => assert_eq!(*i, 4096),
                    _ => panic!("Expected Int"),
                }
                match map.get("gen_ai.usage.input_tokens").unwrap() {
                    JsonType::Int(i) => assert_eq!(*i, 10),
                    _ => panic!("Expected Int"),
                }
                match map.get("gen_ai.usage.output_tokens").unwrap() {
                    JsonType::Int(i) => assert_eq!(*i, 15),
                    _ => panic!("Expected Int"),
                }

                // Double attribute
                match map.get("gen_ai.request.temperature").unwrap() {
                    JsonType::Double(d) => assert_eq!(*d, 0.7),
                    _ => panic!("Expected Double"),
                }

                // Nested array attributes
                match map.get("gen_ai.input.messages").unwrap() {
                    JsonType::Array(messages) => {
                        assert_eq!(messages.len(), 1);
                        match &messages[0] {
                            JsonType::Object(_) => {} // Nested Object is correct
                            _ => panic!("Expected Object for message"),
                        }
                    }
                    _ => panic!("Expected Array for input messages"),
                }
                match map.get("gen_ai.output.messages").unwrap() {
                    JsonType::Array(messages) => {
                        assert_eq!(messages.len(), 1);
                        match &messages[0] {
                            JsonType::Object(_) => {} // Nested Object is correct
                            _ => panic!("Expected Object for message"),
                        }
                    }
                    _ => panic!("Expected Array for output messages"),
                }
            }
            _ => panic!("Expected JSON variant"),
        }
    }
}
