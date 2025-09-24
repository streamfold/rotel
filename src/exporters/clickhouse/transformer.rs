use crate::exporters::clickhouse::rowbinary::json::JsonType;
use crate::exporters::clickhouse::Compression;
use crate::exporters::clickhouse::schema::MapOrJson;
use crate::otlp::cvattr::ConvertedAttrKeyValue;
use std::borrow::Cow;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Transformer {
    pub(crate) compression: Compression,
    use_json: bool,
    use_json_underscore: bool,
}

impl Transformer {
    pub fn new(compression: Compression, use_json: bool, use_json_underscore: bool) -> Self {
        Self {
            compression,
            use_json,
            use_json_underscore,
        }
    }
}

impl Transformer {
    pub(crate) fn transform_attrs<'a>(&self, attrs: &'a [ConvertedAttrKeyValue]) -> MapOrJson<'a> {
        match self.use_json {
            true => MapOrJson::Json(self.build_json_attrs(attrs)),
            false => MapOrJson::Map(
                attrs
                    .iter()
                    .map(|kv| (kv.0.clone(), kv.1.to_string()))
                    .collect(),
            ),
        }
    }

    fn build_json_attrs<'a>(
        &self,
        attrs: &'a [ConvertedAttrKeyValue],
    ) -> HashMap<Cow<'a, str>, JsonType<'a>> {
        if attrs.is_empty() {
            return HashMap::new();
        }

        let hm: HashMap<Cow<'a, str>, JsonType<'a>> = attrs
            .iter()
            // periods(.) in key names will be converted into a nested format, so swap
            // them to underscores to avoid nesting
            .map(|kv| {
                let key = if self.use_json_underscore {
                    Cow::Owned(kv.0.replace(".", "_"))
                } else {
                    Cow::Borrowed(kv.0.as_str())
                };
                (key, (&kv.1).into())
            })
            .collect();

        hm
    }

    pub(crate) fn transform_attrs_owned(
        &self,
        attrs: &[ConvertedAttrKeyValue],
    ) -> MapOrJson<'static> {
        match self.use_json {
            true => MapOrJson::JsonOwned(self.build_json_attrs_owned(attrs)),
            false => MapOrJson::Map(
                attrs
                    .iter()
                    .map(|kv| (kv.0.clone(), kv.1.to_string()))
                    .collect(),
            ),
        }
    }

    fn build_json_attrs_owned(
        &self,
        attrs: &[ConvertedAttrKeyValue],
    ) -> HashMap<String, JsonType<'static>> {
        if attrs.is_empty() {
            return HashMap::new();
        }

        let hm: HashMap<String, JsonType<'static>> = attrs
            .iter()
            // periods(.) in key names will be converted into a nested format, so swap
            // them to underscores to avoid nesting
            .map(|kv| {
                let key = if self.use_json_underscore {
                    kv.0.replace(".", "_")
                } else {
                    kv.0.clone()
                };
                (key, kv.1.clone().into())
            })
            .collect();

        hm
    }
}

pub(crate) fn find_attribute(attr: &str, attributes: &[ConvertedAttrKeyValue]) -> String {
    attributes
        .iter()
        .find(|kv| kv.0 == attr)
        .map(|kv| kv.1.to_string())
        .unwrap_or(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::cvattr::ConvertedAttrValue;

    #[test]
    fn test_transform_attrs_lifetime_correctness() {
        let transformer = Transformer::new(Compression::None, true, false);

        // Create some test attributes
        let attrs = vec![
            ConvertedAttrKeyValue(
                "service.name".to_string(),
                ConvertedAttrValue::String("test-service".to_string()),
            ),
            ConvertedAttrKeyValue("http.status_code".to_string(), ConvertedAttrValue::Int(200)),
            ConvertedAttrKeyValue("duration".to_string(), ConvertedAttrValue::Double(1.23)),
        ];

        // This should work without cloning the values - the returned MapOrJson
        // should maintain references to the original data where possible
        let result = transformer.transform_attrs(&attrs);

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
    fn test_transform_attrs_with_underscore_replacement() {
        let transformer = Transformer::new(Compression::None, true, true);

        let attrs = vec![
            ConvertedAttrKeyValue(
                "service.name".to_string(),
                ConvertedAttrValue::String("test".to_string()),
            ),
            ConvertedAttrKeyValue(
                "http.method".to_string(),
                ConvertedAttrValue::String("GET".to_string()),
            ),
        ];

        let result = transformer.transform_attrs(&attrs);

        match result {
            MapOrJson::Json(map) => {
                assert!(map.contains_key("service_name"));
                assert!(map.contains_key("http_method"));
                assert!(!map.contains_key("service.name"));
                assert!(!map.contains_key("http.method"));
            }
            _ => panic!("Expected JSON variant"),
        }
    }

    #[test]
    fn test_transform_attrs_map_variant() {
        let transformer = Transformer::new(Compression::None, false, false);

        let attrs = vec![
            ConvertedAttrKeyValue(
                "key1".to_string(),
                ConvertedAttrValue::String("value1".to_string()),
            ),
            ConvertedAttrKeyValue("key2".to_string(), ConvertedAttrValue::Int(42)),
        ];

        let result = transformer.transform_attrs(&attrs);

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
        let transformer = Transformer::new(Compression::None, true, false);

        // Create some test attributes
        let attrs = vec![
            ConvertedAttrKeyValue(
                "service.name".to_string(),
                ConvertedAttrValue::String("test-service".to_string()),
            ),
            ConvertedAttrKeyValue("http.status_code".to_string(), ConvertedAttrValue::Int(200)),
            ConvertedAttrKeyValue("duration".to_string(), ConvertedAttrValue::Double(1.23)),
        ];

        // This should work with owned data - the returned MapOrJson
        // should contain owned data that doesn't reference the input
        let result = transformer.transform_attrs_owned(&attrs);

        match result {
            MapOrJson::JsonOwned(map) => {
                assert_eq!(map.len(), 3);
                assert!(map.contains_key("service.name"));
                assert!(map.contains_key("http.status_code"));
                assert!(map.contains_key("duration"));

                // Verify the values are correct and owned
                match &map["service.name"] {
                    JsonType::StrOwned(s) => assert_eq!(s, "test-service"),
                    _ => panic!("Expected StrOwned variant"),
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
    fn test_transform_attrs_owned_with_underscore_replacement() {
        let transformer = Transformer::new(Compression::None, true, true);

        let attrs = vec![
            ConvertedAttrKeyValue(
                "service.name".to_string(),
                ConvertedAttrValue::String("test".to_string()),
            ),
            ConvertedAttrKeyValue(
                "http.method".to_string(),
                ConvertedAttrValue::String("GET".to_string()),
            ),
        ];

        let result = transformer.transform_attrs_owned(&attrs);

        match result {
            MapOrJson::JsonOwned(map) => {
                assert!(map.contains_key("service_name"));
                assert!(map.contains_key("http_method"));
                assert!(!map.contains_key("service.name"));
                assert!(!map.contains_key("http.method"));
            }
            _ => panic!("Expected JsonOwned variant"),
        }
    }
}
