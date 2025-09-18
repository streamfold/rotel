use crate::exporters::clickhouse::Compression;
use crate::exporters::clickhouse::schema::MapOrJson;
use crate::otlp::cvattr::ConvertedAttrKeyValue;
use serde_json::json;
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
    pub(crate) fn transform_attrs(&self, attrs: &[ConvertedAttrKeyValue]) -> MapOrJson {
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

    fn build_json_attrs(&self, attrs: &[ConvertedAttrKeyValue]) -> String {
        if attrs.is_empty() {
            return "{}".to_string();
        }

        let hm: HashMap<Cow<'_, String>, String> = attrs
            .iter()
            // periods(.) in key names will be converted into a nested format, so swap
            // them to underscores to avoid nesting
            .map(|kv| {
                if self.use_json_underscore {
                    (Cow::Owned(kv.0.replace(".", "_")), kv.1.to_string())
                } else {
                    (Cow::Borrowed(&kv.0), kv.1.to_string())
                }
            })
            .collect();

        json!(hm).to_string()
    }
}

pub(crate) fn find_attribute(attr: &str, attributes: &[ConvertedAttrKeyValue]) -> String {
    attributes
        .iter()
        .find(|kv| kv.0 == attr)
        .map(|kv| kv.1.to_string())
        .unwrap_or(String::new())
}
