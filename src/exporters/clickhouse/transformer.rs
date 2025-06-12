use crate::exporters::clickhouse::Compression;
use crate::exporters::clickhouse::schema::MapOrJson;
use crate::otlp::cvattr::ConvertedAttrKeyValue;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use serde_json::json;
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

pub(crate) fn get_scope_properties(scope: Option<&InstrumentationScope>) -> (String, String) {
    match scope {
        None => ("".to_string(), "".to_string()),
        Some(scope) => (scope.name.clone(), scope.version.clone()),
    }
}

impl Transformer {
    pub(crate) fn transform_attrs(&self, attrs: &[ConvertedAttrKeyValue]) -> MapOrJson {
        match self.use_json {
            true => {
                let hm: HashMap<String, String> = attrs
                    .iter()
                    // periods(.) in key names will be converted into a nested format, so swap
                    // them to underscores to avoid nesting
                    .map(|kv| {
                        if self.use_json_underscore {
                            (kv.0.replace(".", "_"), kv.1.to_string())
                        } else {
                            (kv.0.clone(), kv.1.to_string())
                        }
                    })
                    .collect();

                MapOrJson::Json(json!(hm).to_string())
            }
            false => MapOrJson::Map(
                attrs
                    .iter()
                    .map(|kv| (kv.0.clone(), kv.1.to_string()))
                    .collect(),
            ),
        }
    }
}

pub(crate) fn find_attribute(attr: &str, attributes: &[ConvertedAttrKeyValue]) -> String {
    attributes
        .iter()
        .find(|kv| kv.0 == attr)
        .map(|kv| kv.1.to_string())
        .unwrap_or("".to_string())
}
