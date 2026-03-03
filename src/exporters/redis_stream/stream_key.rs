// SPDX-License-Identifier: Apache-2.0

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;

#[derive(Clone, Debug, PartialEq)]
pub enum TemplatePart {
    Literal(String),
    Placeholder(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamKeyTemplate {
    parts: Vec<TemplatePart>,
}

impl StreamKeyTemplate {
    pub fn parse(template: &str) -> Self {
        let mut parts = Vec::new();
        let mut remaining = template;

        while let Some(start) = remaining.find('{') {
            if start > 0 {
                parts.push(TemplatePart::Literal(remaining[..start].to_string()));
            }
            if let Some(end) = remaining[start..].find('}') {
                let placeholder = &remaining[start + 1..start + end];
                parts.push(TemplatePart::Placeholder(placeholder.to_string()));
                remaining = &remaining[start + end + 1..];
            } else {
                // No closing brace, treat rest as literal
                parts.push(TemplatePart::Literal(remaining[start..].to_string()));
                remaining = "";
                break;
            }
        }

        if !remaining.is_empty() {
            parts.push(TemplatePart::Literal(remaining.to_string()));
        }

        StreamKeyTemplate { parts }
    }

    pub fn is_static(&self) -> bool {
        self.parts
            .iter()
            .all(|p| matches!(p, TemplatePart::Literal(_)))
    }

    pub fn resolve(
        &self,
        resource: Option<&Resource>,
        span_attributes: &[KeyValue],
    ) -> String {
        if self.is_static() {
            return self
                .parts
                .iter()
                .map(|p| match p {
                    TemplatePart::Literal(s) => s.as_str(),
                    TemplatePart::Placeholder(_) => unreachable!(),
                })
                .collect();
        }

        let mut result = String::new();
        for part in &self.parts {
            match part {
                TemplatePart::Literal(s) => result.push_str(s),
                TemplatePart::Placeholder(key) => {
                    // Look up in span attributes first, then resource attributes
                    let value = find_attribute_value(span_attributes, key)
                        .or_else(|| {
                            resource
                                .and_then(|r| find_attribute_value(&r.attributes, key))
                        })
                        .unwrap_or_default();
                    result.push_str(&value);
                }
            }
        }
        result
    }
}

fn find_attribute_value(attributes: &[KeyValue], key: &str) -> Option<String> {
    for kv in attributes {
        if kv.key == key {
            if let Some(ref value) = kv.value {
                return Some(any_value_to_string(value));
            }
        }
    }
    None
}

fn any_value_to_string(value: &AnyValue) -> String {
    match &value.value {
        Some(any_value::Value::StringValue(s)) => s.clone(),
        Some(any_value::Value::IntValue(i)) => i.to_string(),
        Some(any_value::Value::DoubleValue(d)) => d.to_string(),
        Some(any_value::Value::BoolValue(b)) => b.to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};

    fn make_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    fn make_resource(attrs: Vec<KeyValue>) -> Resource {
        Resource {
            attributes: attrs,
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }
    }

    #[test]
    fn test_parse_static_key() {
        let template = StreamKeyTemplate::parse("rotel:traces");
        assert!(template.is_static());
        assert_eq!(
            template.parts,
            vec![TemplatePart::Literal("rotel:traces".to_string())]
        );
    }

    #[test]
    fn test_parse_with_placeholders() {
        let template =
            StreamKeyTemplate::parse("traces:{service.name}:{deployment.environment}");
        assert!(!template.is_static());
        assert_eq!(
            template.parts,
            vec![
                TemplatePart::Literal("traces:".to_string()),
                TemplatePart::Placeholder("service.name".to_string()),
                TemplatePart::Literal(":".to_string()),
                TemplatePart::Placeholder("deployment.environment".to_string()),
            ]
        );
    }

    #[test]
    fn test_resolve_static() {
        let template = StreamKeyTemplate::parse("rotel:traces");
        let result = template.resolve(None, &[]);
        assert_eq!(result, "rotel:traces");
    }

    #[test]
    fn test_resolve_with_span_attributes() {
        let template =
            StreamKeyTemplate::parse("traces:{service.name}:{deployment.environment}");
        let attrs = vec![
            make_kv("service.name", "my-service"),
            make_kv("deployment.environment", "production"),
        ];
        let result = template.resolve(None, &attrs);
        assert_eq!(result, "traces:my-service:production");
    }

    #[test]
    fn test_resolve_with_resource_attributes() {
        let template = StreamKeyTemplate::parse("traces:{service.name}");
        let resource = make_resource(vec![make_kv("service.name", "my-service")]);
        let result = template.resolve(Some(&resource), &[]);
        assert_eq!(result, "traces:my-service");
    }

    #[test]
    fn test_resolve_span_attributes_take_priority() {
        let template = StreamKeyTemplate::parse("traces:{service.name}");
        let resource = make_resource(vec![make_kv("service.name", "resource-service")]);
        let span_attrs = vec![make_kv("service.name", "span-service")];
        let result = template.resolve(Some(&resource), &span_attrs);
        assert_eq!(result, "traces:span-service");
    }

    #[test]
    fn test_resolve_missing_attribute() {
        let template = StreamKeyTemplate::parse("traces:{missing.attr}");
        let result = template.resolve(None, &[]);
        assert_eq!(result, "traces:");
    }

    #[test]
    fn test_parse_unclosed_brace() {
        let template = StreamKeyTemplate::parse("traces:{unclosed");
        assert!(template.is_static());
        // Parser splits on '{' then treats remainder as literal
        assert_eq!(
            template.parts,
            vec![
                TemplatePart::Literal("traces:".to_string()),
                TemplatePart::Literal("{unclosed".to_string()),
            ]
        );
    }
}
