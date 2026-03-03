// SPDX-License-Identifier: Apache-2.0

use crate::exporters::shared::otel_span::{
    attributes_to_json_map, find_str_attribute, flatten_keyvalues, span_kind_to_string,
    status_code, status_message,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;

use super::config::SerializationFormat;

pub(crate) fn transform_span(
    format: &SerializationFormat,
    span: &Span,
    resource: Option<&Resource>,
    scope_name: &str,
    scope_version: &str,
) -> Vec<(String, String)> {
    match format {
        SerializationFormat::Flat => transform_span_flat(span, resource, scope_name, scope_version),
        SerializationFormat::Json => transform_span_json(span, resource, scope_name, scope_version),
    }
}

fn transform_span_flat(
    span: &Span,
    resource: Option<&Resource>,
    scope_name: &str,
    scope_version: &str,
) -> Vec<(String, String)> {
    let mut fields = Vec::with_capacity(16);

    fields.push(("trace_id".to_string(), hex::encode(&span.trace_id)));
    fields.push(("span_id".to_string(), hex::encode(&span.span_id)));
    fields.push((
        "parent_span_id".to_string(),
        hex::encode(&span.parent_span_id),
    ));

    let service_name = resource
        .map(|r| find_str_attribute(SERVICE_NAME, &r.attributes).into_owned())
        .unwrap_or_default();
    fields.push(("service_name".to_string(), service_name));

    fields.push(("span_name".to_string(), span.name.clone()));
    fields.push((
        "span_kind".to_string(),
        span_kind_to_string(span.kind).to_string(),
    ));
    fields.push((
        "start_time_unix_nano".to_string(),
        span.start_time_unix_nano.to_string(),
    ));
    fields.push((
        "end_time_unix_nano".to_string(),
        span.end_time_unix_nano.to_string(),
    ));

    let duration = if span.end_time_unix_nano > span.start_time_unix_nano {
        span.end_time_unix_nano - span.start_time_unix_nano
    } else {
        0
    };
    fields.push(("duration_ns".to_string(), duration.to_string()));

    fields.push((
        "status_code".to_string(),
        status_code(span).to_string(),
    ));
    fields.push((
        "status_message".to_string(),
        status_message(span).to_string(),
    ));

    fields.push(("scope_name".to_string(), scope_name.to_string()));
    fields.push(("scope_version".to_string(), scope_version.to_string()));

    // Resource attributes with "resource." prefix
    if let Some(resource) = resource {
        flatten_keyvalues(&resource.attributes, "resource.", &mut fields);
    }

    // Span attributes with "attr." prefix
    flatten_keyvalues(&span.attributes, "attr.", &mut fields);

    fields
}

fn transform_span_json(
    span: &Span,
    resource: Option<&Resource>,
    scope_name: &str,
    scope_version: &str,
) -> Vec<(String, String)> {
    let service_name = resource
        .map(|r| find_str_attribute(SERVICE_NAME, &r.attributes).into_owned())
        .unwrap_or_default();

    let duration = if span.end_time_unix_nano > span.start_time_unix_nano {
        span.end_time_unix_nano - span.start_time_unix_nano
    } else {
        0
    };

    let mut data = serde_json::Map::new();
    data.insert(
        "trace_id".to_string(),
        serde_json::Value::String(hex::encode(&span.trace_id)),
    );
    data.insert(
        "span_id".to_string(),
        serde_json::Value::String(hex::encode(&span.span_id)),
    );
    data.insert(
        "parent_span_id".to_string(),
        serde_json::Value::String(hex::encode(&span.parent_span_id)),
    );
    data.insert(
        "service_name".to_string(),
        serde_json::Value::String(service_name),
    );
    data.insert(
        "span_name".to_string(),
        serde_json::Value::String(span.name.clone()),
    );
    data.insert(
        "span_kind".to_string(),
        serde_json::Value::String(span_kind_to_string(span.kind).to_string()),
    );
    data.insert(
        "start_time_unix_nano".to_string(),
        serde_json::Value::Number(span.start_time_unix_nano.into()),
    );
    data.insert(
        "end_time_unix_nano".to_string(),
        serde_json::Value::Number(span.end_time_unix_nano.into()),
    );
    data.insert(
        "duration_ns".to_string(),
        serde_json::Value::Number(duration.into()),
    );
    data.insert(
        "status_code".to_string(),
        serde_json::Value::String(status_code(span).to_string()),
    );
    data.insert(
        "status_message".to_string(),
        serde_json::Value::String(status_message(span).to_string()),
    );
    data.insert(
        "scope_name".to_string(),
        serde_json::Value::String(scope_name.to_string()),
    );
    data.insert(
        "scope_version".to_string(),
        serde_json::Value::String(scope_version.to_string()),
    );

    // Resource attributes as nested object
    if let Some(resource) = resource {
        let resource_attrs = attributes_to_json_map(&resource.attributes);
        data.insert(
            "resource_attributes".to_string(),
            serde_json::Value::Object(resource_attrs),
        );
    }

    // Span attributes as nested object
    let span_attrs = attributes_to_json_map(&span.attributes);
    data.insert(
        "span_attributes".to_string(),
        serde_json::Value::Object(span_attrs),
    );

    let json_str = match serde_json::to_string(&serde_json::Value::Object(data)) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, span_name = span.name, "Failed to serialize span to JSON, using empty object");
            "{}".to_string()
        }
    };
    vec![("data".to_string(), json_str)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::trace::v1::{Span, Status};

    fn make_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    fn make_test_span() -> Span {
        Span {
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
            parent_span_id: vec![],
            name: "test-span".to_string(),
            kind: opentelemetry_proto::tonic::trace::v1::span::SpanKind::Server as i32,
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes: vec![make_kv("http.method", "GET")],
            status: Some(Status {
                message: "".to_string(),
                code: 1,
            }),
            trace_state: String::new(),
            flags: 0,
            events: vec![],
            links: vec![],
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
        }
    }

    fn make_test_resource() -> Resource {
        Resource {
            attributes: vec![make_kv("service.name", "my-service")],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }
    }

    #[test]
    fn test_flat_transform() {
        let span = make_test_span();
        let resource = make_test_resource();
        let fields =
            transform_span(&SerializationFormat::Flat, &span, Some(&resource), "my-scope", "1.0");

        let field_map: std::collections::HashMap<&str, &str> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        assert_eq!(
            field_map.get("trace_id"),
            Some(&"000102030405060708090a0b0c0d0e0f")
        );
        assert_eq!(field_map.get("span_id"), Some(&"0001020304050607"));
        assert_eq!(field_map.get("service_name"), Some(&"my-service"));
        assert_eq!(field_map.get("span_name"), Some(&"test-span"));
        assert_eq!(field_map.get("span_kind"), Some(&"Server"));
        assert_eq!(field_map.get("duration_ns"), Some(&"1000000000"));
        assert_eq!(field_map.get("status_code"), Some(&"Ok"));
        assert_eq!(field_map.get("scope_name"), Some(&"my-scope"));
        assert_eq!(field_map.get("scope_version"), Some(&"1.0"));
        assert_eq!(
            field_map.get("resource.service.name"),
            Some(&"my-service")
        );
        assert_eq!(field_map.get("attr.http.method"), Some(&"GET"));
    }

    #[test]
    fn test_json_transform() {
        let span = make_test_span();
        let resource = make_test_resource();
        let fields =
            transform_span(&SerializationFormat::Json, &span, Some(&resource), "my-scope", "1.0");

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "data");

        let parsed: serde_json::Value = serde_json::from_str(&fields[0].1).unwrap();
        assert_eq!(
            parsed["trace_id"],
            "000102030405060708090a0b0c0d0e0f"
        );
        assert_eq!(parsed["service_name"], "my-service");
        assert_eq!(parsed["span_name"], "test-span");
        assert_eq!(parsed["span_kind"], "Server");
        assert_eq!(parsed["resource_attributes"]["service.name"], "my-service");
        assert_eq!(parsed["span_attributes"]["http.method"], "GET");
    }
}
