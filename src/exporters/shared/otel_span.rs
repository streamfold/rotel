// SPDX-License-Identifier: Apache-2.0

//! Shared OTLP span utility functions used by multiple exporters (ClickHouse, Redis Stream, etc.).

use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_proto::tonic::trace::v1::Span;
use std::borrow::Cow;

/// Convert a SpanKind integer to its string representation.
pub fn span_kind_to_string(kind: i32) -> &'static str {
    if kind == SpanKind::Internal as i32 {
        "Internal"
    } else if kind == SpanKind::Server as i32 {
        "Server"
    } else if kind == SpanKind::Client as i32 {
        "Client"
    } else if kind == SpanKind::Producer as i32 {
        "Producer"
    } else if kind == SpanKind::Consumer as i32 {
        "Consumer"
    } else {
        "Unspecified"
    }
}

/// Extract the status code string from a span.
pub fn status_code(span: &Span) -> &'static str {
    match &span.status {
        None => "Unset",
        Some(s) => match s.code {
            1 => "Ok",
            2 => "Error",
            _ => "Unset",
        },
    }
}

/// Extract the status message from a span.
pub fn status_message(span: &Span) -> &str {
    match &span.status {
        None => "",
        Some(s) => s.message.as_str(),
    }
}

/// Find a string attribute value by key in a slice of KeyValue pairs.
/// Returns a borrowed Cow to avoid cloning when possible.
pub fn find_str_attribute<'a>(attr: &str, attributes: &'a [KeyValue]) -> Cow<'a, str> {
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

/// Convert an OTLP AnyValue variant to its string representation.
pub fn anyvalue_to_string(val: &Value) -> String {
    match val {
        Value::StringValue(s) => s.clone(),
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => serde_json::json!(d).to_string(),
        Value::ArrayValue(a) => serde_json::to_string(a).unwrap_or_default(),
        Value::KvlistValue(kv) => serde_json::to_string(kv).unwrap_or_default(),
        Value::BytesValue(b) => hex::encode(b),
    }
}

/// Recursively flatten KeyValue attributes into a flat `Vec<(key, value)>` with dotted-path keys.
/// Nested KvlistValue entries are expanded with dot-separated prefixes.
pub fn flatten_keyvalues(attrs: &[KeyValue], prefix: &str, result: &mut Vec<(String, String)>) {
    for kv in attrs {
        if let Some(any_value) = &kv.value {
            let full_key = if prefix.is_empty() {
                kv.key.clone()
            } else {
                format!("{}{}", prefix, kv.key)
            };

            match &any_value.value {
                Some(Value::KvlistValue(kvlist)) => {
                    let nested_prefix = format!("{}.", full_key);
                    flatten_keyvalues(&kvlist.values, &nested_prefix, result);
                }
                Some(val) => {
                    result.push((full_key, anyvalue_to_string(val)));
                }
                None => {}
            }
        }
    }
}

/// Convert an OTLP AnyValue to a serde_json::Value.
pub fn anyvalue_to_json(value: &AnyValue) -> serde_json::Value {
    match &value.value {
        Some(Value::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(Value::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(Value::IntValue(i)) => serde_json::Value::Number((*i).into()),
        Some(Value::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(Value::ArrayValue(a)) => {
            let items: Vec<serde_json::Value> = a.values.iter().map(anyvalue_to_json).collect();
            serde_json::Value::Array(items)
        }
        Some(Value::KvlistValue(kv)) => {
            let map = attributes_to_json_map(&kv.values);
            serde_json::Value::Object(map)
        }
        Some(Value::BytesValue(b)) => serde_json::Value::String(hex::encode(b)),
        None => serde_json::Value::Null,
    }
}

/// Convert a slice of KeyValue pairs into a JSON map.
pub fn attributes_to_json_map(
    attrs: &[KeyValue],
) -> serde_json::Map<String, serde_json::Value> {
    let mut map = serde_json::Map::new();
    for kv in attrs {
        if let Some(any_value) = &kv.value {
            map.insert(kv.key.clone(), anyvalue_to_json(any_value));
        }
    }
    map
}

/// Hex-encode an ID (trace_id or span_id) into a pre-allocated buffer.
/// Returns the encoded string slice, or empty string on encoding failure
/// (e.g. empty parent_span_id on root spans).
pub fn encode_id<'a>(id: &[u8], out: &'a mut [u8]) -> &'a str {
    match hex::encode_to_slice(id, out) {
        Ok(_) => std::str::from_utf8(out).unwrap_or_default(),
        Err(_) => "",
    }
}
