// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::transform::transformer::ScopeSpan;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::trace::v1::Span as OTelSpan;
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Display;
use std::rc::Rc;

/// Store the Key and converted attribute value
/// todo: does not handle missing/blank values
#[derive(Clone, Debug)]
pub struct ConvertedAttrKeyValue(pub String, pub ConvertedAttrValue);

#[derive(Clone, Debug)]
pub enum ConvertedAttrValue {
    Int(i64),
    Double(f64),
    String(String),
}

impl Display for ConvertedAttrValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            ConvertedAttrValue::Int(i) => i.to_string(),
            // todo: match to OTEL conversion at pdata/pcommon/value.go:404
            ConvertedAttrValue::Double(d) => json!(d).to_string(),
            ConvertedAttrValue::String(s) => s.clone(),
        };
        write!(f, "{}", str)
    }
}

impl From<Value> for ConvertedAttrValue {
    fn from(value: Value) -> Self {
        match value {
            Value::StringValue(s) => ConvertedAttrValue::String(s),
            Value::BoolValue(b) => ConvertedAttrValue::String(b.to_string()),
            Value::IntValue(i) => ConvertedAttrValue::Int(i),
            Value::DoubleValue(d) => ConvertedAttrValue::Double(d),
            Value::ArrayValue(a) => ConvertedAttrValue::String(json!(a).to_string()),
            Value::KvlistValue(kv) => ConvertedAttrValue::String(json!(kv).to_string()),
            Value::BytesValue(b) => ConvertedAttrValue::String(hex::encode(b)),
        }
    }
}

impl From<String> for ConvertedAttrValue {
    fn from(value: String) -> Self {
        ConvertedAttrValue::String(value.clone())
    }
}

impl From<&str> for ConvertedAttrValue {
    fn from(value: &str) -> Self {
        ConvertedAttrValue::String(value.to_string())
    }
}

pub struct ConvertedAttrError;

impl TryInto<ConvertedAttrKeyValue> for KeyValue {
    type Error = ConvertedAttrError;

    fn try_into(self) -> Result<ConvertedAttrKeyValue, Self::Error> {
        let value = match self.value {
            None => return Err(ConvertedAttrError {}),
            Some(v) => match v.value {
                None => return Err(ConvertedAttrError {}),
                Some(v) => v,
            },
        };

        Ok(ConvertedAttrKeyValue(self.key.clone(), value.into()))
    }
}

#[derive(Default)]
pub struct ConvertedAttrMap {
    data: HashMap<String, ConvertedAttrValue>,
}

impl AsRef<HashMap<String, ConvertedAttrValue>> for ConvertedAttrMap {
    fn as_ref(&self) -> &HashMap<String, ConvertedAttrValue> {
        &self.data
    }
}

impl<K, T> From<HashMap<K, T>> for ConvertedAttrMap
where
    K: AsRef<str>,
    T: Into<ConvertedAttrValue>,
{
    fn from(value: HashMap<K, T>) -> Self {
        let mut inner = HashMap::new();
        for (k, v) in value {
            inner.insert(k.as_ref().to_string(), v.into());
        }

        Self { data: inner }
    }
}

impl From<Vec<ConvertedAttrKeyValue>> for ConvertedAttrMap {
    fn from(value: Vec<ConvertedAttrKeyValue>) -> Self {
        let data: HashMap<String, ConvertedAttrValue> = value
            .iter()
            .map(|cv| (cv.0.clone(), cv.1.clone()))
            .collect();
        Self { data }
    }
}

// Map OTEL attributes to a simplified form. Integers and Doubles are converted
// to metrics, while everything else is converted to a string or JSON converted
pub(crate) fn convert(attrs: &Vec<KeyValue>) -> Vec<ConvertedAttrKeyValue> {
    // Convert attributes to meta and metrics
    let converted: Vec<ConvertedAttrKeyValue> = attrs
        .iter()
        .filter_map(|attr| attr.clone().try_into().ok())
        .collect();

    converted
}

pub fn find_first_in_resource(
    res_attrs: &ConvertedAttrMap,
    keys: Vec<&str>,
    _normalize: bool,
) -> String {
    // todo: normalize if set

    for key in keys {
        if let Some(av) = res_attrs.as_ref().get(key) {
            return av.to_string();
        }
    }

    "".to_string()
}

// Do we want to differentiate from a true OTEL String value and the AsString() equivalents?
#[allow(dead_code)]
pub fn find_string_with_span_precedence(
    key: &str,
    scope_span: &ScopeSpan,
    res_attrs: &ConvertedAttrMap,
    normalize: bool,
) -> Option<String> {
    find_with_span_precedence(
        key,
        &scope_span.1,
        scope_span.0.clone(),
        res_attrs,
        normalize,
    )
    .and_then(|av| {
        if let ConvertedAttrValue::String(s) = av {
            Some(s)
        } else {
            None
        }
    })
}

/// Search for an attribute value by key, starting at the lowest level and then
/// moving upwards until we hit the resource attributes.
#[allow(dead_code)]
fn find_with_span_precedence(
    key: &str,
    span: &OTelSpan,
    scope: Option<Rc<InstrumentationScope>>,
    res_attrs: &ConvertedAttrMap,
    _normalize: bool,
) -> Option<ConvertedAttrValue> {
    // todo: normalize

    // search span first
    if let Some(v) = find_key_in_attrlist(key, &span.attributes) {
        return Some(v);
    }

    // then scope
    if let Some(scope) = scope {
        if let Some(v) = find_key_in_attrlist(key, &scope.attributes) {
            return Some(v);
        }
    }

    // lastly, check the resource attributes
    res_attrs.as_ref().get(key).map(|cv| cv.clone())
}

/// Search for an attribute value by key names, starting at the highest level and moving
/// downwards. The first key found in the highest precedence is the value used.
pub fn find_with_resource_precedence(
    keys: &Vec<&str>,
    span: &OTelSpan,
    scope: Option<Rc<InstrumentationScope>>,
    res_attrs: &ConvertedAttrMap,
    _normalize: bool,
) -> Option<ConvertedAttrValue> {
    // todo: normalize

    // check the resource attributes
    for key in keys {
        if let Some(v) = res_attrs.as_ref().get(*key) {
            return Some(v.clone());
        }
    }

    // scope
    if let Some(scope) = scope {
        for key in keys {
            if let Some(v) = find_key_in_attrlist(key, &scope.attributes) {
                return Some(v);
            }
        }
    }

    // finally, search span
    for key in keys {
        if let Some(v) = find_key_in_attrlist(key, &span.attributes) {
            return Some(v);
        }
    }

    None
}

pub fn find_key_in_attrlist(key: &str, attributes: &Vec<KeyValue>) -> Option<ConvertedAttrValue> {
    find_key_in_attrlist_anyvalue(key, attributes)
        .and_then(|av| av.value)
        .map(|v| v.into())
}

// Should we map all attributes to a map? What's the average list size and would that
// be beneficial?
fn find_key_in_attrlist_anyvalue(key: &str, attributes: &Vec<KeyValue>) -> Option<AnyValue> {
    attributes
        .iter()
        .find(|&attr| attr.key == *key && attr.value.is_some())
        .map(|kv| kv.value.clone().unwrap())
}
