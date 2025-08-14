// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::transform::transformer::ScopeSpan;
use crate::otlp::cvattr::{ConvertedAttrMap, ConvertedAttrValue};
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::trace::v1::Span as OTelSpan;
use std::rc::Rc;

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
    res_attrs.as_ref().get(key).cloned()
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
        .and_then(|kv| kv.value.clone())
}
