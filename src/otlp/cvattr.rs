use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use serde_json::json;
use std::collections::HashMap;
use std::fmt::Display;

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
        match self {
            ConvertedAttrValue::Int(i) => write!(f, "{}", i),
            // todo: match to OTEL conversion at pdata/pcommon/value.go:404
            ConvertedAttrValue::Double(d) => write!(f, "{}", json!(d).to_string()),
            ConvertedAttrValue::String(s) => write!(f, "{}", s),
        }
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
        ConvertedAttrValue::String(value)
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

        Ok(ConvertedAttrKeyValue(self.key, value.into()))
    }
}

impl TryInto<ConvertedAttrKeyValue> for &KeyValue {
    type Error = ConvertedAttrError;

    fn try_into(self) -> Result<ConvertedAttrKeyValue, Self::Error> {
        let value = match &self.value {
            None => return Err(ConvertedAttrError {}),
            Some(v) => match &v.value {
                None => return Err(ConvertedAttrError {}),
                Some(v) => v.clone(),
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
        let data: HashMap<String, ConvertedAttrValue> =
            value.into_iter().map(|cv| (cv.0, cv.1)).collect();
        Self { data }
    }
}

// Map OTEL attributes to a simplified form. Integers and Doubles are converted
// to metrics, while everything else is converted to a string or JSON converted
pub(crate) fn convert(attrs: &Vec<KeyValue>) -> Vec<ConvertedAttrKeyValue> {
    // Convert attributes to meta and metrics
    let converted: Vec<ConvertedAttrKeyValue> = attrs
        .iter()
        .filter_map(|attr| attr.try_into().ok())
        .collect();

    converted
}
