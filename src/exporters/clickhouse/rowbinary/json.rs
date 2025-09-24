use serde::{Serialize, Serializer};

use crate::otlp::cvattr::ConvertedAttrValue;

#[derive(Debug)]
pub enum JsonType<'a> {
    Int(i64),
    Str(&'a str),
    StrOwned(String),
    Double(f64),
}

impl<'a> From<&'a ConvertedAttrValue> for JsonType<'a> {
    fn from(value: &'a ConvertedAttrValue) -> Self {
        match value {
            ConvertedAttrValue::Int(i) => JsonType::Int(*i),
            ConvertedAttrValue::String(s) => JsonType::Str(s.as_str()),
            ConvertedAttrValue::Double(d) => JsonType::Double(*d),
        }
    }
}

impl From<ConvertedAttrValue> for JsonType<'static> {
    fn from(value: ConvertedAttrValue) -> Self {
        match value {
            ConvertedAttrValue::Int(i) => JsonType::Int(i),
            ConvertedAttrValue::String(s) => JsonType::StrOwned(s),
            ConvertedAttrValue::Double(d) => JsonType::Double(d),
        }
    }
}

impl<'a> Serialize for JsonType<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            JsonType::Int(i) => {
                let jsonint = JsonInt {
                    code: 0x0a,
                    value: *i,
                };
                jsonint.serialize(serializer)
            }
            JsonType::Str(s) => {
                let jsonstr = JsonStr {
                    code: 0x15,
                    value: s,
                };
                jsonstr.serialize(serializer)
            }
            JsonType::StrOwned(s) => {
                let jsonstr = JsonStr {
                    code: 0x15,
                    value: &s,
                };
                jsonstr.serialize(serializer)
            }
            JsonType::Double(d) => {
                let jsondouble = JsonDouble {
                    code: 0x0e,
                    value: *d,
                };
                jsondouble.serialize(serializer)
            }
        }
    }
}

#[derive(Serialize)]
struct JsonStr<'a> {
    code: u8,
    value: &'a str,
}

#[derive(Serialize)]
struct JsonInt {
    code: u8,
    value: i64,
}

#[derive(Serialize)]
struct JsonDouble {
    code: u8,
    value: f64,
}