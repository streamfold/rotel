// SPDX-License-Identifier: Apache-2.0

//! FFI-safe common types used across all telemetry types.

use abi_stable::std_types::{ROption, RString, RVec};
use abi_stable::StableAbi;

/// FFI-safe equivalent of opentelemetry_proto KeyValue
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RKeyValue {
    pub key: RString,
    pub value: ROption<RAnyValue>,
}

impl RKeyValue {
    pub fn new(key: impl Into<RString>, value: RAnyValue) -> Self {
        Self {
            key: key.into(),
            value: ROption::RSome(value),
        }
    }

    pub fn string(key: impl Into<RString>, value: impl Into<RString>) -> Self {
        Self::new(key, RAnyValue::String(value.into()))
    }

    pub fn int(key: impl Into<RString>, value: i64) -> Self {
        Self::new(key, RAnyValue::Int(value))
    }

    pub fn bool(key: impl Into<RString>, value: bool) -> Self {
        Self::new(key, RAnyValue::Bool(value))
    }

    pub fn double(key: impl Into<RString>, value: f64) -> Self {
        Self::new(key, RAnyValue::Double(value))
    }
}

/// FFI-safe equivalent of opentelemetry_proto AnyValue
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub enum RAnyValue {
    String(RString),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(RVec<u8>),
    Array(RVec<RAnyValue>),
    KeyValueList(RVec<RKeyValue>),
}

/// FFI-safe equivalent of opentelemetry_proto Resource
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RResource {
    pub attributes: RVec<RKeyValue>,
    pub dropped_attributes_count: u32,
}

/// FFI-safe equivalent of opentelemetry_proto InstrumentationScope
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RInstrumentationScope {
    pub name: RString,
    pub version: RString,
    pub attributes: RVec<RKeyValue>,
    pub dropped_attributes_count: u32,
}
