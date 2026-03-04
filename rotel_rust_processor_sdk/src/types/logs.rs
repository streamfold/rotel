// SPDX-License-Identifier: Apache-2.0

//! FFI-safe log types.

use abi_stable::std_types::{ROption, RString, RVec};
use abi_stable::StableAbi;

use super::common::{RAnyValue, RInstrumentationScope, RKeyValue, RResource};

/// FFI-safe equivalent of opentelemetry_proto ResourceLogs
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RResourceLogs {
    pub resource: ROption<RResource>,
    pub scope_logs: RVec<RScopeLogs>,
    pub schema_url: RString,
}

/// FFI-safe equivalent of opentelemetry_proto ScopeLogs
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RScopeLogs {
    pub scope: ROption<RInstrumentationScope>,
    pub log_records: RVec<RLogRecord>,
    pub schema_url: RString,
}

/// FFI-safe equivalent of opentelemetry_proto LogRecord
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RLogRecord {
    pub time_unix_nano: u64,
    pub observed_time_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: RString,
    pub body: ROption<RAnyValue>,
    pub attributes: RVec<RKeyValue>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
    pub trace_id: RVec<u8>,
    pub span_id: RVec<u8>,
    pub event_name: RString,
}
