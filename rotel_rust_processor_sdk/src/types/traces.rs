// SPDX-License-Identifier: Apache-2.0

//! FFI-safe trace types.

use abi_stable::std_types::{ROption, RString, RVec};
use abi_stable::StableAbi;

use super::common::{RInstrumentationScope, RKeyValue, RResource};

/// FFI-safe equivalent of opentelemetry_proto ResourceSpans
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RResourceSpans {
    pub resource: ROption<RResource>,
    pub scope_spans: RVec<RScopeSpans>,
    pub schema_url: RString,
}

/// FFI-safe equivalent of opentelemetry_proto ScopeSpans
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RScopeSpans {
    pub scope: ROption<RInstrumentationScope>,
    pub spans: RVec<RSpan>,
    pub schema_url: RString,
}

/// FFI-safe equivalent of opentelemetry_proto Span
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RSpan {
    pub trace_id: RVec<u8>,
    pub span_id: RVec<u8>,
    pub trace_state: RString,
    pub parent_span_id: RVec<u8>,
    pub flags: u32,
    pub name: RString,
    pub kind: i32,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: RVec<RKeyValue>,
    pub dropped_attributes_count: u32,
    pub events: RVec<REvent>,
    pub dropped_events_count: u32,
    pub links: RVec<RLink>,
    pub dropped_links_count: u32,
    pub status: ROption<RStatus>,
}

/// FFI-safe equivalent of opentelemetry_proto Span.Event
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct REvent {
    pub time_unix_nano: u64,
    pub name: RString,
    pub attributes: RVec<RKeyValue>,
    pub dropped_attributes_count: u32,
}

/// FFI-safe equivalent of opentelemetry_proto Span.Link
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RLink {
    pub trace_id: RVec<u8>,
    pub span_id: RVec<u8>,
    pub trace_state: RString,
    pub attributes: RVec<RKeyValue>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
}

/// FFI-safe equivalent of opentelemetry_proto Status
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct RStatus {
    pub message: RString,
    pub code: i32,
}
