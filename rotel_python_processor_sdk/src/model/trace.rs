use crate::model::common::*;
use crate::model::RResource;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct RResourceSpans {
    pub resource: Arc<Mutex<Option<RResource>>>,
    pub scope_spans: Arc<Mutex<Vec<Arc<Mutex<RScopeSpans>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone)]
pub struct RScopeSpans {
    pub scope: Arc<Mutex<Option<RInstrumentationScope>>>,
    pub spans: Arc<Mutex<Vec<Arc<Mutex<RSpan>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone)]
pub struct RSpan {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub trace_state: String,
    pub parent_span_id: Vec<u8>,
    pub flags: u32,
    pub name: String,
    pub kind: i32,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes_arc: Option<Arc<Mutex<Vec<RKeyValue>>>>,
    pub attributes_raw: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub events_raw: Vec<Event>,
    pub events_arc: Option<Arc<Mutex<Vec<Arc<Mutex<REvent>>>>>>,
    pub dropped_events_count: u32,
    pub links_arc: Option<Arc<Mutex<Vec<Arc<Mutex<RLink>>>>>>,
    pub links_raw: Vec<Link>,
    pub dropped_links_count: u32,
    pub status: Arc<Mutex<Option<RStatus>>>,
}

#[derive(Debug, Clone)]
pub struct REvent {
    pub time_unix_nano: u64,
    pub name: String,
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone, Default)]
pub struct RStatus {
    pub message: String,
    pub code: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
pub enum RStatusCode {
    /// The default status.
    Unset = 0,
    /// The Span has been validated by an Application developer or Operator to
    /// have completed successfully.
    Ok = 1,
    /// The Span contains an error.
    Error = 2,
}

#[derive(Debug, Clone)]
pub struct RLink {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub trace_state: String,
    pub attributes: Arc<Mutex<Vec<RKeyValue>>>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
}
