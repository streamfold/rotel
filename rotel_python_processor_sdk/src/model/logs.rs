use crate::model::common::*;
use crate::model::resource::RResource;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use std::sync::{Arc, Mutex};

// Structures for Logs
#[derive(Debug, Clone)]
pub struct RResourceLogs {
    pub resource: Arc<Mutex<Option<RResource>>>,
    pub scope_logs: Arc<Mutex<Vec<Arc<Mutex<RScopeLogs>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone)]
pub struct RScopeLogs {
    pub scope: Arc<Mutex<Option<RInstrumentationScope>>>,
    pub log_records: Arc<Mutex<Vec<Arc<Mutex<RLogRecord>>>>>,
    pub schema_url: String,
}

#[derive(Debug, Clone)]
pub struct RLogRecord {
    pub time_unix_nano: u64,
    pub observed_time_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: String,
    pub body: Arc<Mutex<Option<RAnyValue>>>,
    pub attributes_arc: Option<Arc<Mutex<Vec<RKeyValue>>>>,
    pub attributes_raw: Vec<KeyValue>,
    pub dropped_attributes_count: u32,
    pub flags: u32,
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub event_name: String,
}
