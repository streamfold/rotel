use serde::{Serialize, Serializer};

//
// Trace spans
//

//
// *** NOTE ***
//
// The serde configuration of the column names here currently does not do anything. The column
// names are statically configured in the list below and must match both the ordering of this
// struct and the names created in the DB schema. If they ever get out of alignment, things
// will break. We'll look at ways to fix this in the future.
// *************

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SpanRow {
    pub(crate) timestamp: u64,
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) parent_span_id: String,
    pub(crate) trace_state: String,
    pub(crate) span_name: String,
    pub(crate) span_kind: String,
    pub(crate) service_name: String,
    pub(crate) resource_attributes: MapOrJson,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) span_attributes: MapOrJson,
    pub(crate) duration: i64,
    pub(crate) status_code: String,
    pub(crate) status_message: String,

    #[serde(rename = "Events.Timestamp")]
    pub(crate) events_timestamp: Vec<u64>,
    #[serde(rename = "Events.Name")]
    pub(crate) events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    pub(crate) events_attributes: Vec<MapOrJson>,

    #[serde(rename = "Links.TraceId")]
    pub(crate) links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    pub(crate) links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    pub(crate) links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    pub(crate) links_attributes: Vec<MapOrJson>,
}

pub fn get_span_row_col_keys() -> String {
    let fields = vec![
        "Timestamp",
        "TraceId",
        "SpanId",
        "ParentSpanId",
        "TraceState",
        "SpanName",
        "SpanKind",
        "ServiceName",
        "ResourceAttributes",
        "ScopeName",
        "ScopeVersion",
        "SpanAttributes",
        "Duration",
        "StatusCode",
        "StatusMessage",
        "Events.Timestamp",
        "Events.Name",
        "Events.Attributes",
        "Links.TraceId",
        "Links.SpanId",
        "Links.TraceState",
        "Links.Attributes",
    ];

    fields.join(",")
}

//
// Log records
//

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LogRecordRow {
    pub(crate) timestamp: u64,
    pub(crate) trace_id: String,
    pub(crate) span_id: String,
    pub(crate) trace_flags: u8,
    pub(crate) severity_text: String,
    pub(crate) severity_number: u8,
    pub(crate) service_name: String,
    pub(crate) body: String,
    pub(crate) resource_schema_url: String,
    pub(crate) resource_attributes: MapOrJson,
    pub(crate) scope_schema_url: String,
    pub(crate) scope_name: String,
    pub(crate) scope_version: String,
    pub(crate) scope_attributes: MapOrJson,
    pub(crate) log_attributes: MapOrJson,
}

pub fn get_log_row_col_keys() -> String {
    let fields = vec![
        "Timestamp",
        "TraceId",
        "SpanId",
        "TraceFlags",
        "SeverityText",
        "SeverityNumber",
        "ServiceName",
        "Body",
        "ResourceSchemaUrl",
        "ResourceAttributes",
        "ScopeSchemaUrl",
        "ScopeName",
        "ScopeVersion",
        "ScopeAttributes",
        "LogAttributes",
    ];

    fields.join(",")
}

#[derive(Debug)]
pub enum MapOrJson {
    Map(Vec<(String, String)>),
    Json(String),
}

impl Serialize for MapOrJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            MapOrJson::Map(map) => map.serialize(serializer),
            MapOrJson::Json(str) => str.serialize(serializer),
        }
    }
}
