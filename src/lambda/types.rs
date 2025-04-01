use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterResponseBody {
    pub function_name: String,
    pub function_version: String,
    pub handler: String,
    pub account_id: Option<String>,

    // This is returned in a header
    #[serde(skip_deserializing)]
    pub extension_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryAPISubscribe {
    pub schema_version: String,
    pub types: Vec<String>,
    pub buffering: TelemetryAPISubscribeBuffering,
    pub destination: TelemetryAPISubscribeDestination,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryAPISubscribeBuffering {
    pub max_items: u32,
    pub max_bytes: u32,
    pub timeout_ms: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryAPISubscribeDestination {
    pub protocol: String,

    #[serde(rename = "URI")]
    pub uri: String,
}
