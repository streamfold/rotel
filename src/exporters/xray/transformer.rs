// SPDX-License-Identifier: Apache-2.0

// Notice: Portions of this code are taken from https://github.com/CosmicMind/opentelemetry-xray
/* Copyright © 2025, CosmicMind, Inc. */
use crate::exporters::xray::request_builder::TransformPayload;
use crate::topology::payload::{Message, MessageMetadata};
use bstr::FromUtf8Error;
use opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span};
use opentelemetry_sdk::trace::TraceError;
use serde::de::Error;
use serde_json::Value;
use serde_json::{Error as JsonError, Map, json};
use std::str::Utf8Error;
use std::time::Duration;
use std::{env, io};
use thiserror::Error;

/// Container for XRay values with their associated metadata for proper acknowledgment
#[derive(Clone, Debug)]
pub struct XRayValuePayload {
    pub value: Value,
    pub metadata: Option<Vec<MessageMetadata>>,
}

impl XRayValuePayload {
    pub fn new(value: Value, metadata: Option<Vec<MessageMetadata>>) -> Self {
        Self { value, metadata }
    }
}

#[derive(Clone)]
pub struct Transformer {
    transformer: TraceTransformer,
}

impl Transformer {
    pub fn new(environment: String) -> Self {
        Self {
            transformer: TraceTransformer::new(environment),
        }
    }
}

impl TransformPayload<ResourceSpans> for Transformer {
    fn transform(
        &self,
        messages: Vec<Message<ResourceSpans>>,
    ) -> Result<Vec<XRayValuePayload>, ExportError> {
        let mut all_payloads = Vec::new();
        const MAX_SPANS_PER_CHUNK: usize = 50;

        // Buffer to accumulate spans across messages
        let mut span_buffer = Vec::new();
        // Track metadata from messages that contributed spans to the current buffer
        let mut metadata_tracking: Vec<MessageMetadata> = Vec::new();

        for mut message in messages.into_iter() {
            // Transform all spans from this message
            for rs in message.payload {
                for ss in rs.scope_spans {
                    for span in ss.spans {
                        match self.transformer.apply(span) {
                            Ok(v) => {
                                span_buffer.push(v);

                                // If buffer is full, create a request
                                if span_buffer.len() == MAX_SPANS_PER_CHUNK {
                                    let chunk_value =
                                        Value::Array(std::mem::take(&mut span_buffer));

                                    // Record this message's contribution if it has metadata
                                    if let Some(metadata) = message.metadata.as_ref() {
                                        metadata_tracking.push(metadata.clone());
                                    }

                                    // Build metadata list from all contributing messages
                                    let chunk_metadata = if metadata_tracking.is_empty() {
                                        None
                                    } else {
                                        Some(std::mem::take(&mut metadata_tracking))
                                    };

                                    all_payloads
                                        .push(XRayValuePayload::new(chunk_value, chunk_metadata));
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            }

            // Track this message's metadata for the current buffer if it has metadata
            if let Some(metadata) = message.metadata.take() {
                metadata_tracking.push(metadata);
            }
        }

        // Handle remaining spans in buffer
        if !span_buffer.is_empty() {
            let chunk_value = Value::Array(span_buffer);

            // Build metadata list from all contributing messages
            let chunk_metadata = if metadata_tracking.is_empty() {
                None
            } else {
                Some(metadata_tracking)
            };

            all_payloads.push(XRayValuePayload::new(chunk_value, chunk_metadata));
        }

        Ok(all_payloads)
    }
}

#[derive(Clone, Debug)]
pub struct TraceTransformer {
    environment: String,
}

/// Represents errors that can occur during export.
#[derive(Debug, Error)]
pub enum ExportError {
    #[error("Timestamp error")]
    TimestampError,
    #[error("Exporter is shut down")]
    Shutdown,
    #[error("Export timeout after {0:?}")]
    Timeout(Duration),
    #[error("Serialization error: {0}")]
    Serialization(#[from] JsonError),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Utf8 error: {0}")]
    Utf8Error(#[from] Utf8Error),
    #[error("Utf8 error: {0}")]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("Export failed: {message}, trace id: {trace_id}")]
    ExportFailed { message: String, trace_id: String },
}

/// A simple enum to control validation behavior.
#[derive(Debug)]
pub enum ValueType {
    HttpRequest,
    HttpResponse,
    #[allow(dead_code)]
    Exception,
    Annotation,
    Metadata,
}

impl From<ExportError> for TraceError {
    fn from(err: ExportError) -> Self {
        TraceError::Other(Box::new(err))
    }
}

fn validate_time_range(start: &u64, end: &u64) -> Result<(), ExportError> {
    if start > end {
        Err(ExportError::TimestampError)
    } else {
        Ok(())
    }
}

/// Formats an OpenTelemetry trace ID into the AWS X‑Ray format (1-XXXXXXXX-XXXXXXXXXXXXXXXX).
fn format_xray_trace_id(trace_id: Vec<u8>) -> Result<String, FromUtf8Error> {
    let hex = hex::encode(trace_id);
    Ok(format!("1-{}-{}", &hex[..8], &hex[8..]))
}

fn unix_nanos_to_epoch_seconds_f64(nanos: u64) -> f64 {
    (nanos as f64) / 1_000_000_000.0
}

fn validate_value(value: &str, value_type: ValueType, trace_id: &str) -> Result<(), ExportError> {
    // Size limits rationale:
    // 1. Generic values (256KB limit):
    //    This prevents any single value from being so large that it impacts performance or causes issues
    //    when transmitting telemetry data. It's a safeguard against unbounded data.
    // 2. HTTP values (8KB limit):
    //    HTTP headers and similar data typically adhere to smaller size limits. An 8KB cap aligns with
    //    common practices and expectations around HTTP header sizes.
    // 3. Exception messages (32KB limit):
    //    Exception details are usually verbose, but setting an upper bound (like 32KB) ensures that
    //    exceptionally long messages (which might be the result of unexpected conditions) don't overwhelm
    //    the telemetry system.
    // 4. Metadata (4KB limit):
    //    Metadata is typically concise information. Limiting metadata to a smaller size helps avoid
    //    performance issues and ensures that any accidental oversize data is caught.

    // 256KB limit for all generic values
    const MAX_VALUE_LENGTH: usize = 1024 * 256;

    // Check overall length against the generic limit.
    if value.len() > MAX_VALUE_LENGTH {
        return Err(ExportError::Serialization(JsonError::custom(format!(
            "{:?} value exceeds maximum length of {} bytes (trace_id: {})",
            value_type, MAX_VALUE_LENGTH, trace_id
        ))));
    }

    match value_type {
        ValueType::HttpRequest | ValueType::HttpResponse => {
            // For HTTP values:
            // - Disallow newline and carriage return characters to maintain header/content integrity.
            if value.contains('\n') || value.contains('\r') {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Invalid control characters in {:?} value (trace_id: {})",
                    value_type, trace_id
                ))));
            }
            // - Disallow null characters to avoid potential data corruption.
            if value.contains('\0') {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Null character not allowed in {:?} value (trace_id: {})",
                    value_type, trace_id
                ))));
            }
            // - HTTP values are further limited to 8KB to fit typical header size constraints.
            const MAX_HTTP_LENGTH: usize = 8192;
            if value.len() > MAX_HTTP_LENGTH {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "{:?} value exceeds maximum length of {} bytes (trace_id: {})",
                    value_type, MAX_HTTP_LENGTH, trace_id
                ))));
            }
        }
        ValueType::Exception => {
            // For exception values:
            // - They must not be empty to ensure meaningful error messages.
            if value.is_empty() {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Empty value not allowed in exception details (trace_id: {})",
                    trace_id
                ))));
            }
            // - Exception messages can be verbose, but are capped at 32KB.
            const MAX_EXCEPTION_LENGTH: usize = 32768;
            if value.len() > MAX_EXCEPTION_LENGTH {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Exception value exceeds maximum length (trace_id: {})",
                    trace_id
                ))));
            }
            // - Prevent exception messages that are purely whitespace.
            if value.trim().is_empty() {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Exception value cannot be only whitespace (trace_id: {})",
                    trace_id
                ))));
            }
        }
        ValueType::Metadata => {
            // For metadata values:
            // - Disallow control characters to ensure clean textual data.
            if value.chars().any(|c| c.is_control()) {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Metadata contains control characters (trace_id: {})",
                    trace_id
                ))));
            }
            // - Enforce a 4KB limit on metadata since it's typically concise.
            const MAX_METADATA_LENGTH: usize = 4096;
            if value.len() > MAX_METADATA_LENGTH {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Metadata value exceeds maximum length (trace_id: {})",
                    trace_id
                ))));
            }
            // - If metadata appears to be JSON (starts with '{'), ensure it decodes into a JSON object.
            if value.starts_with('{') {
                match serde_json::from_str::<serde_json::Value>(value) {
                    Ok(json) => {
                        if !json.is_object() {
                            return Err(ExportError::Serialization(JsonError::custom(format!(
                                "Metadata JSON must be an object (trace_id: {})",
                                trace_id
                            ))));
                        }
                    }
                    Err(_) => {
                        return Err(ExportError::Serialization(JsonError::custom(format!(
                            "Invalid JSON in metadata value (trace_id: {})",
                            trace_id
                        ))));
                    }
                }
            }
        }
        ValueType::Annotation => {
            // For annotations:
            // - They must not be empty.
            if value.is_empty() {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Empty value not allowed in annotation (trace_id: {})",
                    trace_id
                ))));
            }
            // - Annotations are meant to be brief, so we cap them at 2KB.
            const MAX_ANNOTATION_LENGTH: usize = 2048;
            if value.len() > MAX_ANNOTATION_LENGTH {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Annotation value exceeds maximum length (trace_id: {})",
                    trace_id
                ))));
            }
            // - Ensure annotations do not contain disallowed control characters.
            if !value.chars().all(|c| !c.is_control() || c == '\n') {
                return Err(ExportError::Serialization(JsonError::custom(format!(
                    "Annotation contains invalid control characters (trace_id: {})",
                    trace_id
                ))));
            }
        }
    }

    // Ensure the value can be correctly encoded as a JSON string.
    if let Err(e) = serde_json::to_string(value) {
        return Err(ExportError::Serialization(JsonError::custom(format!(
            "Invalid JSON string in {:?} value: {} (trace_id: {})",
            value_type, e, trace_id
        ))));
    }

    Ok(())
}

impl TraceTransformer {
    pub fn new(environment: String) -> Self {
        Self { environment }
    }

    pub fn apply(&self, span: Span) -> Result<Value, ExportError> {
        // Validate and process timestamps.
        validate_time_range(&span.start_time_unix_nano, &span.end_time_unix_nano)
            .map_err(|_| ExportError::TimestampError)?;

        let trace_id = format_xray_trace_id(span.trace_id)?;
        let span_id = hex::encode(span.span_id);
        let parent_id = hex::encode(span.parent_span_id);

        // Prepare maps for grouping attributes.
        let mut request: Map<String, Value> = Map::new();
        let mut response: Map<String, Value> = Map::new();
        let mut annotations: Map<String, Value> = Map::new();
        let mut metadata = Map::from_iter([("environment".into(), json!(self.environment))]);
        let mut aws_attrs: Map<String, Value> = Map::new();
        let mut exception: Map<String, Value> = Map::new();

        // Process span attributes.
        for kv in span.attributes.iter() {
            let key = kv.key.as_str();
            if key.starts_with("http.request.") {
                if let Some(value) = &kv.value {
                    if let Some(StringValue(s)) = &value.value {
                        validate_value(s, ValueType::HttpRequest, &trace_id)?;
                        request.insert(
                            key.strip_prefix("http.request.").unwrap().to_string(),
                            json!(s),
                        );
                    }
                }
            } else if key.starts_with("http.response.") {
                if let Some(value) = &kv.value {
                    if let Some(StringValue(s)) = &value.value {
                        validate_value(s, ValueType::HttpResponse, &trace_id)?;
                        response.insert(
                            key.strip_prefix("http.response.").unwrap().to_string(),
                            json!(s),
                        );
                    }
                }
            } else if key.starts_with("annotation.") {
                if let Some(value) = &kv.value {
                    if let Some(StringValue(s)) = &value.value {
                        validate_value(s, ValueType::Annotation, &trace_id)?;
                        annotations.insert(
                            key.strip_prefix("annotation.").unwrap().to_string(),
                            json!(s),
                        );
                    }
                }
            } else if key.starts_with("metadata.") {
                if let Some(value) = &kv.value {
                    if let Some(StringValue(s)) = &value.value {
                        validate_value(s, ValueType::Metadata, &trace_id)?;
                        metadata
                            .insert(key.strip_prefix("metadata.").unwrap().to_string(), json!(s));
                    }
                }
            } else if key.starts_with("aws.") {
                // Collect AWS-specific attributes in a generic AWS block.
                if let Some(value) = &kv.value {
                    if let Some(StringValue(s)) = &value.value {
                        aws_attrs.insert(key.strip_prefix("aws.").unwrap().to_string(), json!(s));
                    }
                }
            } else if key.starts_with("exception.") {
                // Collect AWS-specific attributes in a generic AWS block.
                if let Some(value) = &kv.value {
                    if let Some(StringValue(s)) = &value.value {
                        exception.insert(
                            key.strip_prefix("exception.").unwrap().to_string(),
                            json!(s),
                        );
                    }
                }
            }
        }

        // Optionally merge in generic AWS environment variables if not already provided.
        if !aws_attrs.contains_key("region") {
            if let Ok(region) = env::var("AWS_REGION") {
                aws_attrs.insert("region".to_string(), json!(region));
            }
        }
        if !aws_attrs.contains_key("account_id") {
            if let Ok(account_id) = env::var("AWS_ACCOUNT_ID") {
                aws_attrs.insert("account_id".to_string(), json!(account_id));
            }
        }

        // Convert start and end times to seconds since UNIX epoch.
        let start_time = unix_nanos_to_epoch_seconds_f64(span.start_time_unix_nano);
        let end_time = unix_nanos_to_epoch_seconds_f64(span.end_time_unix_nano);

        let span_type = if !parent_id.is_empty() {
            "subsegment"
        } else {
            "segment"
        };

        // Helper: parse numeric values.
        fn parse_numeric_value(value: &str) -> Value {
            if let Ok(n) = value.parse::<i64>() {
                json!(n)
            } else if let Ok(n) = value.parse::<f64>() {
                json!(n)
            } else {
                json!(value)
            }
        }

        // Build the base JSON segment.
        let mut segment = json!({
            "trace_id": trace_id,
            "id": span_id,
            "name": span.name,
            "start_time": start_time,
            "end_time": end_time,
            "type": span_type,
        });

        {
            let segment_obj = segment.as_object_mut().unwrap();
            if !request.is_empty() || !response.is_empty() {
                segment_obj.insert(
                    "http".to_string(),
                    json!({
                        "request": request,
                        "response": response
                    }),
                );
            }
            if !annotations.is_empty() {
                segment_obj.insert("annotations".to_string(), json!(annotations));
            }
            if !metadata.is_empty() {
                let parsed_metadata: Map<String, Value> = metadata
                    .into_iter()
                    .map(|(k, v)| (k, parse_numeric_value(v.as_str().unwrap_or(""))))
                    .collect();
                segment_obj.insert("metadata".to_string(), json!(parsed_metadata));
            }
            if !parent_id.is_empty() {
                segment_obj.insert("parent_id".to_string(), json!(parent_id));
            }
            if let Some(status) = response.get("status") {
                if let Some(status_str) = status.as_str() {
                    if status_str.starts_with('5') {
                        segment_obj.insert("fault".to_string(), json!(true));
                    } else if status_str == "429" {
                        segment_obj.insert("throttle".to_string(), json!(true));
                    } else if status_str.starts_with('4') {
                        segment_obj.insert("error".to_string(), json!(true));
                    }
                }
            }
            if !aws_attrs.is_empty() {
                segment_obj.insert("aws".to_string(), json!(aws_attrs));
            }
            if !exception.is_empty() {
                segment_obj.insert("exception".to_string(), json!(exception));
            }
        }
        Ok(segment)
    }
}
