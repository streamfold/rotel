// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::transform::attributes::find_key_in_attrlist;
use crate::exporters::datadog::transform::otel_mapping::attributes::HTTP_MAPPINGS;
use crate::exporters::datadog::transform::otel_util::get_otel_env;
use crate::exporters::datadog::transform::sampler::KEY_SAMPLING_RATE_EVENT_EXTRACTION;
use crate::exporters::datadog::transform::source::SourceKind::AWSECSFargateKind;
use crate::exporters::datadog::transform::{attributes, otel_mapping, otel_util, source};
use crate::exporters::datadog::types;
use crate::otlp::cvattr;
use crate::otlp::cvattr::{ConvertedAttrKeyValue, ConvertedAttrMap, ConvertedAttrValue};
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::trace::v1::span::{Event, SpanKind};
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span as OTelSpan, Span, Status};
use opentelemetry_semantic_conventions::{attribute, resource};
use std::collections::HashMap;
use std::rc::Rc;
use types::pb::{Span as DDSpan, TraceChunk, TracerPayload};

const TAG_CONTAINER_TAGS: &str = "_dd.tags.container";

#[derive(Clone, Debug)]
pub struct TraceTransformer {
    #[allow(dead_code)]
    environment: String,
    hostname: String,
}

pub struct ScopeSpan(pub Option<Rc<InstrumentationScope>>, pub OTelSpan);

impl TraceTransformer {
    pub fn new(environment: String, hostname: String) -> Self {
        Self {
            environment,
            hostname,
        }
    }

    pub fn apply(&self, resource_spans: ResourceSpans) -> TracerPayload {
        let mut traces: HashMap<u64, Vec<ScopeSpan>> = HashMap::new();
        let mut payload = TracerPayload::default();

        // Extract resource attributes
        let res_attrs = resource_spans.resource.unwrap_or_default().attributes;
        let res_attrs = cvattr::convert(&res_attrs);

        let resource_attrs: ConvertedAttrMap = res_attrs.into();

        let env = attributes::find_first_in_resource(
            &resource_attrs,
            vec![attribute::DEPLOYMENT_ENVIRONMENT_NAME],
            true,
        );
        let container_id = attributes::find_first_in_resource(
            &resource_attrs,
            vec![resource::CONTAINER_ID, resource::K8S_POD_UID],
            true,
        );
        let lang = attributes::find_first_in_resource(
            &resource_attrs,
            vec![resource::TELEMETRY_SDK_LANGUAGE],
            true,
        );
        let sdk_version = attributes::find_first_in_resource(
            &resource_attrs,
            vec![resource::TELEMETRY_SDK_VERSION],
            true,
        );

        let source = source::from_attributes(&resource_attrs)
            .unwrap_or_else(|| source::Source::from_hostname(self.hostname.clone()));
        let hostname = if source.kind == source::SourceKind::Hostname {
            source.identifier.clone()
        } else {
            "".to_string()
        };

        payload.hostname = hostname;
        payload.env = env;
        payload.container_id = container_id;
        payload.language_name = lang;
        payload.tracer_version = format!("otlp-{}", sdk_version);

        let mut ctags =
            otel_mapping::attributes::container_tags_from_resource_attributes(&resource_attrs);

        // todo: enrich with tags by looking up container ID

        if source.kind == AWSECSFargateKind {
            ctags.insert(source.kind.to_string(), source.identifier);
        }

        if !ctags.is_empty() {
            let flattened = ctags
                .iter()
                .map(|(k, v)| format!("{}:{}", k, v))
                .collect::<Vec<String>>()
                .join(",");
            payload
                .tags
                .insert(TAG_CONTAINER_TAGS.to_string(), flattened);
        }

        // Group spans by trace ID
        for scope_spans in resource_spans.scope_spans {
            let inst_scope = scope_spans.scope.map(Rc::new);
            for span in scope_spans.spans {
                let trace_id = convert_trace_id(&span.trace_id);
                traces
                    .entry(trace_id)
                    .or_default()
                    .push(ScopeSpan(inst_scope.clone(), span));
            }
        }

        // Convert each trace group to a TraceChunk
        for (_trace_id, spans) in traces {
            let mut trace_chunk = TraceChunk::default();

            // todo: get real values for these fields
            trace_chunk.priority = 1;

            // Not set by DD?
            //trace_chunk.origin = "lambda".into();

            trace_chunk.dropped_trace = false;
            trace_chunk.tags = HashMap::default(); // todo: extract common tags

            for scope_span in spans {
                let dd_span = self.otel_span_to_dd_span(&scope_span, &resource_attrs);
                trace_chunk.spans.push(dd_span);
            }

            payload.chunks.push(trace_chunk);
        }

        payload
    }

    fn otel_span_to_dd_span(
        &self,
        scope_span: &ScopeSpan,
        resource_attrs: &ConvertedAttrMap,
    ) -> DDSpan {
        let otel_span = &scope_span.1;
        let mut dd_span =
            self.otel_span_to_dd_span_minimal(resource_attrs, otel_span, &scope_span.0);

        resource_attrs
            .as_ref()
            .iter()
            .map(|(k, v)| (k, v.to_string()))
            .for_each(|(k, v)| set_meta_otlp_with_http_mappings(k, &v, &mut dd_span));

        dd_span.meta.insert(
            "otel.trace_id".to_string(),
            hex::encode(&otel_span.trace_id),
        );
        if !dd_span.meta.contains_key("version") {
            if let Some(svc_version) = resource_attrs.as_ref().get(attribute::SERVICE_VERSION) {
                dd_span
                    .meta
                    .insert("version".to_string(), svc_version.to_string());
            }
        }

        if !dd_span.meta.contains_key("env") {
            let env = get_otel_env(resource_attrs);
            if !env.is_empty() {
                dd_span.meta.insert("env".to_string(), env);
            };
        }

        if !otel_span.events.is_empty() {
            /* todo
            dd_span.meta.insert("events", marshal_events(otel_span.events);
             */
        }

        tag_span_if_contains_exception_event(otel_span, &mut dd_span);

        if !otel_span.links.is_empty() {
            /* todo
            dd_span.meta.insert("_dd.span_links", marshal_links(otel_span.links);
             */
        }

        let mut got_method_from_new_conv = false;
        let mut got_status_code_from_new_conv = false;
        for (key, value) in cvattr::convert(&otel_span.attributes)
            .iter()
            .map(|cv| (&cv.0, &cv.1))
        {
            match value {
                ConvertedAttrValue::Int(i) => set_metric_otlp(&mut dd_span, key, *i as f64),
                ConvertedAttrValue::Double(d) => set_metric_otlp(&mut dd_span, key, *d),
                ConvertedAttrValue::String(s) => {
                    set_meta_otlp_with_http_mappings(key, s, &mut dd_span)
                }
            }

            if key == "http.request.method" {
                got_method_from_new_conv = true;
            } else if key == "http.method" && !got_method_from_new_conv {
                dd_span
                    .meta
                    .insert("http.method".to_string(), value.to_string());
            }

            if key == "http.response.status_code" {
                got_status_code_from_new_conv = true;
            } else if key == "http.status_code" && !got_status_code_from_new_conv {
                dd_span
                    .meta
                    .insert("http.status_code".to_string(), value.to_string());
            }
        }

        if !otel_span.trace_state.is_empty() {
            dd_span
                .meta
                .insert("w3c.tracestate".to_string(), otel_span.trace_state.clone());
        }

        if let Some(inst_scope) = &scope_span.0 {
            if !inst_scope.name.is_empty() {
                #[allow(deprecated)]
                dd_span.meta.insert(
                    attribute::OTEL_LIBRARY_NAME.to_string(),
                    inst_scope.name.clone(),
                );
            }

            if !inst_scope.version.is_empty() {
                #[allow(deprecated)]
                dd_span.meta.insert(
                    attribute::OTEL_LIBRARY_VERSION.to_string(),
                    inst_scope.version.clone(),
                );
            }

            if let Some(status) = &otel_span.status {
                dd_span.meta.insert(
                    attribute::OTEL_STATUS_CODE.to_string(),
                    status.code.to_string(),
                );

                if !status.message.is_empty() {
                    dd_span.meta.insert(
                        attribute::OTEL_STATUS_DESCRIPTION.to_string(),
                        status.message.clone(),
                    );
                }

                status_to_error(status, &otel_span.events, &mut dd_span)
            }
        }

        dd_span
    }

    fn otel_span_to_dd_span_minimal(
        &self,
        resource_attrs: &ConvertedAttrMap,
        otel_span: &Span,
        scope_span: &Option<Rc<InstrumentationScope>>,
    ) -> DDSpan {
        let mut dd_span = DDSpan::default();

        let is_top_level = otel_span.parent_span_id.is_empty()
            || otel_span.kind() == SpanKind::Server
            || otel_span.kind() == SpanKind::Consumer;

        // Convert basic span information
        dd_span.service =
            otel_util::get_otel_service(otel_span, scope_span.clone(), resource_attrs, true);
        dd_span.name = otel_util::get_otel_operation_name_v2(otel_span);
        dd_span.resource =
            otel_util::get_otel_resource_v2(otel_span, scope_span.clone(), resource_attrs);

        dd_span.trace_id = convert_trace_id(&otel_span.trace_id);
        dd_span.span_id = convert_span_id(&otel_span.span_id);
        dd_span.parent_id = convert_span_id(&otel_span.parent_span_id);

        dd_span.start = otel_span.start_time_unix_nano as i64;
        dd_span.duration = (otel_span.end_time_unix_nano - otel_span.start_time_unix_nano) as i64;

        dd_span.r#type =
            otel_util::get_otel_span_type(otel_span, scope_span.clone(), resource_attrs);

        dd_span.meta.insert(
            "span.kind".to_string(),
            otel_util::span_kind_name(otel_span.kind()),
        );

        if let Some(status_code) = otel_util::status_code(otel_span) {
            dd_span
                .metrics
                .insert(otel_util::TAG_STATUS_CODE.to_string(), status_code as f64);
        }
        if let Some(status) = otel_span.status.clone() {
            if status.code() == StatusCode::Error {
                dd_span.error = 1
            }
        }
        if is_top_level {
            dd_span
                .meta
                .insert("_top_level".to_string(), "1".to_string());
        }

        if find_key_in_attrlist("_dd.measured", &otel_span.attributes)
            .is_some_and(|v| v.to_string() == *"1")
        {
            dd_span.metrics.insert("_dd.measured".to_string(), 1.0);
        } else if otel_span.kind() == SpanKind::Client || otel_span.kind() == SpanKind::Producer {
            // assumes the feature enable_otlp_compute_top_level_by_span_kind is active
            dd_span.metrics.insert("_dd.measured".to_string(), 1.0);
        }

        dd_span
    }
}

fn status_to_error(status: &Status, events: &Vec<Event>, dd_span: &mut DDSpan) {
    if status.code != StatusCode::Error as i32 {
        return;
    }

    dd_span.error = 1;

    for e in events {
        if e.name.to_lowercase() != "exception" {
            continue;
        }

        e.attributes
            .iter()
            .filter_map(|kv| kv.clone().try_into().ok())
            .filter_map(|kv: ConvertedAttrKeyValue| match kv.0.as_str() {
                attribute::EXCEPTION_MESSAGE => Some(("error.msg", kv.1)),
                attribute::EXCEPTION_TYPE => Some(("error.type", kv.1)),
                attribute::EXCEPTION_STACKTRACE => Some(("error.stack", kv.1)),
                _ => None,
            })
            .for_each(|(k, kv)| {
                dd_span.meta.insert(k.to_string(), kv.to_string());
            });
    }

    if dd_span.meta.contains_key("error.msg") {
        return;
    }

    // Find alternative error msgs

    if !status.message.is_empty() {
        dd_span
            .meta
            .insert("error.msg".to_string(), status.message.clone());
    } else if let Some(httpcode) = dd_span
        .meta
        .get("http.response.status_code")
        .or_else(|| dd_span.meta.get("http.status_code"))
    {
        if let Some(http_text) = dd_span.meta.get("http.status_text") {
            dd_span.meta.insert(
                "error.msg".to_string(),
                format!("{} {}", httpcode, http_text),
            );
        } else {
            dd_span
                .meta
                .insert("error.msg".to_string(), httpcode.clone());
        }
    }
}

fn tag_span_if_contains_exception_event(otel_span: &Span, dd_span: &mut DDSpan) {
    for event in &otel_span.events {
        if event.name == "exception" {
            dd_span.meta.insert(
                "_dd.span_events.has_exception".to_string(),
                "true".to_string(),
            );
            return;
        }
    }
}

fn set_meta_otlp_with_http_mappings(k: &str, value: &str, span: &mut DDSpan) {
    if !value.is_empty() {
        if let Some(dd_key) = HTTP_MAPPINGS.get(k).map(|s| s.to_string()) {
            span.meta.insert(dd_key, value.to_string());
            return;
        }
    }

    if let Some(rem) = k.strip_prefix("http.request.header.") {
        let key = format!("http.request.headers.{}", rem);
        span.meta.insert(key, value.to_string());
        return;
    }

    if !is_datadog_apm_convention_key(k) {
        set_meta_otlp(span, k, value);
    }
}

fn set_meta_otlp(span: &mut DDSpan, k: &str, value: &str) {
    match k {
        "operation.name" => span.name = value.to_string(),
        "service.name" => span.service = value.to_string(),
        "resource.name" => span.resource = value.to_string(),
        "span.type" => span.r#type = value.to_string(),
        "analytics.event" => {
            if let Ok(b) = value.parse::<bool>() {
                _ = match b {
                    true => span
                        .metrics
                        .insert(KEY_SAMPLING_RATE_EVENT_EXTRACTION.to_string(), 1.0),
                    false => span
                        .metrics
                        .insert(KEY_SAMPLING_RATE_EVENT_EXTRACTION.to_string(), 0.0),
                }
            }
        }
        _ => _ = span.meta.insert(k.to_string(), value.to_string()),
    }
}

fn set_metric_otlp(dd_span: &mut DDSpan, key: &String, value: f64) {
    if key == "sampling.priority" {
        dd_span
            .metrics
            .insert("_sampling_priority_v1".to_string(), value);
    } else {
        dd_span.metrics.insert(key.clone(), value);
    }
}

fn is_datadog_apm_convention_key(k: &str) -> bool {
    [
        "service.name",
        "operation.name",
        "resource.name",
        "span.type",
        "http.method",
        "http.status_code",
    ]
    .contains(&k)
}

fn convert_trace_id(trace_id: &[u8]) -> u64 {
    if trace_id.len() < 8 {
        return 0;
    }

    u64::from_be_bytes((&trace_id[(trace_id.len() - 8)..]).try_into().unwrap())
}

fn convert_span_id(span_id: &[u8]) -> u64 {
    if span_id.len() != 8 {
        return 0;
    }

    u64::from_be_bytes(span_id.try_into().unwrap())
}
