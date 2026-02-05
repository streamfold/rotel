// SPDX-License-Identifier: Apache-2.0

//! Conversions between OpenTelemetry protobuf types and FFI-safe types.

use abi_stable::std_types::{RString, RVec};
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as OtelValue, AnyValue, InstrumentationScope, KeyValue,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    exponential_histogram_data_point::Buckets, metric::Data as OtelMetricData,
    number_data_point::Value as OtelNumberValue, Exemplar, ExponentialHistogram,
    ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
    ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    span::{Event, Link},
    ResourceSpans, ScopeSpans, Span, Status,
};

use crate::types::*;

// ============================================================================
// Common Types
// ============================================================================

impl From<KeyValue> for RKeyValue {
    fn from(kv: KeyValue) -> Self {
        RKeyValue {
            key: kv.key.into(),
            value: kv.value.map(|v| v.into()).into(),
        }
    }
}

impl From<RKeyValue> for KeyValue {
    fn from(rkv: RKeyValue) -> Self {
        KeyValue {
            key: rkv.key.into_string(),
            value: rkv.value.into_option().map(|v| v.into()),
        }
    }
}

impl From<AnyValue> for RAnyValue {
    fn from(av: AnyValue) -> Self {
        match av.value {
            Some(OtelValue::StringValue(s)) => RAnyValue::String(s.into()),
            Some(OtelValue::BoolValue(b)) => RAnyValue::Bool(b),
            Some(OtelValue::IntValue(i)) => RAnyValue::Int(i),
            Some(OtelValue::DoubleValue(d)) => RAnyValue::Double(d),
            Some(OtelValue::BytesValue(b)) => RAnyValue::Bytes(b.into()),
            Some(OtelValue::ArrayValue(arr)) => RAnyValue::Array(
                arr.values
                    .into_iter()
                    .map(|v| v.into())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            Some(OtelValue::KvlistValue(kvl)) => RAnyValue::KeyValueList(
                kvl.values
                    .into_iter()
                    .map(|kv| kv.into())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            None => RAnyValue::String(RString::new()),
        }
    }
}

impl From<RAnyValue> for AnyValue {
    fn from(rav: RAnyValue) -> Self {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

        let value = match rav {
            RAnyValue::String(s) => OtelValue::StringValue(s.into_string()),
            RAnyValue::Bool(b) => OtelValue::BoolValue(b),
            RAnyValue::Int(i) => OtelValue::IntValue(i),
            RAnyValue::Double(d) => OtelValue::DoubleValue(d),
            RAnyValue::Bytes(b) => OtelValue::BytesValue(b.into_iter().collect()),
            RAnyValue::Array(arr) => OtelValue::ArrayValue(ArrayValue {
                values: arr.into_iter().map(|v| v.into()).collect(),
            }),
            RAnyValue::KeyValueList(kvl) => OtelValue::KvlistValue(KeyValueList {
                values: kvl.into_iter().map(|kv| kv.into()).collect(),
            }),
        };
        AnyValue { value: Some(value) }
    }
}

impl From<Resource> for RResource {
    fn from(r: Resource) -> Self {
        RResource {
            attributes: r
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            dropped_attributes_count: r.dropped_attributes_count,
        }
    }
}

impl From<RResource> for Resource {
    fn from(rr: RResource) -> Self {
        Resource {
            attributes: rr.attributes.into_iter().map(|kv| kv.into()).collect(),
            dropped_attributes_count: rr.dropped_attributes_count,
            entity_refs: Vec::new(),
        }
    }
}

impl From<InstrumentationScope> for RInstrumentationScope {
    fn from(is: InstrumentationScope) -> Self {
        RInstrumentationScope {
            name: is.name.into(),
            version: is.version.into(),
            attributes: is
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            dropped_attributes_count: is.dropped_attributes_count,
        }
    }
}

impl From<RInstrumentationScope> for InstrumentationScope {
    fn from(ris: RInstrumentationScope) -> Self {
        InstrumentationScope {
            name: ris.name.into_string(),
            version: ris.version.into_string(),
            attributes: ris.attributes.into_iter().map(|kv| kv.into()).collect(),
            dropped_attributes_count: ris.dropped_attributes_count,
        }
    }
}

// ============================================================================
// Trace Types
// ============================================================================

impl From<ResourceSpans> for RResourceSpans {
    fn from(rs: ResourceSpans) -> Self {
        RResourceSpans {
            resource: rs.resource.map(|r| r.into()).into(),
            scope_spans: rs
                .scope_spans
                .into_iter()
                .map(|ss| ss.into())
                .collect::<Vec<_>>()
                .into(),
            schema_url: rs.schema_url.into(),
        }
    }
}

impl From<RResourceSpans> for ResourceSpans {
    fn from(rrs: RResourceSpans) -> Self {
        ResourceSpans {
            resource: rrs.resource.into_option().map(|r| r.into()),
            scope_spans: rrs.scope_spans.into_iter().map(|ss| ss.into()).collect(),
            schema_url: rrs.schema_url.into_string(),
        }
    }
}

impl From<ScopeSpans> for RScopeSpans {
    fn from(ss: ScopeSpans) -> Self {
        RScopeSpans {
            scope: ss.scope.map(|s| s.into()).into(),
            spans: ss
                .spans
                .into_iter()
                .map(|s| s.into())
                .collect::<Vec<_>>()
                .into(),
            schema_url: ss.schema_url.into(),
        }
    }
}

impl From<RScopeSpans> for ScopeSpans {
    fn from(rss: RScopeSpans) -> Self {
        ScopeSpans {
            scope: rss.scope.into_option().map(|s| s.into()),
            spans: rss.spans.into_iter().map(|s| s.into()).collect(),
            schema_url: rss.schema_url.into_string(),
        }
    }
}

impl From<Span> for RSpan {
    fn from(s: Span) -> Self {
        RSpan {
            trace_id: s.trace_id.into(),
            span_id: s.span_id.into(),
            trace_state: s.trace_state.into(),
            parent_span_id: s.parent_span_id.into(),
            flags: s.flags,
            name: s.name.into(),
            kind: s.kind,
            start_time_unix_nano: s.start_time_unix_nano,
            end_time_unix_nano: s.end_time_unix_nano,
            attributes: s
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            dropped_attributes_count: s.dropped_attributes_count,
            events: s
                .events
                .into_iter()
                .map(|e| e.into())
                .collect::<Vec<_>>()
                .into(),
            dropped_events_count: s.dropped_events_count,
            links: s
                .links
                .into_iter()
                .map(|l| l.into())
                .collect::<Vec<_>>()
                .into(),
            dropped_links_count: s.dropped_links_count,
            status: s.status.map(|st| st.into()).into(),
        }
    }
}

impl From<RSpan> for Span {
    fn from(rs: RSpan) -> Self {
        Span {
            trace_id: rs.trace_id.into_iter().collect(),
            span_id: rs.span_id.into_iter().collect(),
            trace_state: rs.trace_state.into_string(),
            parent_span_id: rs.parent_span_id.into_iter().collect(),
            flags: rs.flags,
            name: rs.name.into_string(),
            kind: rs.kind,
            start_time_unix_nano: rs.start_time_unix_nano,
            end_time_unix_nano: rs.end_time_unix_nano,
            attributes: rs.attributes.into_iter().map(|kv| kv.into()).collect(),
            dropped_attributes_count: rs.dropped_attributes_count,
            events: rs.events.into_iter().map(|e| e.into()).collect(),
            dropped_events_count: rs.dropped_events_count,
            links: rs.links.into_iter().map(|l| l.into()).collect(),
            dropped_links_count: rs.dropped_links_count,
            status: rs.status.into_option().map(|st| st.into()),
        }
    }
}

impl From<Event> for REvent {
    fn from(e: Event) -> Self {
        REvent {
            time_unix_nano: e.time_unix_nano,
            name: e.name.into(),
            attributes: e
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            dropped_attributes_count: e.dropped_attributes_count,
        }
    }
}

impl From<REvent> for Event {
    fn from(re: REvent) -> Self {
        Event {
            time_unix_nano: re.time_unix_nano,
            name: re.name.into_string(),
            attributes: re.attributes.into_iter().map(|kv| kv.into()).collect(),
            dropped_attributes_count: re.dropped_attributes_count,
        }
    }
}

impl From<Link> for RLink {
    fn from(l: Link) -> Self {
        RLink {
            trace_id: l.trace_id.into(),
            span_id: l.span_id.into(),
            trace_state: l.trace_state.into(),
            attributes: l
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            dropped_attributes_count: l.dropped_attributes_count,
            flags: l.flags,
        }
    }
}

impl From<RLink> for Link {
    fn from(rl: RLink) -> Self {
        Link {
            trace_id: rl.trace_id.into_iter().collect(),
            span_id: rl.span_id.into_iter().collect(),
            trace_state: rl.trace_state.into_string(),
            attributes: rl.attributes.into_iter().map(|kv| kv.into()).collect(),
            dropped_attributes_count: rl.dropped_attributes_count,
            flags: rl.flags,
        }
    }
}

impl From<Status> for RStatus {
    fn from(s: Status) -> Self {
        RStatus {
            message: s.message.into(),
            code: s.code,
        }
    }
}

impl From<RStatus> for Status {
    fn from(rs: RStatus) -> Self {
        Status {
            message: rs.message.into_string(),
            code: rs.code,
        }
    }
}

// ============================================================================
// Log Types
// ============================================================================

impl From<ResourceLogs> for RResourceLogs {
    fn from(rl: ResourceLogs) -> Self {
        RResourceLogs {
            resource: rl.resource.map(|r| r.into()).into(),
            scope_logs: rl
                .scope_logs
                .into_iter()
                .map(|sl| sl.into())
                .collect::<Vec<_>>()
                .into(),
            schema_url: rl.schema_url.into(),
        }
    }
}

impl From<RResourceLogs> for ResourceLogs {
    fn from(rrl: RResourceLogs) -> Self {
        ResourceLogs {
            resource: rrl.resource.into_option().map(|r| r.into()),
            scope_logs: rrl.scope_logs.into_iter().map(|sl| sl.into()).collect(),
            schema_url: rrl.schema_url.into_string(),
        }
    }
}

impl From<ScopeLogs> for RScopeLogs {
    fn from(sl: ScopeLogs) -> Self {
        RScopeLogs {
            scope: sl.scope.map(|s| s.into()).into(),
            log_records: sl
                .log_records
                .into_iter()
                .map(|lr| lr.into())
                .collect::<Vec<_>>()
                .into(),
            schema_url: sl.schema_url.into(),
        }
    }
}

impl From<RScopeLogs> for ScopeLogs {
    fn from(rsl: RScopeLogs) -> Self {
        ScopeLogs {
            scope: rsl.scope.into_option().map(|s| s.into()),
            log_records: rsl.log_records.into_iter().map(|lr| lr.into()).collect(),
            schema_url: rsl.schema_url.into_string(),
        }
    }
}

impl From<LogRecord> for RLogRecord {
    fn from(lr: LogRecord) -> Self {
        RLogRecord {
            time_unix_nano: lr.time_unix_nano,
            observed_time_unix_nano: lr.observed_time_unix_nano,
            severity_number: lr.severity_number,
            severity_text: lr.severity_text.into(),
            body: lr.body.map(|b| b.into()).into(),
            attributes: lr
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            dropped_attributes_count: lr.dropped_attributes_count,
            flags: lr.flags,
            trace_id: lr.trace_id.into(),
            span_id: lr.span_id.into(),
            event_name: lr.event_name.into(),
        }
    }
}

impl From<RLogRecord> for LogRecord {
    fn from(rlr: RLogRecord) -> Self {
        LogRecord {
            time_unix_nano: rlr.time_unix_nano,
            observed_time_unix_nano: rlr.observed_time_unix_nano,
            severity_number: rlr.severity_number,
            severity_text: rlr.severity_text.into_string(),
            body: rlr.body.into_option().map(|b| b.into()),
            attributes: rlr.attributes.into_iter().map(|kv| kv.into()).collect(),
            dropped_attributes_count: rlr.dropped_attributes_count,
            flags: rlr.flags,
            trace_id: rlr.trace_id.into_iter().collect(),
            span_id: rlr.span_id.into_iter().collect(),
            event_name: rlr.event_name.into_string(),
        }
    }
}

// ============================================================================
// Metric Types
// ============================================================================

impl From<ResourceMetrics> for RResourceMetrics {
    fn from(rm: ResourceMetrics) -> Self {
        RResourceMetrics {
            resource: rm.resource.map(|r| r.into()).into(),
            scope_metrics: rm
                .scope_metrics
                .into_iter()
                .map(|sm| sm.into())
                .collect::<Vec<_>>()
                .into(),
            schema_url: rm.schema_url.into(),
        }
    }
}

impl From<RResourceMetrics> for ResourceMetrics {
    fn from(rrm: RResourceMetrics) -> Self {
        ResourceMetrics {
            resource: rrm.resource.into_option().map(|r| r.into()),
            scope_metrics: rrm.scope_metrics.into_iter().map(|sm| sm.into()).collect(),
            schema_url: rrm.schema_url.into_string(),
        }
    }
}

impl From<ScopeMetrics> for RScopeMetrics {
    fn from(sm: ScopeMetrics) -> Self {
        RScopeMetrics {
            scope: sm.scope.map(|s| s.into()).into(),
            metrics: sm
                .metrics
                .into_iter()
                .map(|m| m.into())
                .collect::<Vec<_>>()
                .into(),
            schema_url: sm.schema_url.into(),
        }
    }
}

impl From<RScopeMetrics> for ScopeMetrics {
    fn from(rsm: RScopeMetrics) -> Self {
        ScopeMetrics {
            scope: rsm.scope.into_option().map(|s| s.into()),
            metrics: rsm.metrics.into_iter().map(|m| m.into()).collect(),
            schema_url: rsm.schema_url.into_string(),
        }
    }
}

impl From<Metric> for RMetric {
    fn from(m: Metric) -> Self {
        RMetric {
            name: m.name.into(),
            description: m.description.into(),
            unit: m.unit.into(),
            metadata: m
                .metadata
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            data: m.data.map(|d| d.into()).into(),
        }
    }
}

impl From<RMetric> for Metric {
    fn from(rm: RMetric) -> Self {
        Metric {
            name: rm.name.into_string(),
            description: rm.description.into_string(),
            unit: rm.unit.into_string(),
            metadata: rm.metadata.into_iter().map(|kv| kv.into()).collect(),
            data: rm.data.into_option().map(|d| d.into()),
        }
    }
}

impl From<OtelMetricData> for RMetricData {
    fn from(d: OtelMetricData) -> Self {
        match d {
            OtelMetricData::Gauge(g) => RMetricData::Gauge(g.into()),
            OtelMetricData::Sum(s) => RMetricData::Sum(s.into()),
            OtelMetricData::Histogram(h) => RMetricData::Histogram(h.into()),
            OtelMetricData::ExponentialHistogram(eh) => {
                RMetricData::ExponentialHistogram(eh.into())
            }
            OtelMetricData::Summary(s) => RMetricData::Summary(s.into()),
        }
    }
}

impl From<RMetricData> for OtelMetricData {
    fn from(rd: RMetricData) -> Self {
        match rd {
            RMetricData::Gauge(g) => OtelMetricData::Gauge(g.into()),
            RMetricData::Sum(s) => OtelMetricData::Sum(s.into()),
            RMetricData::Histogram(h) => OtelMetricData::Histogram(h.into()),
            RMetricData::ExponentialHistogram(eh) => {
                OtelMetricData::ExponentialHistogram(eh.into())
            }
            RMetricData::Summary(s) => OtelMetricData::Summary(s.into()),
        }
    }
}

impl From<Gauge> for RGauge {
    fn from(g: Gauge) -> Self {
        RGauge {
            data_points: g
                .data_points
                .into_iter()
                .map(|dp| dp.into())
                .collect::<Vec<_>>()
                .into(),
        }
    }
}

impl From<RGauge> for Gauge {
    fn from(rg: RGauge) -> Self {
        Gauge {
            data_points: rg.data_points.into_iter().map(|dp| dp.into()).collect(),
        }
    }
}

impl From<Sum> for RSum {
    fn from(s: Sum) -> Self {
        RSum {
            data_points: s
                .data_points
                .into_iter()
                .map(|dp| dp.into())
                .collect::<Vec<_>>()
                .into(),
            aggregation_temporality: s.aggregation_temporality,
            is_monotonic: s.is_monotonic,
        }
    }
}

impl From<RSum> for Sum {
    fn from(rs: RSum) -> Self {
        Sum {
            data_points: rs.data_points.into_iter().map(|dp| dp.into()).collect(),
            aggregation_temporality: rs.aggregation_temporality,
            is_monotonic: rs.is_monotonic,
        }
    }
}

impl From<NumberDataPoint> for RNumberDataPoint {
    fn from(dp: NumberDataPoint) -> Self {
        RNumberDataPoint {
            attributes: dp
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            value: dp.value.map(|v| v.into()).unwrap_or(RNumberValue::Int(0)),
            exemplars: dp
                .exemplars
                .into_iter()
                .map(|e| e.into())
                .collect::<Vec<_>>()
                .into(),
            flags: dp.flags,
        }
    }
}

impl From<RNumberDataPoint> for NumberDataPoint {
    fn from(rdp: RNumberDataPoint) -> Self {
        NumberDataPoint {
            attributes: rdp.attributes.into_iter().map(|kv| kv.into()).collect(),
            start_time_unix_nano: rdp.start_time_unix_nano,
            time_unix_nano: rdp.time_unix_nano,
            value: Some(rdp.value.into()),
            exemplars: rdp.exemplars.into_iter().map(|e| e.into()).collect(),
            flags: rdp.flags,
        }
    }
}

impl From<OtelNumberValue> for RNumberValue {
    fn from(v: OtelNumberValue) -> Self {
        match v {
            OtelNumberValue::AsInt(i) => RNumberValue::Int(i),
            OtelNumberValue::AsDouble(d) => RNumberValue::Double(d),
        }
    }
}

impl From<RNumberValue> for OtelNumberValue {
    fn from(rv: RNumberValue) -> Self {
        match rv {
            RNumberValue::Int(i) => OtelNumberValue::AsInt(i),
            RNumberValue::Double(d) => OtelNumberValue::AsDouble(d),
        }
    }
}

impl From<Histogram> for RHistogram {
    fn from(h: Histogram) -> Self {
        RHistogram {
            data_points: h
                .data_points
                .into_iter()
                .map(|dp| dp.into())
                .collect::<Vec<_>>()
                .into(),
            aggregation_temporality: h.aggregation_temporality,
        }
    }
}

impl From<RHistogram> for Histogram {
    fn from(rh: RHistogram) -> Self {
        Histogram {
            data_points: rh.data_points.into_iter().map(|dp| dp.into()).collect(),
            aggregation_temporality: rh.aggregation_temporality,
        }
    }
}

impl From<HistogramDataPoint> for RHistogramDataPoint {
    fn from(dp: HistogramDataPoint) -> Self {
        RHistogramDataPoint {
            attributes: dp
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            count: dp.count,
            sum: dp.sum.into(),
            bucket_counts: dp.bucket_counts.into(),
            explicit_bounds: dp.explicit_bounds.into(),
            exemplars: dp
                .exemplars
                .into_iter()
                .map(|e| e.into())
                .collect::<Vec<_>>()
                .into(),
            flags: dp.flags,
            min: dp.min.into(),
            max: dp.max.into(),
        }
    }
}

impl From<RHistogramDataPoint> for HistogramDataPoint {
    fn from(rdp: RHistogramDataPoint) -> Self {
        HistogramDataPoint {
            attributes: rdp.attributes.into_iter().map(|kv| kv.into()).collect(),
            start_time_unix_nano: rdp.start_time_unix_nano,
            time_unix_nano: rdp.time_unix_nano,
            count: rdp.count,
            sum: rdp.sum.into_option(),
            bucket_counts: rdp.bucket_counts.into_iter().collect(),
            explicit_bounds: rdp.explicit_bounds.into_iter().collect(),
            exemplars: rdp.exemplars.into_iter().map(|e| e.into()).collect(),
            flags: rdp.flags,
            min: rdp.min.into_option(),
            max: rdp.max.into_option(),
        }
    }
}

impl From<ExponentialHistogram> for RExponentialHistogram {
    fn from(eh: ExponentialHistogram) -> Self {
        RExponentialHistogram {
            data_points: eh
                .data_points
                .into_iter()
                .map(|dp| dp.into())
                .collect::<Vec<_>>()
                .into(),
            aggregation_temporality: eh.aggregation_temporality,
        }
    }
}

impl From<RExponentialHistogram> for ExponentialHistogram {
    fn from(reh: RExponentialHistogram) -> Self {
        ExponentialHistogram {
            data_points: reh.data_points.into_iter().map(|dp| dp.into()).collect(),
            aggregation_temporality: reh.aggregation_temporality,
        }
    }
}

impl From<ExponentialHistogramDataPoint> for RExponentialHistogramDataPoint {
    fn from(dp: ExponentialHistogramDataPoint) -> Self {
        RExponentialHistogramDataPoint {
            attributes: dp
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            count: dp.count,
            sum: dp.sum.into(),
            scale: dp.scale,
            zero_count: dp.zero_count,
            positive: dp.positive.map(|b| b.into()).unwrap_or(RBuckets {
                offset: 0,
                bucket_counts: RVec::new(),
            }),
            negative: dp.negative.map(|b| b.into()).unwrap_or(RBuckets {
                offset: 0,
                bucket_counts: RVec::new(),
            }),
            flags: dp.flags,
            exemplars: dp
                .exemplars
                .into_iter()
                .map(|e| e.into())
                .collect::<Vec<_>>()
                .into(),
            min: dp.min.into(),
            max: dp.max.into(),
            zero_threshold: dp.zero_threshold,
        }
    }
}

impl From<RExponentialHistogramDataPoint> for ExponentialHistogramDataPoint {
    fn from(rdp: RExponentialHistogramDataPoint) -> Self {
        ExponentialHistogramDataPoint {
            attributes: rdp.attributes.into_iter().map(|kv| kv.into()).collect(),
            start_time_unix_nano: rdp.start_time_unix_nano,
            time_unix_nano: rdp.time_unix_nano,
            count: rdp.count,
            sum: rdp.sum.into_option(),
            scale: rdp.scale,
            zero_count: rdp.zero_count,
            positive: Some(rdp.positive.into()),
            negative: Some(rdp.negative.into()),
            flags: rdp.flags,
            exemplars: rdp.exemplars.into_iter().map(|e| e.into()).collect(),
            min: rdp.min.into_option(),
            max: rdp.max.into_option(),
            zero_threshold: rdp.zero_threshold,
        }
    }
}

impl From<Buckets> for RBuckets {
    fn from(b: Buckets) -> Self {
        RBuckets {
            offset: b.offset,
            bucket_counts: b.bucket_counts.into(),
        }
    }
}

impl From<RBuckets> for Buckets {
    fn from(rb: RBuckets) -> Self {
        Buckets {
            offset: rb.offset,
            bucket_counts: rb.bucket_counts.into_iter().collect(),
        }
    }
}

impl From<Summary> for RSummary {
    fn from(s: Summary) -> Self {
        RSummary {
            data_points: s
                .data_points
                .into_iter()
                .map(|dp| dp.into())
                .collect::<Vec<_>>()
                .into(),
        }
    }
}

impl From<RSummary> for Summary {
    fn from(rs: RSummary) -> Self {
        Summary {
            data_points: rs.data_points.into_iter().map(|dp| dp.into()).collect(),
        }
    }
}

impl From<SummaryDataPoint> for RSummaryDataPoint {
    fn from(dp: SummaryDataPoint) -> Self {
        RSummaryDataPoint {
            attributes: dp
                .attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            count: dp.count,
            sum: dp.sum,
            quantile_values: dp
                .quantile_values
                .into_iter()
                .map(|qv| RQuantileValue {
                    quantile: qv.quantile,
                    value: qv.value,
                })
                .collect::<Vec<_>>()
                .into(),
            flags: dp.flags,
        }
    }
}

impl From<RSummaryDataPoint> for SummaryDataPoint {
    fn from(rdp: RSummaryDataPoint) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
        SummaryDataPoint {
            attributes: rdp.attributes.into_iter().map(|kv| kv.into()).collect(),
            start_time_unix_nano: rdp.start_time_unix_nano,
            time_unix_nano: rdp.time_unix_nano,
            count: rdp.count,
            sum: rdp.sum,
            quantile_values: rdp
                .quantile_values
                .into_iter()
                .map(|qv| ValueAtQuantile {
                    quantile: qv.quantile,
                    value: qv.value,
                })
                .collect(),
            flags: rdp.flags,
        }
    }
}

impl From<Exemplar> for RExemplar {
    fn from(e: Exemplar) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::exemplar::Value;
        RExemplar {
            filtered_attributes: e
                .filtered_attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect::<Vec<_>>()
                .into(),
            time_unix_nano: e.time_unix_nano,
            value: match e.value {
                Some(Value::AsInt(i)) => RNumberValue::Int(i),
                Some(Value::AsDouble(d)) => RNumberValue::Double(d),
                None => RNumberValue::Int(0),
            },
            span_id: e.span_id.into(),
            trace_id: e.trace_id.into(),
        }
    }
}

impl From<RExemplar> for Exemplar {
    fn from(re: RExemplar) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::exemplar::Value;
        Exemplar {
            filtered_attributes: re
                .filtered_attributes
                .into_iter()
                .map(|kv| kv.into())
                .collect(),
            time_unix_nano: re.time_unix_nano,
            value: Some(match re.value {
                RNumberValue::Int(i) => Value::AsInt(i),
                RNumberValue::Double(d) => Value::AsDouble(d),
            }),
            span_id: re.span_id.into_iter().collect(),
            trace_id: re.trace_id.into_iter().collect(),
        }
    }
}
