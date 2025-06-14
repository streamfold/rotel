use crate::model::common::RValue::*;
use crate::model::common::*;
use crate::model::logs::*;
use crate::model::trace::*;

use crate::model::metrics::{
    RExemplar, RExemplarValue, RExponentialHistogram, RExponentialHistogramBuckets,
    RExponentialHistogramDataPoint, RGauge, RHistogram, RHistogramDataPoint, RMetric, RMetricData,
    RNumberDataPoint, RNumberDataPointValue, RScopeMetrics, RSum, RSummary, RSummaryDataPoint,
    RValueAtQuantile,
};
use crate::model::resource::RResource;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1;
use std::mem;
use std::sync::{Arc, Mutex};

pub fn transform_spans(
    scope_spans: Vec<Arc<Mutex<RScopeSpans>>>,
) -> Vec<opentelemetry_proto::tonic::trace::v1::ScopeSpans> {
    let mut new_scope_spans = vec![];
    for ss in scope_spans.iter() {
        let ss = ss.lock().unwrap();
        let schema = ss.schema_url.clone();
        let scope = convert_scope(ss.scope.clone());
        let ss = ss.spans.lock().unwrap();
        let mut spans = vec![];
        for span in ss.iter() {
            let mut guard = span.lock().unwrap();
            let moved_data = mem::replace(
                &mut *guard,
                RSpan {
                    trace_id: vec![],
                    span_id: vec![],
                    trace_state: "".to_string(),
                    parent_span_id: vec![],
                    flags: 0,
                    name: "".to_string(),
                    kind: 0,
                    events_arc: None,
                    events_raw: vec![],
                    links_arc: None,
                    links_raw: vec![],
                    start_time_unix_nano: 0,
                    end_time_unix_nano: 0,
                    attributes_arc: None,
                    attributes_raw: vec![],
                    dropped_attributes_count: 0,
                    dropped_events_count: 0,
                    dropped_links_count: 0,
                    status: Arc::new(Mutex::new(None)),
                },
            );
            let status = Arc::into_inner(moved_data.status).unwrap();
            let status = status.into_inner().unwrap();
            spans.push(opentelemetry_proto::tonic::trace::v1::Span {
                trace_id: moved_data.trace_id,
                span_id: moved_data.span_id,
                trace_state: moved_data.trace_state,
                parent_span_id: moved_data.parent_span_id,
                flags: moved_data.flags,
                name: moved_data.name,
                kind: moved_data.kind,
                start_time_unix_nano: moved_data.start_time_unix_nano,
                end_time_unix_nano: moved_data.end_time_unix_nano,
                attributes: convert_attributes(
                    moved_data.attributes_raw,
                    moved_data.attributes_arc,
                ),
                dropped_attributes_count: moved_data.dropped_attributes_count,
                events: convert_events(moved_data.events_raw, moved_data.events_arc),
                links: convert_links(moved_data.links_raw, moved_data.links_arc),
                dropped_events_count: moved_data.dropped_events_count,
                dropped_links_count: moved_data.dropped_links_count,
                status: status.map(|s| opentelemetry_proto::tonic::trace::v1::Status {
                    message: s.message,
                    code: s.code,
                }),
            })
        }
        new_scope_spans.push(opentelemetry_proto::tonic::trace::v1::ScopeSpans {
            scope,
            spans,
            schema_url: schema,
        })
    }
    new_scope_spans
}

pub fn transform_logs(
    scope_logs: Vec<Arc<Mutex<RScopeLogs>>>,
) -> Vec<opentelemetry_proto::tonic::logs::v1::ScopeLogs> {
    let mut new_scope_logs = vec![];
    for sl in scope_logs.iter() {
        let sl = sl.lock().unwrap();
        let schema = sl.schema_url.clone();
        let scope = convert_scope(sl.scope.clone());
        let sl_records = sl.log_records.lock().unwrap();
        let mut log_records = vec![];
        for lr in sl_records.iter() {
            let mut guard = lr.lock().unwrap();
            let moved_data = mem::replace(
                &mut *guard,
                RLogRecord {
                    time_unix_nano: 0,
                    observed_time_unix_nano: 0,
                    severity_number: 0,
                    severity_text: "".to_string(),
                    body: Arc::new(Mutex::new(Some(RAnyValue {
                        value: Arc::new(Mutex::new(None)),
                    }))),
                    attributes_arc: None,
                    attributes_raw: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: "".to_string(),
                },
            );
            let body = Arc::into_inner(moved_data.body)
                .unwrap()
                .into_inner()
                .unwrap();
            let body: Option<AnyValue> = body.map_or(None, |b| Some(convert_value(b)));
            log_records.push(opentelemetry_proto::tonic::logs::v1::LogRecord {
                body,
                time_unix_nano: moved_data.time_unix_nano,
                observed_time_unix_nano: moved_data.observed_time_unix_nano,
                severity_number: moved_data.severity_number,
                severity_text: moved_data.severity_text,
                attributes: convert_attributes(
                    moved_data.attributes_raw,
                    moved_data.attributes_arc,
                ),
                dropped_attributes_count: moved_data.dropped_attributes_count,
                flags: moved_data.flags,
                trace_id: moved_data.trace_id,
                span_id: moved_data.span_id,
                event_name: moved_data.event_name,
            });
        }
        new_scope_logs.push(opentelemetry_proto::tonic::logs::v1::ScopeLogs {
            scope,
            log_records,
            schema_url: schema,
        });
    }
    new_scope_logs
}

// Renamed and re-typed based on your request
pub fn transform_metrics(scope_metrics: Vec<Arc<Mutex<RScopeMetrics>>>) -> Vec<v1::ScopeMetrics> {
    let mut new_scope_metrics = vec![];
    for sm_arc in scope_metrics.into_iter() {
        // Consume the outer Vec
        new_scope_metrics.push(transform_scope_metrics(sm_arc));
    }
    new_scope_metrics
}

// transform_resource_metrics is no longer a direct part of this new entry point
// and would only be needed if converting RResourceMetrics specifically.
// If it's used elsewhere, keep it; otherwise, it can be removed.
// For this response, I'm assuming its removal as it's not part of the requested chain.

fn transform_scope_metrics(sm_arc: Arc<Mutex<RScopeMetrics>>) -> v1::ScopeMetrics {
    let mut sm_guard = sm_arc.lock().unwrap();
    let moved_scope_metrics = mem::replace(
        &mut *sm_guard,
        RScopeMetrics {
            // Placeholder
            scope: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(vec![])),
            schema_url: Arc::new(Mutex::new("".to_string())),
        },
    );

    let scope = moved_scope_metrics.scope.lock().unwrap().take().map(|s| {
        opentelemetry_proto::tonic::common::v1::InstrumentationScope {
            name: s.name,
            version: s.version,
            attributes: convert_attributes(s.attributes_raw, s.attributes_arc), // This will consume internal Arcs
            dropped_attributes_count: s.dropped_attributes_count,
        }
    });

    let mut metrics_vec = moved_scope_metrics.metrics.lock().unwrap();
    let metrics = metrics_vec
        .drain(..) // Use drain
        .map(|m_arc| transform_metric(m_arc))
        .collect();

    v1::ScopeMetrics {
        scope,
        metrics,
        schema_url: Arc::into_inner(moved_scope_metrics.schema_url)
            .unwrap()
            .into_inner()
            .unwrap(),
    }
}

fn transform_metric(m_arc: Arc<Mutex<RMetric>>) -> v1::Metric {
    let mut m_guard = m_arc.lock().unwrap();
    let moved_metric = mem::replace(
        &mut *m_guard,
        RMetric {
            // Placeholder
            name: "".to_string(),
            description: "".to_string(),
            unit: "".to_string(),
            metadata: Arc::new(Mutex::new(vec![])),
            data: Arc::new(Mutex::new(None)),
        },
    );

    let m_data = Arc::into_inner(moved_metric.data)
        .unwrap()
        .into_inner()
        .unwrap();
    let data = match m_data {
        Some(RMetricData::Gauge(g)) => Some(v1::metric::Data::Gauge(transform_gauge(g))),
        Some(RMetricData::Sum(s)) => Some(v1::metric::Data::Sum(transform_sum(s))),
        Some(RMetricData::Histogram(h)) => {
            Some(v1::metric::Data::Histogram(transform_histogram(h)))
        }
        Some(RMetricData::ExponentialHistogram(eh)) => Some(
            v1::metric::Data::ExponentialHistogram(transform_exponential_histogram(eh)),
        ),
        Some(RMetricData::Summary(s)) => Some(v1::metric::Data::Summary(transform_summary(s))),
        None => None,
    };

    let x = v1::Metric {
        name: moved_metric.name,
        description: moved_metric.description,
        unit: moved_metric.unit,
        metadata: moved_metric
            .metadata
            .lock()
            .unwrap()
            .drain(..)
            .map(|kv| {
                let key = Arc::into_inner(kv.key).unwrap().into_inner().unwrap();
                let value = kv.value.lock().unwrap().take().map(convert_value);
                KeyValue { key, value }
            })
            .collect(),
        data,
    };
    x
}

fn transform_gauge(g: RGauge) -> v1::Gauge {
    let mut data_points_vec = g.data_points.lock().unwrap();
    let data_points = data_points_vec
        .drain(..)
        .map(|dp_arc| transform_number_data_point(dp_arc))
        .collect();
    v1::Gauge { data_points }
}

fn transform_sum(s: RSum) -> v1::Sum {
    let mut data_points_vec = s.data_points.lock().unwrap();
    let data_points = data_points_vec
        .drain(..)
        .map(|dp_arc| transform_number_data_point(dp_arc))
        .collect();
    v1::Sum {
        data_points,
        aggregation_temporality: s.aggregation_temporality,
        is_monotonic: s.is_monotonic,
    }
}

fn transform_histogram(h: RHistogram) -> v1::Histogram {
    let mut data_points_vec = h.data_points.lock().unwrap();
    let data_points = data_points_vec
        .drain(..)
        .map(|dp_arc| transform_histogram_data_point(dp_arc))
        .collect();
    v1::Histogram {
        data_points,
        aggregation_temporality: h.aggregation_temporality,
    }
}

fn transform_exponential_histogram(eh: RExponentialHistogram) -> v1::ExponentialHistogram {
    let mut data_points_vec = eh.data_points.lock().unwrap();
    let data_points = data_points_vec
        .drain(..)
        .map(|dp_arc| transform_exponential_histogram_data_point(dp_arc))
        .collect();
    v1::ExponentialHistogram {
        data_points,
        aggregation_temporality: eh.aggregation_temporality,
    }
}

fn transform_summary(s: RSummary) -> v1::Summary {
    let mut data_points_vec = s.data_points.lock().unwrap();
    let data_points = data_points_vec
        .drain(..)
        .map(|dp_arc| transform_summary_data_point(dp_arc))
        .collect();
    v1::Summary { data_points }
}

fn transform_number_data_point(ndp_arc: Arc<Mutex<RNumberDataPoint>>) -> v1::NumberDataPoint {
    let mut ndp_guard = ndp_arc.lock().unwrap();
    let moved_ndp = mem::replace(
        &mut *ndp_guard,
        RNumberDataPoint {
            // Placeholder
            attributes: Arc::new(Mutex::new(vec![])),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            exemplars: Arc::new(Mutex::new(vec![])),
            flags: 0,
            value: None,
        },
    );

    let value = match moved_ndp.value {
        Some(RNumberDataPointValue::AsDouble(d)) => Some(v1::number_data_point::Value::AsDouble(d)),
        Some(RNumberDataPointValue::AsInt(i)) => Some(v1::number_data_point::Value::AsInt(i)),
        None => None,
    };

    let mut exemplars_vec = moved_ndp.exemplars.lock().unwrap();
    let exemplars = exemplars_vec
        .drain(..)
        .map(|e_arc| transform_exemplar(e_arc))
        .collect();

    let x = v1::NumberDataPoint {
        attributes: moved_ndp
            .attributes
            .lock()
            .unwrap()
            .drain(..)
            .map(|kv_arc| {
                let key = Arc::into_inner(kv_arc.key).unwrap().into_inner().unwrap();
                let value = kv_arc.value.lock().unwrap().take().map(convert_value);
                KeyValue { key, value }
            })
            .collect(),
        start_time_unix_nano: moved_ndp.start_time_unix_nano,
        time_unix_nano: moved_ndp.time_unix_nano,
        exemplars,
        flags: moved_ndp.flags,
        value,
    };
    x
}

fn transform_histogram_data_point(
    hdp_arc: Arc<Mutex<RHistogramDataPoint>>,
) -> v1::HistogramDataPoint {
    let mut hdp_guard = hdp_arc.lock().unwrap();
    let moved_hdp = mem::replace(
        &mut *hdp_guard,
        RHistogramDataPoint {
            // Placeholder
            attributes: Arc::new(Mutex::new(vec![])),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            count: 0,
            sum: None,
            bucket_counts: vec![],
            explicit_bounds: vec![],
            exemplars: Arc::new(Mutex::new(vec![])),
            flags: 0,
            min: None,
            max: None,
        },
    );

    let mut exemplars_vec = moved_hdp.exemplars.lock().unwrap();
    let exemplars = exemplars_vec
        .drain(..)
        .map(|e_arc| transform_exemplar(e_arc))
        .collect();

    let x = v1::HistogramDataPoint {
        attributes: moved_hdp
            .attributes
            .lock()
            .unwrap()
            .drain(..)
            .map(|kv_arc| {
                let key = Arc::into_inner(kv_arc.key).unwrap().into_inner().unwrap();
                let value = kv_arc.value.lock().unwrap().take().map(convert_value);
                KeyValue { key, value }
            })
            .collect(),
        start_time_unix_nano: moved_hdp.start_time_unix_nano,
        time_unix_nano: moved_hdp.time_unix_nano,
        count: moved_hdp.count,
        sum: moved_hdp.sum,
        bucket_counts: moved_hdp.bucket_counts,
        explicit_bounds: moved_hdp.explicit_bounds,
        exemplars,
        flags: moved_hdp.flags,
        min: moved_hdp.min,
        max: moved_hdp.max,
    };
    x
}

fn transform_exponential_histogram_data_point(
    ehdp_arc: Arc<Mutex<RExponentialHistogramDataPoint>>,
) -> v1::ExponentialHistogramDataPoint {
    let mut ehdp_guard = ehdp_arc.lock().unwrap();
    let moved_ehdp = mem::replace(
        &mut *ehdp_guard,
        RExponentialHistogramDataPoint {
            // Placeholder
            attributes: Arc::new(Mutex::new(vec![])),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            count: 0,
            sum: None,
            scale: 0,
            zero_count: 0,
            positive: Arc::new(Mutex::new(None)),
            negative: Arc::new(Mutex::new(None)),
            flags: 0,
            exemplars: Arc::new(Mutex::new(vec![])),
            min: None,
            max: None,
            zero_threshold: 0.0,
        },
    );

    let positive_buckets = Arc::into_inner(moved_ehdp.positive)
        .unwrap()
        .into_inner()
        .unwrap();
    let positive_buckets = positive_buckets.map(transform_exponential_histogram_buckets);
    let negative_buckets = Arc::into_inner(moved_ehdp.negative)
        .unwrap()
        .into_inner()
        .unwrap();
    let negative_buckets = negative_buckets.map(transform_exponential_histogram_buckets);

    let mut exemplars_vec = moved_ehdp.exemplars.lock().unwrap();
    let exemplars = exemplars_vec
        .drain(..)
        .map(|e_arc| transform_exemplar(e_arc))
        .collect();

    let x = v1::ExponentialHistogramDataPoint {
        attributes: moved_ehdp
            .attributes
            .lock()
            .unwrap()
            .drain(..)
            .map(|kv_arc| {
                let key = Arc::into_inner(kv_arc.key).unwrap().into_inner().unwrap();
                let value = kv_arc.value.lock().unwrap().take().map(convert_value);
                KeyValue { key, value }
            })
            .collect(),
        start_time_unix_nano: moved_ehdp.start_time_unix_nano,
        time_unix_nano: moved_ehdp.time_unix_nano,
        count: moved_ehdp.count,
        sum: moved_ehdp.sum,
        scale: moved_ehdp.scale,
        zero_count: moved_ehdp.zero_count,
        positive: positive_buckets,
        negative: negative_buckets,
        flags: moved_ehdp.flags,
        exemplars,
        min: moved_ehdp.min,
        max: moved_ehdp.max,
        zero_threshold: moved_ehdp.zero_threshold,
    };
    x
}

fn transform_exponential_histogram_buckets(
    b: Arc<Mutex<RExponentialHistogramBuckets>>,
) -> v1::exponential_histogram_data_point::Buckets {
    let b = Arc::into_inner(b).unwrap().into_inner().unwrap();
    v1::exponential_histogram_data_point::Buckets {
        offset: b.offset,
        bucket_counts: b.bucket_counts,
    }
}

fn transform_summary_data_point(sdp_arc: Arc<Mutex<RSummaryDataPoint>>) -> v1::SummaryDataPoint {
    let mut sdp_guard = sdp_arc.lock().unwrap();
    let moved_sdp = mem::replace(
        &mut *sdp_guard,
        RSummaryDataPoint {
            // Placeholder
            attributes: Arc::new(Mutex::new(vec![])),
            start_time_unix_nano: 0,
            time_unix_nano: 0,
            count: 0,
            sum: 0.0,
            quantile_values: vec![],
            flags: 0,
        },
    );

    let quantile_values = moved_sdp
        .quantile_values
        .into_iter()
        .map(transform_value_at_quantile)
        .collect();

    let x = v1::SummaryDataPoint {
        attributes: moved_sdp
            .attributes
            .lock()
            .unwrap()
            .drain(..)
            .map(|kv_arc| {
                let key = Arc::into_inner(kv_arc.key).unwrap().into_inner().unwrap();
                let value = kv_arc.value.lock().unwrap().take().map(convert_value);
                KeyValue { key, value }
            })
            .collect(),
        start_time_unix_nano: moved_sdp.start_time_unix_nano,
        time_unix_nano: moved_sdp.time_unix_nano,
        count: moved_sdp.count,
        sum: moved_sdp.sum,
        quantile_values,
        flags: moved_sdp.flags,
    };
    x
}

fn transform_value_at_quantile(qv: RValueAtQuantile) -> v1::summary_data_point::ValueAtQuantile {
    v1::summary_data_point::ValueAtQuantile {
        quantile: qv.quantile,
        value: qv.value,
    }
}

fn transform_exemplar(e_arc: Arc<Mutex<RExemplar>>) -> v1::Exemplar {
    let mut e_guard = e_arc.lock().unwrap();
    let moved_exemplar = mem::replace(
        &mut *e_guard,
        RExemplar {
            // Placeholder
            filtered_attributes: Arc::new(Mutex::new(vec![])),
            time_unix_nano: 0,
            span_id: vec![],
            trace_id: vec![],
            value: None,
        },
    );

    let value = match moved_exemplar.value {
        Some(RExemplarValue::AsDouble(d)) => Some(v1::exemplar::Value::AsDouble(d)),
        Some(RExemplarValue::AsInt(i)) => Some(v1::exemplar::Value::AsInt(i)),
        None => None,
    };

    let x = v1::Exemplar {
        filtered_attributes: moved_exemplar
            .filtered_attributes
            .lock()
            .unwrap()
            .drain(..)
            .map(|kv_arc| {
                let key = Arc::into_inner(kv_arc.key).unwrap().into_inner().unwrap();
                let value = kv_arc.value.lock().unwrap().take().map(convert_value);
                KeyValue { key, value }
            })
            .collect(),
        time_unix_nano: moved_exemplar.time_unix_nano,
        span_id: moved_exemplar.span_id,
        trace_id: moved_exemplar.trace_id,
        value,
    };
    x
}

fn convert_events(
    events_raw: Vec<opentelemetry_proto::tonic::trace::v1::span::Event>,
    events_arc: Option<Arc<Mutex<Vec<Arc<Mutex<REvent>>>>>>,
) -> Vec<opentelemetry_proto::tonic::trace::v1::span::Event> {
    if events_arc.is_none() {
        return events_raw;
    }
    let events = Arc::into_inner(events_arc.unwrap()).unwrap();
    let mut events = events.into_inner().unwrap();
    events
        .drain(..) // Creates an iterator that removes all elements
        .map(|e| {
            let e = Arc::into_inner(e).unwrap();
            let e = e.into_inner().unwrap();
            opentelemetry_proto::tonic::trace::v1::span::Event {
                time_unix_nano: e.time_unix_nano,
                name: e.name.clone(),
                attributes: convert_attributes(vec![], Some(e.attributes)),
                dropped_attributes_count: e.dropped_attributes_count,
            }
        })
        .collect()
}

fn convert_links(
    links_raw: Vec<opentelemetry_proto::tonic::trace::v1::span::Link>,
    links_arc: Option<Arc<Mutex<Vec<Arc<Mutex<RLink>>>>>>,
) -> Vec<opentelemetry_proto::tonic::trace::v1::span::Link> {
    if links_arc.is_none() {
        return links_raw;
    }
    let links = Arc::into_inner(links_arc.unwrap()).unwrap();
    let mut links = links.into_inner().unwrap();
    links
        .drain(..) // Creates an iterator that removes all elements
        .map(|l| {
            let l = Arc::into_inner(l).unwrap();
            let l = l.into_inner().unwrap();
            opentelemetry_proto::tonic::trace::v1::span::Link {
                trace_id: l.trace_id,
                span_id: l.span_id,
                attributes: convert_attributes(vec![], Some(l.attributes)),
                dropped_attributes_count: l.dropped_attributes_count,
                trace_state: l.trace_state,
                flags: l.flags,
            }
        })
        .collect()
}

fn convert_scope(
    scope: Arc<Mutex<Option<RInstrumentationScope>>>,
) -> Option<opentelemetry_proto::tonic::common::v1::InstrumentationScope> {
    let guard = scope.lock().unwrap();
    if guard.is_none() {
        return None;
    }
    let scope = guard.clone().unwrap();
    let attrs = convert_attributes(scope.attributes_raw, scope.attributes_arc);
    Some(
        opentelemetry_proto::tonic::common::v1::InstrumentationScope {
            name: scope.name,
            version: scope.version,
            attributes: attrs,
            dropped_attributes_count: scope.dropped_attributes_count,
        },
    )
}

fn convert_attributes(
    attr_raw: Vec<KeyValue>,
    attrs_arc: Option<Arc<Mutex<Vec<RKeyValue>>>>,
) -> Vec<KeyValue> {
    if attrs_arc.is_none() {
        return attr_raw;
    }
    let attrs = attrs_arc.unwrap();
    let attrs = attrs.lock().unwrap();
    let mut new_attrs = vec![];
    for attr in attrs.iter() {
        let key = attr.key.lock().unwrap();
        let key = key.to_string();
        let mut any_value = attr.value.lock().unwrap();
        let any_value = any_value.take();
        match any_value {
            None => new_attrs.push(KeyValue { key, value: None }),
            Some(v) => {
                let converted = convert_value(v);
                new_attrs.push(KeyValue {
                    key,
                    value: Some(converted),
                })
            }
        }
    }
    new_attrs
}

pub fn transform_resource(
    resource: RResource,
) -> Option<opentelemetry_proto::tonic::resource::v1::Resource> {
    let attributes = Arc::into_inner(resource.attributes).unwrap();
    let attributes = attributes.into_inner().unwrap();
    let dropped_attributes_count = Arc::into_inner(resource.dropped_attributes_count).unwrap();
    let dropped_attributes_count = dropped_attributes_count.into_inner().unwrap();
    let mut new_attrs = vec![];
    for attr in attributes.iter() {
        let kv = attr.lock().unwrap();
        let key = kv.key.lock().unwrap();
        let key = key.to_string();
        let mut any_value = kv.value.lock().unwrap();
        let any_value = any_value.take();
        match any_value {
            None => new_attrs.push(KeyValue { key, value: None }),
            Some(v) => {
                let converted = convert_value(v);
                new_attrs.push(KeyValue {
                    key,
                    value: Some(converted),
                })
            }
        }
    }
    Some(opentelemetry_proto::tonic::resource::v1::Resource {
        attributes: new_attrs,
        dropped_attributes_count,
    })
}

pub fn convert_value(v: RAnyValue) -> opentelemetry_proto::tonic::common::v1::AnyValue {
    let inner_value = Arc::into_inner(v.value).unwrap();
    let inner_value = inner_value.into_inner().unwrap();
    if inner_value.is_none() {
        return opentelemetry_proto::tonic::common::v1::AnyValue { value: None };
    }
    match inner_value.unwrap() {
        StringValue(s) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)),
        },
        BoolValue(b) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)),
        },
        IntValue(i) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)),
        },
        DoubleValue(d) => opentelemetry_proto::tonic::common::v1::AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)),
        },
        RVArrayValue(a) => {
            let mut values = vec![];
            let inner_values = Arc::into_inner(a.values).unwrap();
            let mut inner_values = inner_values.into_inner().unwrap();
            while !inner_values.is_empty() {
                let inner_v = inner_values.pop().expect("inner value should not be none");
                let inner_v = Arc::into_inner(inner_v).unwrap();
                let inner_v = inner_v.into_inner().unwrap();
                if inner_v.is_none() {
                    values.push(opentelemetry_proto::tonic::common::v1::AnyValue { value: None })
                } else {
                    let converted = convert_value(inner_v.unwrap());
                    values.push(converted);
                }
            }
            opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(
                        opentelemetry_proto::tonic::common::v1::ArrayValue { values },
                    ),
                ),
            }
        }
        KvListValue(kvl) => {
            let mut values = vec![];
            let inner_values = Arc::into_inner(kvl.values).unwrap();
            let inner_values = inner_values.into_inner().unwrap();
            // TODO: We might need to remove these from the vec?
            for kv in inner_values {
                let key = Arc::into_inner(kv.key).unwrap();
                let key = key.into_inner().unwrap();
                let value = Arc::into_inner(kv.value).unwrap();
                let value = value.into_inner().unwrap();
                let mut new_value = None;
                if value.is_some() {
                    new_value = Some(convert_value(value.unwrap()));
                }
                values.push(KeyValue {
                    key,
                    value: new_value,
                });
            }
            AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(
                        opentelemetry_proto::tonic::common::v1::KeyValueList { values },
                    ),
                ),
            }
        }
        BytesValue(b) => AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b)),
        },
    }
}
