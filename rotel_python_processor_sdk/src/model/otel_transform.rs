use crate::model::common::RValue::{
    BoolValue, BytesValue, DoubleValue, IntValue, RVArrayValue, StringValue,
};
use crate::model::common::*;
use crate::model::logs::*;
use crate::model::metrics::*;
use crate::model::trace::*;
use std::sync::{Arc, Mutex};

pub fn transform_resource_spans(
    rs: opentelemetry_proto::tonic::trace::v1::ResourceSpans,
) -> RResourceSpans {
    let mut resource_span = RResourceSpans {
        resource: Arc::new(Mutex::new(None)),
        scope_spans: Arc::new(Mutex::new(vec![])),
        schema_url: rs.schema_url,
    };
    if rs.resource.is_some() {
        let resource = rs.resource.unwrap();
        let dropped_attributes_count = resource.dropped_attributes_count;
        let kvs = build_rotel_sdk_resource(resource);
        let res = Arc::new(Mutex::new(Some(crate::model::resource::RResource {
            attributes: Arc::new(Mutex::new(kvs.to_owned())),
            dropped_attributes_count: Arc::new(Mutex::new(dropped_attributes_count)),
        })));
        resource_span.resource = res.clone()
    }
    let mut scope_spans = vec![];
    for ss in rs.scope_spans {
        let mut scope = None;
        if ss.scope.is_some() {
            let s = ss.scope.unwrap();
            scope = Some(RInstrumentationScope {
                name: s.name,
                version: s.version,
                attributes_raw: s.attributes,
                attributes_arc: None,
                dropped_attributes_count: s.dropped_attributes_count,
            })
        }
        let scope_span = RScopeSpans {
            scope: Arc::new(Mutex::new(scope)),
            spans: Arc::new(Mutex::new(vec![])),
            schema_url: ss.schema_url,
        };
        for s in ss.spans {
            let span = RSpan {
                trace_id: s.trace_id,
                span_id: s.span_id,
                trace_state: s.trace_state,
                parent_span_id: s.parent_span_id,
                flags: s.flags,
                name: s.name,
                kind: s.kind,
                start_time_unix_nano: s.start_time_unix_nano,
                end_time_unix_nano: s.end_time_unix_nano,
                events_raw: s.events,
                events_arc: None,
                links_raw: s.links,
                links_arc: None,
                attributes_arc: None,
                attributes_raw: s.attributes,
                dropped_attributes_count: s.dropped_attributes_count,
                dropped_events_count: s.dropped_events_count,
                dropped_links_count: s.dropped_links_count,
                status: Arc::new(Mutex::new(s.status.map(|status| RStatus {
                    message: status.message,
                    code: status.code,
                }))),
            };
            scope_span
                .spans
                .lock()
                .unwrap()
                .push(Arc::new(Mutex::new(span)))
        }
        scope_spans.push(Arc::new(Mutex::new(scope_span)))
    }
    resource_span.scope_spans = Arc::new(Mutex::new(scope_spans));
    resource_span
}

pub fn transform_resource_logs(
    rl: opentelemetry_proto::tonic::logs::v1::ResourceLogs,
) -> RResourceLogs {
    let mut resource_logs = RResourceLogs {
        resource: Arc::new(Mutex::new(None)),
        scope_logs: Arc::new(Mutex::new(vec![])),
        schema_url: rl.schema_url,
    };
    if rl.resource.is_some() {
        let resource = rl.resource.unwrap();
        let dropped_attributes_count = resource.dropped_attributes_count;
        let kvs = build_rotel_sdk_resource(resource);
        let res = Arc::new(Mutex::new(Some(crate::model::resource::RResource {
            attributes: Arc::new(Mutex::new(kvs.to_owned())),
            dropped_attributes_count: Arc::new(Mutex::new(dropped_attributes_count)),
        })));
        resource_logs.resource = res.clone();
    }

    let mut scope_logs_vec = vec![];
    for sl in rl.scope_logs {
        let mut scope = None;
        if sl.scope.is_some() {
            let s = sl.scope.unwrap();
            scope = Some(RInstrumentationScope {
                name: s.name,
                version: s.version,
                attributes_raw: s.attributes,
                attributes_arc: None,
                dropped_attributes_count: s.dropped_attributes_count,
            });
        }

        let mut log_records_vec = vec![];
        for lr in sl.log_records {
            log_records_vec.push(Arc::new(Mutex::new(transform_log_record(lr))));
        }

        let scope_log = RScopeLogs {
            scope: Arc::new(Mutex::new(scope)),
            log_records: Arc::new(Mutex::new(log_records_vec)),
            schema_url: sl.schema_url,
        };
        scope_logs_vec.push(Arc::new(Mutex::new(scope_log)));
    }
    resource_logs.scope_logs = Arc::new(Mutex::new(scope_logs_vec));
    resource_logs
}

fn transform_log_record(lr: opentelemetry_proto::tonic::logs::v1::LogRecord) -> RLogRecord {
    RLogRecord {
        time_unix_nano: lr.time_unix_nano,
        observed_time_unix_nano: lr.observed_time_unix_nano,
        severity_number: lr.severity_number,
        severity_text: lr.severity_text,
        body: lr.body.map_or(
            Arc::new(Mutex::new(Some(RAnyValue {
                value: Arc::new(Mutex::new(None)),
            }))),
            |any_value_from_lr_body| {
                Arc::new(Mutex::new(Some(convert_value(any_value_from_lr_body))))
            },
        ),
        attributes_arc: None,
        attributes_raw: lr.attributes,
        dropped_attributes_count: lr.dropped_attributes_count,
        flags: lr.flags,
        trace_id: lr.trace_id,
        span_id: lr.span_id,
        event_name: lr.event_name,
    }
}

pub fn transform_resource_metrics(
    rm: opentelemetry_proto::tonic::metrics::v1::ResourceMetrics,
) -> RResourceMetrics {
    let mut resource_metrics = RResourceMetrics {
        resource: Arc::new(Mutex::new(None)),
        scope_metrics: Arc::new(Mutex::new(vec![])),
        schema_url: rm.schema_url,
    };

    if rm.resource.is_some() {
        let resource = rm.resource.unwrap();
        let dropped_attributes_count = resource.dropped_attributes_count;
        let kvs = build_rotel_sdk_resource(resource);
        let res = Arc::new(Mutex::new(Some(crate::model::resource::RResource {
            attributes: Arc::new(Mutex::new(kvs.to_owned())),
            dropped_attributes_count: Arc::new(Mutex::new(dropped_attributes_count)),
        })));
        resource_metrics.resource = res.clone();
    }

    let mut scope_metrics_vec = vec![];
    for sm in rm.scope_metrics {
        let scope = sm.scope.map(|s| RInstrumentationScope {
            name: s.name,
            version: s.version,
            attributes_raw: s.attributes,
            attributes_arc: None,
            dropped_attributes_count: s.dropped_attributes_count,
        });

        let mut metrics_vec = vec![];
        for m in sm.metrics {
            metrics_vec.push(Arc::new(Mutex::new(transform_metric(m))));
        }

        let scope_metrics = RScopeMetrics {
            scope: Arc::new(Mutex::new(scope)),
            metrics: Arc::new(Mutex::new(metrics_vec)),
            schema_url: Arc::new(Mutex::new(sm.schema_url)),
        };
        scope_metrics_vec.push(Arc::new(Mutex::new(scope_metrics)));
    }
    resource_metrics.scope_metrics = Arc::new(Mutex::new(scope_metrics_vec));
    resource_metrics
}

fn transform_metric(m: opentelemetry_proto::tonic::metrics::v1::Metric) -> RMetric {
    let data = match m.data {
        Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(g)) => {
            Some(RMetricData::Gauge(transform_gauge(g)))
        }
        Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(s)) => {
            Some(RMetricData::Sum(transform_sum(s)))
        }
        Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(h)) => {
            Some(RMetricData::Histogram(transform_histogram(h)))
        }
        Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::ExponentialHistogram(eh)) => {
            Some(RMetricData::ExponentialHistogram(
                transform_exponential_histogram(eh),
            ))
        }
        Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Summary(s)) => {
            Some(RMetricData::Summary(transform_summary(s)))
        }
        None => None,
    };

    RMetric {
        name: m.name,
        description: m.description,
        unit: m.unit,
        metadata: Arc::new(Mutex::new(convert_attributes(m.metadata))),
        data: Arc::new(Mutex::new(data)),
    }
}

fn transform_gauge(g: opentelemetry_proto::tonic::metrics::v1::Gauge) -> RGauge {
    let data_points = g
        .data_points
        .into_iter()
        .map(|dp| Arc::new(Mutex::new(transform_number_data_point(dp))))
        .collect();
    RGauge {
        data_points: Arc::new(Mutex::new(data_points)),
    }
}

fn transform_sum(s: opentelemetry_proto::tonic::metrics::v1::Sum) -> RSum {
    let data_points = s
        .data_points
        .into_iter()
        .map(|dp| Arc::new(Mutex::new(transform_number_data_point(dp))))
        .collect();
    RSum {
        data_points: Arc::new(Mutex::new(data_points)),
        aggregation_temporality: s.aggregation_temporality,
        is_monotonic: s.is_monotonic,
    }
}

fn transform_histogram(h: opentelemetry_proto::tonic::metrics::v1::Histogram) -> RHistogram {
    let data_points = h
        .data_points
        .into_iter()
        .map(|dp| Arc::new(Mutex::new(transform_histogram_data_point(dp))))
        .collect();
    RHistogram {
        data_points: Arc::new(Mutex::new(data_points)),
        aggregation_temporality: h.aggregation_temporality,
    }
}

fn transform_exponential_histogram(
    eh: opentelemetry_proto::tonic::metrics::v1::ExponentialHistogram,
) -> RExponentialHistogram {
    let data_points = eh
        .data_points
        .into_iter()
        .map(|dp| Arc::new(Mutex::new(transform_exponential_histogram_data_point(dp))))
        .collect();
    RExponentialHistogram {
        data_points: Arc::new(Mutex::new(data_points)),
        aggregation_temporality: eh.aggregation_temporality,
    }
}

fn transform_summary(s: opentelemetry_proto::tonic::metrics::v1::Summary) -> RSummary {
    let data_points = s
        .data_points
        .into_iter()
        .map(|dp| Arc::new(Mutex::new(transform_summary_data_point(dp))))
        .collect();
    RSummary {
        data_points: Arc::new(Mutex::new(data_points)),
    }
}

fn transform_number_data_point(
    ndp: opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
) -> RNumberDataPoint {
    let value = match ndp.value {
        Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(d)) => {
            Some(RNumberDataPointValue::AsDouble(d))
        }
        Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(i)) => {
            Some(RNumberDataPointValue::AsInt(i))
        }
        None => None,
    };

    let exemplars = ndp
        .exemplars
        .into_iter()
        .map(|e| Arc::new(Mutex::new(transform_exemplar(e))))
        .collect();

    RNumberDataPoint {
        attributes: Arc::new(Mutex::new(convert_attributes(ndp.attributes))),
        start_time_unix_nano: ndp.start_time_unix_nano,
        time_unix_nano: ndp.time_unix_nano,
        exemplars: Arc::new(Mutex::new(exemplars)),
        flags: ndp.flags,
        value,
    }
}

fn transform_histogram_data_point(
    hdp: opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
) -> RHistogramDataPoint {
    let exemplars = hdp
        .exemplars
        .into_iter()
        .map(|e| Arc::new(Mutex::new(transform_exemplar(e))))
        .collect();

    RHistogramDataPoint {
        attributes: Arc::new(Mutex::new(convert_attributes(hdp.attributes))),
        start_time_unix_nano: hdp.start_time_unix_nano,
        time_unix_nano: hdp.time_unix_nano,
        count: hdp.count,
        sum: hdp.sum,
        bucket_counts: hdp.bucket_counts,
        explicit_bounds: hdp.explicit_bounds,
        exemplars: Arc::new(Mutex::new(exemplars)),
        flags: hdp.flags,
        min: hdp.min,
        max: hdp.max,
    }
}

fn transform_exponential_histogram_data_point(
    ehdp: opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint,
) -> RExponentialHistogramDataPoint {
    let positive_buckets = ehdp.positive.map(|b| RExponentialHistogramBuckets {
        offset: b.offset,
        bucket_counts: b.bucket_counts,
    });
    let negative_buckets = ehdp.negative.map(|b| RExponentialHistogramBuckets {
        offset: b.offset,
        bucket_counts: b.bucket_counts,
    });

    let exemplars = ehdp
        .exemplars
        .into_iter()
        .map(|e| Arc::new(Mutex::new(transform_exemplar(e))))
        .collect();

    RExponentialHistogramDataPoint {
        attributes: Arc::new(Mutex::new(convert_attributes(ehdp.attributes))),
        start_time_unix_nano: ehdp.start_time_unix_nano,
        time_unix_nano: ehdp.time_unix_nano,
        count: ehdp.count,
        sum: ehdp.sum,
        scale: ehdp.scale,
        zero_count: ehdp.zero_count,
        positive: positive_buckets,
        negative: negative_buckets,
        flags: ehdp.flags,
        exemplars: Arc::new(Mutex::new(exemplars)),
        min: ehdp.min,
        max: ehdp.max,
        zero_threshold: ehdp.zero_threshold,
    }
}

fn transform_summary_data_point(
    sdp: opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
) -> RSummaryDataPoint {
    let quantile_values = sdp
        .quantile_values
        .into_iter()
        .map(|qv| RValueAtQuantile {
            quantile: qv.quantile,
            value: qv.value,
        })
        .collect();

    RSummaryDataPoint {
        attributes: Arc::new(Mutex::new(convert_attributes(sdp.attributes))),
        start_time_unix_nano: sdp.start_time_unix_nano,
        time_unix_nano: sdp.time_unix_nano,
        count: sdp.count,
        sum: sdp.sum,
        quantile_values,
        flags: sdp.flags,
    }
}

fn transform_exemplar(e: opentelemetry_proto::tonic::metrics::v1::Exemplar) -> RExemplar {
    let value = match e.value {
        Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(d)) => {
            Some(RExemplarValue::AsDouble(d))
        }
        Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsInt(i)) => {
            Some(RExemplarValue::AsInt(i))
        }
        None => None,
    };

    RExemplar {
        filtered_attributes: Arc::new(Mutex::new(convert_attributes(e.filtered_attributes))),
        time_unix_nano: e.time_unix_nano,
        span_id: e.span_id,
        trace_id: e.trace_id,
        value,
    }
}

pub fn convert_attributes(
    attributes: Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
) -> Vec<RKeyValue> {
    let mut kvs = vec![];
    for a in attributes {
        let key = Arc::new(Mutex::new(a.key));
        let any_value = a.value;
        match any_value {
            None => kvs.push(RKeyValue {
                key,
                value: Arc::new(Mutex::new(None)),
            }),
            Some(v) => {
                let converted = convert_value(v);
                kvs.push(RKeyValue {
                    key,
                    value: Arc::new(Mutex::new(Some(converted))),
                })
            }
        }
    }
    kvs
}

fn build_rotel_sdk_resource(
    resource: opentelemetry_proto::tonic::resource::v1::Resource,
) -> Vec<Arc<Mutex<RKeyValue>>> {
    let mut kvs = vec![];
    for a in resource.attributes {
        let key = Arc::new(Mutex::new(a.key));
        let any_value = a.value;
        match any_value {
            None => kvs.push(Arc::new(Mutex::new(RKeyValue {
                key,
                value: Arc::new(Mutex::new(None)),
            }))),
            Some(v) => {
                let converted = convert_value(v);
                kvs.push(Arc::new(Mutex::new(RKeyValue {
                    key,
                    value: Arc::new(Mutex::new(Some(converted))),
                })))
            }
        }
    }
    kvs
}

pub fn convert_value(v: opentelemetry_proto::tonic::common::v1::AnyValue) -> RAnyValue {
    match v.value {
        None => RAnyValue {
            value: Arc::new(Mutex::new(None)),
        },
        Some(v) => match v {
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s) => RAnyValue {
                value: Arc::new(Mutex::new(Some(StringValue(s)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b) => RAnyValue {
                value: Arc::new(Mutex::new(Some(BoolValue(b)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i) => RAnyValue {
                value: Arc::new(Mutex::new(Some(IntValue(i)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d) => RAnyValue {
                value: Arc::new(Mutex::new(Some(DoubleValue(d)))),
            },
            opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(a) => {
                let mut values = vec![];
                for v in a.values.iter() {
                    let conv = convert_value(v.clone());
                    values.push(Arc::new(Mutex::new(Some(conv))));
                }
                RAnyValue {
                    value: Arc::new(Mutex::new(Some(RVArrayValue(RArrayValue {
                        values: Arc::new(Mutex::new(values)),
                    })))),
                }
            }
            opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(kvl) => {
                let mut key_values = vec![];
                for kv in kvl.values {
                    let mut new_value = None;
                    if kv.value.is_some() {
                        new_value = Some(convert_value(kv.value.unwrap()));
                    }
                    key_values.push(RKeyValue {
                        key: Arc::new(Mutex::new(kv.key)),
                        value: Arc::new(Mutex::new(new_value)),
                    })
                }
                RAnyValue {
                    value: Arc::new(Mutex::new(Some(RValue::KvListValue(RKeyValueList {
                        values: Arc::new(Mutex::new(key_values)),
                    })))),
                }
            }
            opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b) => RAnyValue {
                value: Arc::new(Mutex::new(Some(BytesValue(b)))),
            },
        },
    }
}
