use crate::exporters::clickhouse::payload::{ClickhousePayload, ClickhousePayloadBuilder};
use crate::exporters::clickhouse::request_builder::TransformPayload;
use crate::exporters::clickhouse::request_mapper::RequestType;
use crate::exporters::clickhouse::schema::{
    MapOrJson, MetricsExemplars, MetricsExpHistogramRow, MetricsGaugeRow, MetricsHistogramRow,
    MetricsMeta, MetricsSumRow, MetricsSummaryRow,
};
use crate::exporters::clickhouse::transformer::{
    Transformer, find_attribute, get_scope_properties,
};
use crate::otlp::cvattr;
use opentelemetry_proto::tonic::metrics::v1::exemplar::Value;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as DataPointValue;
use opentelemetry_proto::tonic::metrics::v1::{Exemplar, ResourceMetrics};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use std::collections::HashMap;
use tower::BoxError;

impl TransformPayload<ResourceMetrics> for Transformer {
    fn transform(
        &self,
        input: Vec<ResourceMetrics>,
    ) -> Result<Vec<(RequestType, ClickhousePayload)>, BoxError> {
        let mut payloads = HashMap::new();

        for rm in input {
            let res_attrs = rm.resource.unwrap_or_default().attributes;
            let res_attrs = cvattr::convert(&res_attrs);
            let service_name = find_attribute(SERVICE_NAME, &res_attrs);

            for sm in rm.scope_metrics {
                let (scope_name, scope_version) = get_scope_properties(sm.scope.as_ref());
                let scope_attrs = match &sm.scope {
                    None => Vec::new(),
                    Some(scope) => cvattr::convert(&scope.attributes),
                };

                let dropped_attr_count = sm.scope.map(|s| s.dropped_attributes_count).unwrap_or(0);

                for metric in sm.metrics {
                    if let Some(data) = metric.data {
                        let mut meta = MetricsMeta {
                            resource_attributes: self.transform_attrs(&res_attrs),
                            resource_schema_url: rm.schema_url.clone(),
                            scope_name: scope_name.clone(),
                            scope_version: scope_version.clone(),
                            scope_attributes: self.transform_attrs(&scope_attrs),
                            scope_dropped_attr_count: dropped_attr_count,
                            scope_schema_url: sm.schema_url.clone(),
                            service_name: service_name.clone(),
                            metric_name: metric.name,
                            metric_description: metric.description,
                            metric_unit: metric.unit,
                            // Placeholder values that will be replaced per data point
                            attributes: MapOrJson::Json("".to_string()),
                            start_time_unix: 0,
                            time_unix: 0,
                        };

                        match data {
                            Data::Sum(s) => {
                                for dp in s.data_points {
                                    let attrs = cvattr::convert(&dp.attributes);

                                    meta.attributes = self.transform_attrs(&attrs);
                                    meta.start_time_unix = dp.start_time_unix_nano;
                                    meta.time_unix = dp.time_unix_nano;

                                    let row = MetricsSumRow {
                                        meta: &meta,

                                        value: get_metric_value(dp.value),
                                        flags: dp.flags,
                                        aggregation_temporality: s.aggregation_temporality,
                                        is_monotonic: s.is_monotonic,
                                        exemplars: self.parse_exemplars(&dp.exemplars),
                                    };

                                    let e = payloads.entry(RequestType::MetricsSum).or_insert(
                                        ClickhousePayloadBuilder::new(self.compression.clone()),
                                    );

                                    e.add_row(&row)?;
                                }
                            }
                            Data::Gauge(g) => {
                                for dp in g.data_points {
                                    let attrs = cvattr::convert(&dp.attributes);

                                    meta.attributes = self.transform_attrs(&attrs);
                                    meta.start_time_unix = dp.start_time_unix_nano;
                                    meta.time_unix = dp.time_unix_nano;

                                    let row = MetricsGaugeRow {
                                        meta: &meta,

                                        value: get_metric_value(dp.value),
                                        flags: dp.flags,
                                        exemplars: self.parse_exemplars(&dp.exemplars),
                                    };

                                    let e = payloads.entry(RequestType::MetricsGauge).or_insert(
                                        ClickhousePayloadBuilder::new(self.compression.clone()),
                                    );

                                    e.add_row(&row)?;
                                }
                            }
                            Data::Histogram(h) => {
                                for dp in h.data_points {
                                    let attrs = cvattr::convert(&dp.attributes);

                                    meta.attributes = self.transform_attrs(&attrs);
                                    meta.start_time_unix = dp.start_time_unix_nano;
                                    meta.time_unix = dp.time_unix_nano;

                                    let row = MetricsHistogramRow {
                                        meta: &meta,

                                        count: dp.count,
                                        sum: dp.sum.unwrap_or(0.0),
                                        bucket_counts: dp.bucket_counts,
                                        explicit_bounds: dp.explicit_bounds,
                                        flags: dp.flags,
                                        min: dp.min.unwrap_or(0.0),
                                        max: dp.max.unwrap_or(0.0),
                                        aggregation_temporality: h.aggregation_temporality,
                                        exemplars: self.parse_exemplars(&dp.exemplars),
                                    };

                                    let e =
                                        payloads.entry(RequestType::MetricsHistogram).or_insert(
                                            ClickhousePayloadBuilder::new(self.compression.clone()),
                                        );

                                    e.add_row(&row)?;
                                }
                            }
                            Data::ExponentialHistogram(e) => {
                                for dp in e.data_points {
                                    let attrs = cvattr::convert(&dp.attributes);

                                    meta.attributes = self.transform_attrs(&attrs);
                                    meta.start_time_unix = dp.start_time_unix_nano;
                                    meta.time_unix = dp.time_unix_nano;

                                    let mut row = MetricsExpHistogramRow {
                                        meta: &meta,

                                        count: dp.count,
                                        sum: dp.sum.unwrap_or(0.0),
                                        scale: dp.scale,
                                        zero_count: dp.zero_count,
                                        positive_offset: 0,
                                        positive_bucket_counts: vec![],
                                        negative_offset: 0,
                                        negative_bucket_counts: vec![],
                                        flags: dp.flags,
                                        min: dp.min.unwrap_or(0.0),
                                        max: dp.max.unwrap_or(0.0),
                                        aggregation_temporality: e.aggregation_temporality,
                                        exemplars: self.parse_exemplars(&dp.exemplars),
                                    };

                                    if let Some(pos) = dp.positive {
                                        row.positive_offset = pos.offset;
                                        row.positive_bucket_counts = pos.bucket_counts;
                                    }

                                    if let Some(neg) = dp.negative {
                                        row.negative_offset = neg.offset;
                                        row.negative_bucket_counts = neg.bucket_counts;
                                    }

                                    let e = payloads
                                        .entry(RequestType::MetricsExponentialHistogram)
                                        .or_insert(ClickhousePayloadBuilder::new(
                                            self.compression.clone(),
                                        ));

                                    e.add_row(&row)?;
                                }
                            }
                            Data::Summary(s) => {
                                for dp in s.data_points {
                                    let attrs = cvattr::convert(&dp.attributes);

                                    meta.attributes = self.transform_attrs(&attrs);
                                    meta.start_time_unix = dp.start_time_unix_nano;
                                    meta.time_unix = dp.time_unix_nano;

                                    let row = MetricsSummaryRow {
                                        meta: &meta,

                                        count: dp.count,
                                        sum: dp.sum,
                                        value_at_quantiles_quantile: dp
                                            .quantile_values
                                            .iter()
                                            .map(|q| q.quantile)
                                            .collect(),
                                        value_at_quantiles_value: dp
                                            .quantile_values
                                            .iter()
                                            .map(|q| q.value)
                                            .collect(),
                                        flags: dp.flags,
                                    };

                                    let e = payloads.entry(RequestType::MetricsSummary).or_insert(
                                        ClickhousePayloadBuilder::new(self.compression.clone()),
                                    );

                                    e.add_row(&row)?;
                                }
                            }
                        }
                    }
                }
            }
        }

        payloads
            .into_iter()
            .filter_map(|(typ, builder)| match builder.is_empty() {
                true => None,
                false => match builder.finish() {
                    Ok(payload) => Some(Ok((typ, payload))),
                    Err(e) => Some(Err(e)),
                },
            })
            .collect()
    }
}

impl Transformer {
    fn parse_exemplars(&self, exemplars: &[Exemplar]) -> MetricsExemplars {
        MetricsExemplars {
            exemplars_filtered_attributes: exemplars
                .iter()
                .map(|e| {
                    let event_attrs = cvattr::convert(&e.filtered_attributes);
                    self.transform_attrs(&event_attrs)
                })
                .collect(),
            exemplars_time_unix: exemplars.iter().map(|e| e.time_unix_nano).collect(),
            exemplars_value: exemplars
                .iter()
                .map(|e| match e.value {
                    None => 0.0,
                    Some(value) => match value {
                        Value::AsDouble(f) => f,
                        Value::AsInt(i) => i as f64,
                    },
                })
                .collect(),
            exemplars_span_id: exemplars.iter().map(|e| hex::encode(&e.span_id)).collect(),
            exemplars_trace_id: exemplars.iter().map(|e| hex::encode(&e.trace_id)).collect(),
        }
    }
}

fn get_metric_value(dp: Option<DataPointValue>) -> f64 {
    match dp {
        None => 0.0,
        Some(value) => match value {
            DataPointValue::AsDouble(f) => f,
            DataPointValue::AsInt(i) => i as f64,
        },
    }
}
