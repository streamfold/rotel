use crate::exporters::clickhouse::payload::{ClickhousePayload, ClickhousePayloadBuilder};
use crate::exporters::clickhouse::request_builder::TransformPayload;
use crate::exporters::clickhouse::request_mapper::RequestType;
use crate::exporters::clickhouse::schema::{
    MetricsExemplars, MetricsExpHistogramRow, MetricsGaugeRow, MetricsHistogramRow, MetricsMeta,
    MetricsSumRow, MetricsSummaryRow,
};
use crate::exporters::clickhouse::transformer::{Transformer, find_str_attribute_kv};
use crate::topology::payload::{Message, MessageMetadata};
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
        input: Vec<Message<ResourceMetrics>>,
    ) -> (
        Result<Vec<(RequestType, ClickhousePayload)>, BoxError>,
        Option<Vec<MessageMetadata>>,
    ) {
        let mut payloads = HashMap::new();
        let mut all_metadata = Vec::new();

        for message in input {
            if let Some(metadata) = message.metadata {
                all_metadata.push(metadata);
            }
            for rm in message.payload {
                let res_attrs = rm.resource.unwrap_or_default().attributes;
                let service_name = find_str_attribute_kv(SERVICE_NAME, &res_attrs);
                let res_attrs_field = self.transform_attrs_kv(&res_attrs);

                for sm in rm.scope_metrics {
                    let (scope_name, scope_version, scope_attrs, dropped_attr_count) =
                        match sm.scope {
                            Some(scope) => (
                                scope.name,
                                scope.version,
                                scope.attributes,
                                scope.dropped_attributes_count,
                            ),
                            None => (String::new(), String::new(), Vec::new(), 0),
                        };

                    let scope_attrs = self.transform_attrs_kv(&scope_attrs);

                    for metric in sm.metrics {
                        if let Some(data) = metric.data {
                            let meta = MetricsMeta {
                                resource_attributes: &res_attrs_field,
                                resource_schema_url: &rm.schema_url,
                                scope_name: &scope_name,
                                scope_version: &scope_version,
                                scope_attributes: &scope_attrs,
                                scope_dropped_attr_count: dropped_attr_count,
                                scope_schema_url: &sm.schema_url,
                                service_name: &service_name,
                                metric_name: &metric.name,
                                metric_description: &metric.description,
                                metric_unit: &metric.unit,
                            };

                            match data {
                                Data::Sum(s) => {
                                    for dp in s.data_points {
                                        let attrs = self.transform_attrs_kv(&dp.attributes);

                                        let row = MetricsSumRow {
                                            meta: &meta,

                                            attributes: &attrs,
                                            start_time_unix: dp.start_time_unix_nano,
                                            time_unix: dp.time_unix_nano,
                                            value: get_metric_value(dp.value),
                                            flags: dp.flags,
                                            aggregation_temporality: s.aggregation_temporality,
                                            is_monotonic: s.is_monotonic,
                                            exemplars: self.parse_exemplars(&dp.exemplars),
                                        };

                                        let e = payloads
                                            .entry(RequestType::MetricsSum)
                                            .or_insert_with(|| {
                                                ClickhousePayloadBuilder::new(
                                                    self.compression.clone(),
                                                )
                                            });

                                        match e.add_row(&row) {
                                            Ok(_) => {}
                                            Err(err) => return (Err(err), None),
                                        }
                                    }
                                }
                                Data::Gauge(g) => {
                                    for dp in g.data_points {
                                        let attrs = self.transform_attrs_kv(&dp.attributes);

                                        let row = MetricsGaugeRow {
                                            meta: &meta,

                                            attributes: &attrs,
                                            start_time_unix: dp.start_time_unix_nano,
                                            time_unix: dp.time_unix_nano,
                                            value: get_metric_value(dp.value),
                                            flags: dp.flags,
                                            exemplars: self.parse_exemplars(&dp.exemplars),
                                        };

                                        let e = payloads
                                            .entry(RequestType::MetricsGauge)
                                            .or_insert_with(|| {
                                                ClickhousePayloadBuilder::new(
                                                    self.compression.clone(),
                                                )
                                            });

                                        match e.add_row(&row) {
                                            Ok(_) => {}
                                            Err(err) => return (Err(err), None),
                                        }
                                    }
                                }
                                Data::Histogram(h) => {
                                    for dp in h.data_points {
                                        let attrs = self.transform_attrs_kv(&dp.attributes);

                                        let row = MetricsHistogramRow {
                                            meta: &meta,

                                            attributes: &attrs,
                                            start_time_unix: dp.start_time_unix_nano,
                                            time_unix: dp.time_unix_nano,
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

                                        let e = payloads
                                            .entry(RequestType::MetricsHistogram)
                                            .or_insert_with(|| {
                                                ClickhousePayloadBuilder::new(
                                                    self.compression.clone(),
                                                )
                                            });

                                        match e.add_row(&row) {
                                            Ok(_) => {}
                                            Err(err) => return (Err(err), None),
                                        }
                                    }
                                }
                                Data::ExponentialHistogram(e) => {
                                    for dp in e.data_points {
                                        let attrs = self.transform_attrs_kv(&dp.attributes);

                                        let mut row = MetricsExpHistogramRow {
                                            meta: &meta,

                                            attributes: &attrs,
                                            start_time_unix: dp.start_time_unix_nano,
                                            time_unix: dp.time_unix_nano,
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
                                            .or_insert_with(|| {
                                                ClickhousePayloadBuilder::new(
                                                    self.compression.clone(),
                                                )
                                            });

                                        match e.add_row(&row) {
                                            Ok(_) => {}
                                            Err(err) => return (Err(err), None),
                                        }
                                    }
                                }
                                Data::Summary(s) => {
                                    for dp in s.data_points {
                                        let attrs = self.transform_attrs_kv(&dp.attributes);

                                        let row = MetricsSummaryRow {
                                            meta: &meta,

                                            attributes: &attrs,
                                            start_time_unix: dp.start_time_unix_nano,
                                            time_unix: dp.time_unix_nano,
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

                                        let e = payloads
                                            .entry(RequestType::MetricsSummary)
                                            .or_insert_with(|| {
                                                ClickhousePayloadBuilder::new(
                                                    self.compression.clone(),
                                                )
                                            });

                                        match e.add_row(&row) {
                                            Ok(_) => {}
                                            Err(err) => return (Err(err), None),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let metadata = if all_metadata.is_empty() {
            None
        } else {
            Some(all_metadata)
        };

        let result = payloads
            .into_iter()
            .filter_map(|(typ, builder)| match builder.is_empty() {
                true => None,
                false => match builder.finish_with_metadata(metadata.clone()) {
                    Ok(payload) => Some(Ok((typ, payload))),
                    Err(e) => Some(Err(e)),
                },
            })
            .collect();

        (result, None)
    }
}

impl Transformer {
    fn parse_exemplars(&self, exemplars: &[Exemplar]) -> MetricsExemplars {
        MetricsExemplars {
            exemplars_filtered_attributes: exemplars
                .iter()
                .map(|e| self.transform_attrs_kv_owned(&e.filtered_attributes))
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
