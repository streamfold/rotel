use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::exemplar::Value as ExemplarValue;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, Metric,
    NumberDataPoint, SummaryDataPoint,
};
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use std::fmt::{Display, Formatter, Write};

#[derive(Default)]
pub struct DebugBuffer {
    buf: String,
}

// Macro for logging entries
macro_rules! log_entry {
    ($self:expr, $format:expr $(, $args:expr)*) => {
        writeln!($self.buf, "{}", format!($format $(, $args)*)).unwrap()
    };
}

impl DebugBuffer {
    pub fn new() -> Self {
        DebugBuffer { buf: String::new() }
    }

    pub(crate) fn log_entry(&mut self, line: String) {
        log_entry!(self, "{}", line);
    }

    pub(crate) fn log_attr(&mut self, attr: &str, value: &dyn std::fmt::Display) {
        log_entry!(self, "    {:<15}: {}", attr, value);
    }

    pub(crate) fn log_attributes(&mut self, header: &str, attributes: &[KeyValue]) {
        if attributes.is_empty() {
            return;
        }

        log_entry!(self, "{}:", header);
        let mut attr_prefix = "     ->".to_string();

        let header_parts: Vec<&str> = header.split("->").collect();
        if header_parts.len() > 1 {
            attr_prefix = format!("{}{}", header_parts[0], attr_prefix);
        }

        for attr in attributes {
            log_entry!(
                self,
                "{} {}: {}",
                attr_prefix,
                attr.key,
                value_to_string(attr.value.as_ref())
            );
        }
    }

    pub(crate) fn log_instrumentation_scope(
        &mut self,
        il: &opentelemetry_proto::tonic::common::v1::InstrumentationScope,
    ) {
        log_entry!(self, "InstrumentationScope {} {}", il.name, il.version);
        self.log_attributes("InstrumentationScope attributes", &il.attributes);
    }

    pub(crate) fn log_metric_descriptor(&mut self, md: &Metric) {
        log_entry!(self, "Descriptor:");
        log_entry!(self, "     -> Name: {}", md.name);
        log_entry!(self, "     -> Description: {}", md.description);
        log_entry!(self, "     -> Unit: {}", md.unit);
        log_entry!(
            self,
            "     -> DataType: {}",
            metric_data_case_to_string(&md.data)
        );
    }

    pub(crate) fn log_metric_data_points(&mut self, m: &Metric) {
        match m.data {
            Some(Data::Gauge(ref gauge)) => {
                self.log_number_data_points(&gauge.data_points);
            }
            Some(Data::Sum(ref sum)) => {
                log_entry!(self, "     -> IsMonotonic: {}", sum.is_monotonic);
                log_entry!(
                    self,
                    "     -> AggregationTemporality: {}",
                    aggregation_temporality_to_string(sum.aggregation_temporality)
                );
                self.log_number_data_points(&sum.data_points);
            }
            Some(Data::Histogram(ref histogram)) => {
                log_entry!(
                    self,
                    "     -> AggregationTemporality: {}",
                    aggregation_temporality_to_string(histogram.aggregation_temporality)
                );
                self.log_histogram_data_points(&histogram.data_points);
            }
            Some(Data::ExponentialHistogram(ref exp_histogram)) => {
                log_entry!(
                    self,
                    "     -> AggregationTemporality: {}",
                    aggregation_temporality_to_string(exp_histogram.aggregation_temporality)
                );
                self.log_exponential_histogram_data_points(&exp_histogram.data_points);
            }
            Some(Data::Summary(ref summary)) => {
                self.log_double_summary_data_points(&summary.data_points);
            }
            None => { /* MetricTypeEmpty or unhandled case */ }
        }
    }

    fn log_number_data_points(&mut self, ps: &[NumberDataPoint]) {
        for (i, p) in ps.iter().enumerate() {
            log_entry!(self, "NumberDataPoints #{}", i);
            self.log_data_point_attributes(&p.attributes);

            log_entry!(self, "StartTimestamp: {}", p.start_time_unix_nano);
            log_entry!(self, "Timestamp: {}", p.time_unix_nano);
            match p.value {
                Some(Value::AsInt(val)) => log_entry!(self, "Value: {}", val),
                Some(Value::AsDouble(val)) => log_entry!(self, "Value: {}", val),
                None => {} // Handle cases where value_data is not set
            }

            self.log_exemplars("Exemplars", &p.exemplars);
        }
    }

    fn log_histogram_data_points(&mut self, ps: &[HistogramDataPoint]) {
        for (i, p) in ps.iter().enumerate() {
            log_entry!(self, "HistogramDataPoints #{}", i);
            self.log_data_point_attributes(&p.attributes);

            log_entry!(self, "StartTimestamp: {}", p.start_time_unix_nano);
            log_entry!(self, "Timestamp: {}", p.time_unix_nano);
            log_entry!(self, "Count: {}", p.count);

            if let Some(sum) = p.sum {
                log_entry!(self, "Sum: {}", sum);
            }

            if let Some(min) = p.min {
                log_entry!(self, "Min: {}", min);
            }

            if let Some(max) = p.max {
                log_entry!(self, "Max: {}", max);
            }

            for (j, bound) in p.explicit_bounds.iter().enumerate() {
                log_entry!(self, "ExplicitBounds #{}: {}", j, bound);
            }

            for (j, bucket_count) in p.bucket_counts.iter().enumerate() {
                log_entry!(self, "Buckets #{}, Count: {}", j, bucket_count);
            }

            self.log_exemplars("Exemplars", &p.exemplars);
        }
    }

    fn log_exponential_histogram_data_points(&mut self, ps: &[ExponentialHistogramDataPoint]) {
        for (i, p) in ps.iter().enumerate() {
            log_entry!(self, "ExponentialHistogramDataPoints #{}", i);
            self.log_data_point_attributes(&p.attributes);

            log_entry!(self, "StartTimestamp: {}", p.start_time_unix_nano);
            log_entry!(self, "Timestamp: {}", p.time_unix_nano);
            log_entry!(self, "Count: {}", p.count);

            if let Some(sum) = p.sum {
                log_entry!(self, "Sum: {}", sum);
            }

            if let Some(min) = p.min {
                log_entry!(self, "Min: {}", min);
            }

            if let Some(max) = p.max {
                log_entry!(self, "Max: {}", max);
            }

            let factor = (2.0f64).ln() * (-p.scale as f64).exp2();

            if let Some(ref negative_data) = p.negative {
                let neg_b = &negative_data.bucket_counts;
                for j in 0..neg_b.len() {
                    let pos = neg_b.len() - j - 1;
                    let index = negative_data.offset as f64 + pos as f64;
                    let lower = (index * factor).exp();
                    let upper = ((index + 1.0) * factor).exp();
                    log_entry!(
                        self,
                        "Bucket [{}, {}), Count: {}",
                        -upper,
                        -lower,
                        neg_b[pos]
                    );
                }
            }

            if p.zero_count != 0 {
                log_entry!(self, "Bucket [0, 0], Count: {}", p.zero_count);
            }

            if let Some(ref positive_data) = p.positive {
                let pos_b = &positive_data.bucket_counts;
                for (pos, bucket) in pos_b.iter().enumerate() {
                    let index = positive_data.offset as f64 + pos as f64;
                    let lower = (index * factor).exp();
                    let upper = ((index + 1.0) * factor).exp();
                    log_entry!(self, "Bucket ({}, {}], Count: {}", lower, upper, bucket);
                }
            }

            self.log_exemplars("Exemplars", &p.exemplars);
        }
    }

    fn log_double_summary_data_points(&mut self, ps: &[SummaryDataPoint]) {
        for (i, p) in ps.iter().enumerate() {
            log_entry!(self, "SummaryDataPoints #{}", i);
            self.log_data_point_attributes(&p.attributes);

            log_entry!(self, "StartTimestamp: {}", p.start_time_unix_nano);
            log_entry!(self, "Timestamp: {}", p.time_unix_nano);
            log_entry!(self, "Count: {}", p.count);
            log_entry!(self, "Sum: {}", p.sum);

            for (j, quantile) in p.quantile_values.iter().enumerate() {
                log_entry!(
                    self,
                    "QuantileValue #{}: Quantile {}, Value {}",
                    j,
                    quantile.quantile,
                    quantile.value
                );
            }
        }
    }

    fn log_data_point_attributes(&mut self, attributes: &[KeyValue]) {
        self.log_attributes("Data point attributes", attributes);
    }

    pub(crate) fn log_events(&mut self, description: &str, events: &[Event]) {
        if events.is_empty() {
            return;
        }

        log_entry!(self, "{}:", description);
        for (i, e) in events.iter().enumerate() {
            log_entry!(self, "SpanEvent #{}", i);
            log_entry!(self, "     -> Name: {}", e.name);
            log_entry!(self, "     -> Timestamp: {}", e.time_unix_nano);
            log_entry!(
                self,
                "     -> DroppedAttributesCount: {}",
                e.dropped_attributes_count
            );
            self.log_attributes("     -> Attributes:", &e.attributes);
        }
    }

    pub(crate) fn log_links(&mut self, description: &str, links: &[Link]) {
        if links.is_empty() {
            return;
        }

        log_entry!(self, "{}:", description);

        for (i, l) in links.iter().enumerate() {
            log_entry!(self, "SpanLink #{}", i);
            log_entry!(self, "     -> Trace ID: {}", hex::encode(&l.trace_id));
            log_entry!(self, "     -> ID: {}", hex::encode(&l.span_id));
            log_entry!(self, "     -> TraceState: {}", l.trace_state);
            log_entry!(
                self,
                "     -> DroppedAttributesCount: {}",
                l.dropped_attributes_count
            );
            self.log_attributes("     -> Attributes:", &l.attributes);
        }
    }

    fn log_exemplars(&mut self, description: &str, se: &[Exemplar]) {
        if se.is_empty() {
            return;
        }

        log_entry!(self, "{}:", description);

        for (i, e) in se.iter().enumerate() {
            log_entry!(self, "Exemplar #{}", i);
            log_entry!(self, "     -> Trace ID: {}", hex::encode(&e.trace_id));
            log_entry!(self, "     -> Span ID: {}", hex::encode(&e.span_id));
            log_entry!(self, "     -> Timestamp: {}", e.time_unix_nano);
            match e.value {
                Some(ExemplarValue::AsInt(val)) => log_entry!(self, "     -> Value: {}", val),
                Some(ExemplarValue::AsDouble(val)) => log_entry!(self, "     -> Value: {}", val),
                None => {}
            }
            self.log_attributes("     -> FilteredAttributes", &e.filtered_attributes);
        }
    }
}

impl Display for DebugBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.buf)
    }
}

pub(crate) fn value_to_string(v: Option<&AnyValue>) -> String {
    if let Some(any_value) = v {
        match any_value.value {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(ref s)) => {
                format!("Str({})", s)
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)) => {
                format!("Bool({})", b)
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                format!("Int({})", i)
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
                format!("Double({})", d)
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(ref arr)) => {
                let elements: Vec<String> = arr
                    .values
                    .iter()
                    .map(|v| value_to_string(Some(v)))
                    .collect();
                format!("Slice({})", elements.join(", "))
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(
                ref kv_list,
            )) => {
                let elements: Vec<String> = kv_list
                    .values
                    .iter()
                    .map(|kv| format!("{}:{}", kv.key, value_to_string(kv.value.as_ref())))
                    .collect();
                format!("Map({})", elements.join(", "))
            }
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(ref b)) => {
                format!("Bytes({:?})", b)
            }
            None => "Empty".to_string(),
        }
    } else {
        "Empty".to_string()
    }
}

fn metric_data_case_to_string(data_case: &Option<Data>) -> String {
    match data_case {
        Some(Data::Gauge(_)) => "Gauge".to_string(),
        Some(Data::Sum(_)) => "Sum".to_string(),
        Some(Data::Histogram(_)) => "Histogram".to_string(),
        Some(Data::ExponentialHistogram(_)) => "ExponentialHistogram".to_string(),
        Some(Data::Summary(_)) => "Summary".to_string(),
        None => "Empty".to_string(),
    }
}

fn aggregation_temporality_to_string(temporality: i32) -> String {
    match AggregationTemporality::try_from(temporality) {
        Ok(AggregationTemporality::Unspecified) => {
            "AGGREGATION_TEMPORALITY_UNSPECIFIED".to_string()
        }
        Ok(AggregationTemporality::Delta) => "AGGREGATION_TEMPORALITY_DELTA".to_string(),
        Ok(AggregationTemporality::Cumulative) => "AGGREGATION_TEMPORALITY_CUMULATIVE".to_string(),
        Err(_) => format!("Unknown_Temporality({})", temporality),
    }
}
