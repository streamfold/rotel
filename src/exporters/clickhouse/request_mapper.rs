use crate::exporters::clickhouse::api_request::{ApiRequestBuilder, ConnectionConfig};
use crate::exporters::clickhouse::payload::ClickhousePayload;
use crate::exporters::clickhouse::schema::{
    get_log_row_col_keys, get_metrics_exp_histogram_row_col_keys, get_metrics_gauge_row_col_keys,
    get_metrics_histogram_row_col_keys, get_metrics_sum_row_col_keys,
    get_metrics_summary_row_col_keys, get_span_row_col_keys,
};
use http::Request;
use std::collections::HashMap;
use tower::BoxError;

#[derive(PartialEq, Eq, Hash, Clone)]
pub enum RequestType {
    Traces,
    Logs,
    MetricsSum,
    MetricsGauge,
    MetricsHistogram,
    MetricsExponentialHistogram,
    MetricsSummary,
}

#[derive(Clone)]
pub struct RequestMapper {
    mapper: HashMap<RequestType, ApiRequestBuilder>,
}

impl RequestMapper {
    pub(crate) fn new(config: &ConnectionConfig, table_prefix: String) -> Result<Self, BoxError> {
        let mut mapper = HashMap::new();

        mapper.insert(
            RequestType::Traces,
            new_api_req_builder(config, &table_prefix, "traces", get_span_row_col_keys())?,
        );
        mapper.insert(
            RequestType::Logs,
            new_api_req_builder(config, &table_prefix, "logs", get_log_row_col_keys())?,
        );
        mapper.insert(
            RequestType::MetricsSum,
            new_api_req_builder(
                config,
                &table_prefix,
                "metrics_sum",
                get_metrics_sum_row_col_keys(),
            )?,
        );
        mapper.insert(
            RequestType::MetricsGauge,
            new_api_req_builder(
                config,
                &table_prefix,
                "metrics_gauge",
                get_metrics_gauge_row_col_keys(),
            )?,
        );
        mapper.insert(
            RequestType::MetricsHistogram,
            new_api_req_builder(
                config,
                &table_prefix,
                "metrics_histogram",
                get_metrics_histogram_row_col_keys(),
            )?,
        );
        mapper.insert(
            RequestType::MetricsExponentialHistogram,
            new_api_req_builder(
                config,
                &table_prefix,
                "metrics_exponential_histogram",
                get_metrics_exp_histogram_row_col_keys(),
            )?,
        );
        mapper.insert(
            RequestType::MetricsSummary,
            new_api_req_builder(
                config,
                &table_prefix,
                "metrics_summary",
                get_metrics_summary_row_col_keys(),
            )?,
        );

        Ok(Self { mapper })
    }

    pub(crate) fn build(
        &self,
        req_type: RequestType,
        payload: ClickhousePayload,
    ) -> Result<Request<ClickhousePayload>, BoxError> {
        let req_builder = self
            .mapper
            .get(&req_type)
            .expect("unknown request type in mapper");

        req_builder.build(payload)
    }
}

fn new_api_req_builder(
    config: &ConnectionConfig,
    table_prefix: &String,
    table_name: &str,
    col_keys: String,
) -> Result<ApiRequestBuilder, BoxError> {
    let query = build_insert_sql(get_table_name(table_prefix, table_name), col_keys);

    ApiRequestBuilder::new(config, query)
}

fn build_insert_sql(table: String, cols: String) -> String {
    format!("INSERT INTO {} ({}) FORMAT RowBinary", table, cols,)
}

fn get_table_name(table_prefix: &String, table: &str) -> String {
    format!("{}_{}", table_prefix, table)
}
