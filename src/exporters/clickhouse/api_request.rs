use crate::exporters::clickhouse::Compression;
use crate::exporters::clickhouse::payload::ClickhousePayload;
use crate::exporters::http::request::{BaseRequestBuilder, RequestUri};
use http::{HeaderMap, HeaderName, Request};
use tower::BoxError;
use url::Url;

pub struct ConnectionConfig {
    pub(crate) endpoint: String,
    pub(crate) database: String,
    pub(crate) compression: Compression,
    pub(crate) auth_user: Option<String>,
    pub(crate) auth_password: Option<String>,
    pub(crate) async_insert: bool,
    pub(crate) use_json: bool,
    pub(crate) use_json_underscore: bool,
}

#[derive(Clone)]
pub struct ApiRequestBuilder {
    base: BaseRequestBuilder<ClickhousePayload>,
}

impl ApiRequestBuilder {
    pub fn new(config: &ConnectionConfig, query: String) -> Result<Self, BoxError> {
        let full_url = construct_full_url(&config.endpoint);
        let mut uri = Url::parse(full_url.as_str())?;

        {
            let mut pairs = uri.query_pairs_mut();
            pairs.clear();

            pairs.append_pair("database", config.database.as_str());
            pairs.append_pair("query", query.as_str());
            if config.compression == Compression::Lz4 {
                pairs.append_pair("decompress", "1");
            }
            if config.async_insert {
                pairs.append_pair("async_insert", "1");
            }
            if config.use_json {
                pairs.append_pair("allow_experimental_json_type", "1");
                // This interprets Strings as JSON columns
                pairs.append_pair("input_format_binary_read_json_as_string", "1");
            }
        }

        let mut headers = HeaderMap::new();

        if let Some(user) = &config.auth_user {
            headers.insert(
                HeaderName::from_static("x-clickhouse-user"),
                user.clone().parse()?,
            );
        }
        if let Some(password) = &config.auth_password {
            headers.insert(
                HeaderName::from_static("x-clickhouse-key"),
                password.clone().parse()?,
            );
        }

        let base = BaseRequestBuilder::new(Some(RequestUri::Post(uri)), headers);

        Ok(Self { base })
    }

    pub fn build(
        &self,
        payload: ClickhousePayload,
    ) -> Result<Request<ClickhousePayload>, BoxError> {
        self.base
            .builder()
            .body(payload)?
            .build()
            .map_err(|e| format!("failed to build request: {:?}", e).into())
    }
}

fn construct_full_url(endpoint: &String) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return endpoint.clone();
    }
    format!("http://{}/", endpoint)
}
