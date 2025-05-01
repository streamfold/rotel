use crate::exporters::clickhouse::Compression;
use crate::exporters::clickhouse::payload::ClickhousePayload;
use crate::exporters::http::request::{BaseRequestBuilder, RequestUri};
use http::{HeaderMap, HeaderName, Request};
use tower::BoxError;
use url::Url;

#[derive(Clone)]
pub struct ApiRequestBuilder {
    base: BaseRequestBuilder<ClickhousePayload>,
}

impl ApiRequestBuilder {
    pub fn new(
        endpoint: String,
        database: String,
        query: String,
        compression: Compression,
        auth_user: Option<String>,
        auth_password: Option<String>,
        async_insert: bool,
    ) -> Result<Self, BoxError> {
        let full_url = construct_full_url(endpoint);
        let mut uri = Url::parse(full_url.as_str())?;

        {
            let mut pairs = uri.query_pairs_mut();
            pairs.clear();

            pairs.append_pair("database", database.as_str());
            pairs.append_pair("query", query.as_str());
            if compression == Compression::Lz4 {
                pairs.append_pair("decompress", "1");
            }
            if async_insert {
                pairs.append_pair("async_insert", "1");
            }
        }

        let mut headers = HeaderMap::new();

        if let Some(user) = auth_user {
            headers.insert(
                HeaderName::from_static("x-clickhouse-user"),
                user.clone().parse()?,
            );
        }
        if let Some(password) = auth_password {
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

fn construct_full_url(endpoint: String) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return endpoint;
    }
    format!("http://{}/", endpoint)
}
