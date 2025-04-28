use bytes::Bytes;
use http::{Method, Request};
use http_body_util::Full;
use tower::BoxError;
use http::request::Request as HttpRequest;
use url::Url;
use crate::exporters::clickhouse::payload::ClickhousePayload;

#[derive(Clone)]
pub struct ApiRequestBuilder {
    pub endpoint: String,
    pub query: String,
}

impl ApiRequestBuilder {
    pub fn new(endpoint: String, query: String) -> Self {
        Self {
            endpoint,
            query,
        }
    }

    pub fn build(&self, payload: ClickhousePayload) -> Result<Request<Full<Bytes>>, BoxError> {
        let full_url = format!("http://{}/", self.endpoint);
        
        let mut uri = Url::parse(full_url.as_str())?;

        let mut pairs = uri.query_pairs_mut();
        pairs.clear();

        pairs.append_pair("database", "otel");
        pairs.append_pair("query", self.query.as_str());
        pairs.append_pair("compress", "0");
        drop(pairs);

        // todo: if we could return a chunked body, then we could return these as chunks
        let body = payload.rows.concat();
        
        let req = HttpRequest::builder()
            .uri(uri.to_string())
            .method(Method::POST)
            .body(Full::from(Bytes::from(body)))?;

        Ok(req)
    }
}