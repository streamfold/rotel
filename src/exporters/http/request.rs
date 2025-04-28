// SPDX-License-Identifier: Apache-2.0

use http::header::USER_AGENT;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request};
use std::error::Error;
use std::marker::PhantomData;
use tower::BoxError;

#[derive(Clone)]
pub enum RequestUri {
    Post(url::Url),
}

#[derive(Clone)]
pub struct BaseRequestBuilder<ReqBody: Clone> {
    pub uri: Option<RequestUri>,
    pub header_map: HeaderMap,
    _phantom: PhantomData<ReqBody>,
}

impl<ReqBody: Clone> BaseRequestBuilder<ReqBody> {
    pub fn new(uri: Option<RequestUri>, header_map: HeaderMap) -> Self {
        let mut base = Self {
            uri,
            header_map,
            _phantom: PhantomData::default(),
        };

        // add base headers here
        base.header_map.insert(
            USER_AGENT,
            HeaderValue::from_static("Rotel Rust/1.84.1 hyper/1.52.0"),
        );

        base
    }

    pub fn builder(&self) -> RequestBuilder<ReqBody> {
        RequestBuilder {
            // could we reduce clones here?
            uri: self.uri.clone(),
            header_map: self.header_map.clone(),
            body: None,
        }
    }
}

pub struct RequestBuilder<ReqBody> {
    uri: Option<RequestUri>,
    header_map: HeaderMap,
    body: Option<ReqBody>,
}

impl<ReqBody> RequestBuilder<ReqBody> {
    #[allow(dead_code)]
    pub fn post<T: TryInto<url::Url>>(mut self, uri: T) -> Result<Self, T::Error> {
        let uri = uri.try_into()?;

        self.uri = Some(RequestUri::Post(uri));
        Ok(self)
    }

    #[allow(dead_code)]
    pub fn header<K: TryInto<HeaderName>, V: TryInto<HeaderValue>>(
        mut self,
        key: K,
        value: V,
    ) -> Result<Self, BoxError>
    where
        <K as TryInto<HeaderName>>::Error: Error + Send + Sync + 'static,
        <V as TryInto<HeaderValue>>::Error: Error + Send + Sync + 'static,
    {
        let key = key.try_into()?;
        let value = value.try_into()?;
        self.header_map.insert(key, value);
        Ok(self)
    }

    pub fn body<T: TryInto<ReqBody>>(mut self, body: T) -> Result<Self, T::Error> {
        let body = body.try_into()?;

        self.body = Some(body);
        Ok(self)
    }

    pub fn build(self) -> Result<Request<ReqBody>, BoxError> {
        let uri = match self.uri {
            None => return Err("URI is not set".into()),
            Some(RequestUri::Post(u)) => u,
        };

        // if we example request types, body may not always be required
        if self.body.is_none() {
            return Err("Body is not set".into());
        }

        let mut builder = hyper::Request::builder()
            .method(Method::POST)
            .uri(uri.to_string());

        let headers = builder.headers_mut().unwrap();
        for (k, v) in &self.header_map {
            headers.insert(k, v.clone());
        }

        let req = match builder.body(self.body.unwrap()) {
            Ok(req) => req,
            Err(e) => return Err(format!("unable to build request: {}", e).into()),
        };

        Ok(req)
    }
}
