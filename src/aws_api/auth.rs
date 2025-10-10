use crate::aws_api::creds::AwsCreds;
use crate::aws_api::error::Error;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use http::header::{AUTHORIZATION, HOST};
use http::{HeaderMap, HeaderValue, Method, Request, Uri};
use http_body_util::Full;
use sha2::Digest;
use sha2::Sha256;
use std::str;

pub trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Default, Clone)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct AwsRequestSigner<C> {
    service: String,
    region: String,
    clock: C,
}

impl<C> AwsRequestSigner<C>
where
    C: Clock,
{
    pub fn new(service: &str, region: &str, clock: C) -> Self {
        Self {
            service: service.to_string(),
            region: region.to_string(),
            clock,
        }
    }

    pub fn sign(
        &self,
        uri: Uri,
        method: Method,
        mut headers_mut: HeaderMap,
        payload: Bytes,
        creds: &AwsCreds,
    ) -> Result<Request<Full<Bytes>>, Error> {
        let now = self.clock.now();

        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let host = uri.host().unwrap();

        // Add host header if it doesn't exist
        if !headers_mut.contains_key(HOST) {
            let port = uri.port();
            let host_value = if let Some(port) = port {
                format!("{}:{}", host, port)
            } else {
                host.to_string()
            };

            headers_mut.insert(
                HOST,
                HeaderValue::from_str(&host_value)
                    .map_err(|_| Error::SignatureError("Invalid host header".to_string()))?,
            );
        }

        // Add session token if provided
        if let Some(token) = creds.session_token() {
            if !token.is_empty() {
                headers_mut.insert(
                    "X-Amz-Security-Token",
                    HeaderValue::from_str(token.as_str())
                        .map_err(|_| Error::SignatureError("Invalid session token".to_string()))?,
                );
            }
        }

        // Add date header
        headers_mut.insert(
            "X-Amz-Date",
            HeaderValue::from_str(&amz_date)
                .map_err(|_| Error::SignatureError("Invalid date".to_string()))?,
        );

        // Step 1: Create canonical request
        let canonical_uri = uri.path();

        let query = uri.path_and_query().unwrap().query();
        let canonical_querystring = match query {
            None => "".to_string(),
            Some(q) => {
                // Collect and sort query parameters
                let mut query_params: Vec<(String, String)> = q
                    .split("&")
                    .map(|s| {
                        let splits: Vec<&str> = s.splitn(2, "=").collect();
                        if splits.len() > 1 {
                            (splits[0], splits[1])
                        } else {
                            (splits[0], "")
                        }
                    })
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                query_params.sort();

                let canonical_querystring = query_params
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<String>>()
                    .join("&");

                canonical_querystring
            }
        };

        // Get and sort headers
        let mut canonical_headers = String::new();
        let mut signed_headers = Vec::new();

        let mut headers: Vec<(String, String)> = headers_mut
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_lowercase(),
                    value.to_str().unwrap_or_default().trim().to_string(),
                )
            })
            .collect();
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        for (name, value) in &headers {
            canonical_headers.push_str(&format!("{}:{}\n", name, value));
            signed_headers.push(name.clone());
        }

        let signed_headers_str = signed_headers.join(";");

        // Calculate payload hash
        let payload_hash = hex::encode(Sha256::digest(&payload));

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers_str,
            payload_hash
        );

        // Step 2: Create the string to sign
        let algorithm = "AWS4-HMAC-SHA256";
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            date_stamp, self.region, self.service
        );
        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            algorithm, amz_date, credential_scope, canonical_request_hash
        );

        // Step 3: Calculate the signature
        let signature = self.calculate_signature(&date_stamp, &string_to_sign, &creds)?;

        // Step 4: Add signature to request header
        let authorization_header = format!(
            "{} Credential={}/{}, SignedHeaders={}, Signature={}",
            algorithm,
            creds.access_key_id(),
            credential_scope,
            signed_headers_str,
            signature
        );

        headers_mut.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&authorization_header)
                .map_err(|_| Error::SignatureError("Invalid authorization header".to_string()))?,
        );

        let mut req_builder = Request::builder().uri(uri).method(method);

        let builder_headers = req_builder.headers_mut().unwrap();
        for (k, v) in headers_mut.iter() {
            builder_headers.insert(k, v.clone());
        }

        req_builder
            .body(Full::from(payload))
            .map_err(Error::RequestBuildError)
    }

    fn calculate_signature(
        &self,
        date_stamp: &str,
        string_to_sign: &str,
        creds: &AwsCreds,
    ) -> Result<String, Error> {
        // Create signing key
        let k_secret = format!("AWS4{}", creds.secret_access_key());

        let k_date = self.sign_hmac(k_secret.as_bytes(), date_stamp.as_bytes())?;
        let k_region = self.sign_hmac(&k_date, self.region.as_bytes())?;
        let k_service = self.sign_hmac(&k_region, self.service.as_bytes())?;
        let k_signing = self.sign_hmac(&k_service, b"aws4_request")?;

        // Sign the string to sign with the signing key
        let signature = self.sign_hmac(&k_signing, string_to_sign.as_bytes())?;
        Ok(hex::encode(signature))
    }

    fn sign_hmac(&self, key: &[u8], message: &[u8]) -> Result<Vec<u8>, Error> {
        let mut mac = HmacSha256::new_from_slice(key)
            .map_err(|_| Error::SignatureError("Invalid HMAC key".to_string()))?;
        mac.update(message);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use http_body_util::BodyExt;
    use std::collections::HashMap;

    struct MockClock(DateTime<Utc>);
    impl Clock for MockClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    // Helper function to extract headers from a signed request
    fn extract_headers(request: &Request<Full<Bytes>>) -> HashMap<String, String> {
        request
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn test_sign_basic_request() {
        // Arrange
        let signer = AwsRequestSigner::new("s3", "us-east-1", SystemClock {});

        let uri = "https://s3.amazonaws.com/test-bucket/test-object"
            .parse::<Uri>()
            .unwrap();
        let method = Method::GET;
        let headers = HeaderMap::new();

        // Act
        let signed_request = signer
            .sign(uri, method, headers, Bytes::new(), &sample_creds())
            .unwrap();

        // Assert
        let headers = extract_headers(&signed_request);
        assert!(headers.contains_key(&HOST.to_string()));
        assert!(headers.contains_key(&"x-amz-date".to_string()));
        assert!(headers.contains_key(&AUTHORIZATION.to_string()));

        let auth_header = &headers[&AUTHORIZATION.to_string()];
        assert!(auth_header.starts_with("AWS4-HMAC-SHA256 Credential="));
        assert!(auth_header.contains("us-east-1/s3/aws4_request"));
    }

    #[test]
    fn test_sign_with_query_parameters() {
        // Arrange
        let signer = AwsRequestSigner::new("s3", "us-east-1", SystemClock {});

        let uri = "https://s3.amazonaws.com/test-bucket/test-object?prefix=test&delimiter=/"
            .parse::<Uri>()
            .unwrap();
        let method = Method::GET;
        let headers = HeaderMap::new();

        // Act
        let signed_request = signer
            .sign(uri, method, headers, Bytes::new(), &sample_creds())
            .unwrap();

        // Assert
        let headers = extract_headers(&signed_request);
        let auth_header = &headers[&AUTHORIZATION.to_string()];
        // The canonical query string should be included in the signature
        assert!(auth_header.contains("SignedHeaders="));
    }

    #[test]
    fn test_sign_with_session_token() {
        // Arrange
        let signer = AwsRequestSigner::new("s3", "us-east-1", SystemClock {});

        let uri = "https://s3.amazonaws.com/test-bucket/test-object"
            .parse::<Uri>()
            .unwrap();
        let method = Method::GET;
        let headers = HeaderMap::new();

        // Act
        let signed_request = signer
            .sign(
                uri,
                method,
                headers,
                Bytes::new(),
                &sample_creds_with_session("SESSION_TOKEN_EXAMPLE"),
            )
            .unwrap();

        // Assert
        let headers = extract_headers(&signed_request);
        assert_eq!(
            headers.get("x-amz-security-token").unwrap(),
            "SESSION_TOKEN_EXAMPLE"
        );
    }

    #[tokio::test]
    async fn test_sign_with_existing_headers() {
        // Arrange
        let signer = AwsRequestSigner::new("s3", "us-east-1", SystemClock {});

        let uri = "https://s3.amazonaws.com/test-bucket/test-object"
            .parse::<Uri>()
            .unwrap();
        let method = Method::POST;

        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());

        let payload = b"Hello, world!".to_vec();

        // Act
        let signed_request = signer
            .sign(uri, method, headers, Bytes::from(payload), &sample_creds())
            .unwrap();

        // Assert
        let headers = extract_headers(&signed_request);
        assert_eq!(headers.get("content-type").unwrap(), "application/json");
        let auth_header = &headers[&AUTHORIZATION.to_string()];
        assert!(auth_header.contains("content-type"));
        let body = signed_request.into_body().collect().await.unwrap();

        assert_eq!(b"Hello, world!", body.to_bytes().as_ref());
    }

    #[test]
    fn test_sign_with_existing_host_header() {
        // Arrange
        let signer = AwsRequestSigner::new("s3", "us-east-1", SystemClock {});

        let uri = "https://s3.amazonaws.com/test-bucket/test-object"
            .parse::<Uri>()
            .unwrap();
        let method = Method::GET;

        let mut headers = HeaderMap::new();
        headers.insert(HOST, "custom-host.com".parse().unwrap());

        // Act
        let signed_request = signer
            .sign(uri, method, headers, Bytes::new(), &sample_creds())
            .unwrap();

        // Assert
        let headers = extract_headers(&signed_request);
        assert_eq!(headers.get(&HOST.to_string()).unwrap(), "custom-host.com");
    }

    #[test]
    fn test_with_fixed_time_full_authorization_header() {
        // Arrange - use a fixed time to generate a deterministic signature
        let fixed_time = Utc.with_ymd_and_hms(2023, 4, 1, 12, 0, 0).unwrap();

        let signer = AwsRequestSigner::new("s3", "us-east-1", MockClock(fixed_time));

        let uri = "https://s3.amazonaws.com/test-bucket/test-key"
            .parse::<Uri>()
            .unwrap();
        let method = Method::GET;

        // Create a fixed set of headers for deterministic results
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "s3.amazonaws.com".parse().unwrap());
        headers.insert("Content-Type", "application/json".parse().unwrap());

        let payload = b"test-payload".to_vec();

        // Act
        let signed_request = signer
            .sign(uri, method, headers, Bytes::from(payload), &sample_creds())
            .unwrap();

        // Assert
        let headers = extract_headers(&signed_request);

        // Check the x-amz-date header matches our fixed time
        assert_eq!(headers.get("x-amz-date").unwrap(), "20230401T120000Z");

        // Verify the full Authorization header
        // This expected value was pre-calculated based on the fixed inputs
        let expected_auth_header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230401/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=c180093fab25fabef7f4a7bf3235c704e5ba9cba022ba23045656577472c65b0";
        let actual_auth_header = &headers[&AUTHORIZATION.to_string()];

        assert_eq!(actual_auth_header, expected_auth_header);
    }

    #[test]
    fn test_uri_with_port() {
        // Arrange
        let signer = AwsRequestSigner::new("s3", "us-east-1", SystemClock {});

        let uri = "https://s3.amazonaws.com:8443/test-bucket/test-object"
            .parse::<Uri>()
            .unwrap();
        let method = Method::GET;
        let headers = HeaderMap::new();

        // Act
        let signed_request = signer
            .sign(uri, method, headers, Bytes::new(), &sample_creds())
            .unwrap();

        // Assert
        let headers = extract_headers(&signed_request);
        assert_eq!(
            headers.get(&HOST.to_string()).unwrap(),
            "s3.amazonaws.com:8443"
        );
    }

    fn sample_creds() -> AwsCreds {
        AwsCreds::new(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            None,
        )
    }

    fn sample_creds_with_session(session: &str) -> AwsCreds {
        AwsCreds::new(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            Some(session.to_string()),
        )
    }
}
