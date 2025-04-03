use crate::lambda::constants;
use crate::lambda::types::{
    RegisterResponseBody, TelemetryAPISubscribe, TelemetryAPISubscribeBuffering,
    TelemetryAPISubscribeDestination,
};
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{Method, Request};
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use lambda_extension::NextEvent;
use std::net::SocketAddr;
use tower::BoxError;

pub async fn register(
    client: Client<HttpConnector, Full<Bytes>>,
) -> Result<RegisterResponseBody, BoxError> {
    let events = serde_json::json!({"events": ["INVOKE", "SHUTDOWN"]});

    let url = lambda_api_url(constants::REGISTER_PATH)?;
    let req = Request::builder()
        .method(Method::POST)
        .uri(&url)
        // This value must match the binary name, or this call will 403
        .header(constants::EXTENSION_NAME_HEADER, "rotel-lambda-extension")
        .header(
            constants::EXTENSION_ACCEPT_FEATURE,
            constants::EXTENSION_FEATURE_ACCOUNTID,
        )
        .header(CONTENT_TYPE, "application/json")
        .body(Full::from(Bytes::from(serde_json::to_vec(&events)?)))?;

    let resp = client.request(req).await?;
    if resp.status() != 200 {
        return Err(format!("Can not register extension at {}, got {}", url, resp.status()).into());
    }

    let (parts, body) = resp.into_parts();

    let ext_id = match parts.headers.get(constants::EXTENSION_ID_HEADER) {
        None => {
            return Err("Can not get extension id, got no header".into());
        }
        Some(v) => match v.to_str() {
            Ok(v) => v,
            Err(e) => {
                return Err(
                    format!("Can not get extension id, got invalid header value: {}", e).into(),
                )
            }
        },
    };

    let body = body.collect().await?.to_bytes();
    let mut reg_resp: RegisterResponseBody = serde_json::from_slice(&body)?;

    reg_resp.extension_id = ext_id.to_string();
    Ok(reg_resp)
}

// Sends a "next" request to the Lambda runtime API, which will wait until
// the next invocation request or shutdown. This request may block for an undermined
// amount of time since Lambda may put the instance to sleep. Therefore, there should
// not be a timeout set on this request.
pub async fn next_request(
    client: Client<HttpConnector, Full<Bytes>>,
    ext_id: &str,
) -> Result<NextEvent, BoxError> {
    let url = lambda_api_url(constants::NEXT_PATH)?;
    let req = Request::builder()
        .method(Method::GET)
        .uri(&url)
        .header(constants::EXTENSION_ID_HEADER, ext_id)
        .body(Full::default())?;

    let resp = client.request(req).await?;

    let (parts, body) = resp.into_parts();
    let status = parts.status;
    let text = body
        .collect()
        .await
        .map_err(|e| format!("Failed to read response body from {}: {}", url, e))
        .map(|c| c.to_bytes())
        .map(|s| String::from_utf8(s.to_vec()))?
        .map_err(|e| format!("Unable to convert response body to string: {}", e))?;
    if status != 200 {
        return Err(format!(
            "Runtime API next request failed at {}, returned: {}: {}",
            url, status, text
        )
        .into());
    }

    let event: NextEvent = serde_json::from_str(text.as_str())
        .map_err(|e| format!("Unable to deser next_event: {}", e))?;

    Ok(event)
}

pub async fn telemetry_subscribe(
    client: Client<HttpConnector, Full<Bytes>>,
    ext_id: &str,
    addr: &SocketAddr,
) -> Result<(), BoxError> {
    let sub = serde_json::json!(TelemetryAPISubscribe {
        schema_version: "2022-12-13".to_string(),
        types: vec![
            "platform".to_string(),
            "function".to_string(),
            "extension".to_string()
        ],
        buffering: TelemetryAPISubscribeBuffering {
            // todo: these are the defaults from API ref, consider adjusting
            max_items: 1000,
            max_bytes: 256 * 1024,
            timeout_ms: 100,
        },
        destination: TelemetryAPISubscribeDestination {
            protocol: "HTTP".to_string(),
            uri: format!("http://sandbox.localdomain:{}/", addr.port()),
        },
    });

    let url = lambda_api_url(constants::TELEMETRY_PATH)?;
    let req = Request::builder()
        .method(Method::PUT)
        .uri(&url)
        .header(CONTENT_TYPE, "application/json")
        .header(constants::EXTENSION_ID_HEADER, ext_id)
        .body(Full::from(Bytes::from(serde_json::to_vec(&sub)?)))?;

    let resp = client.request(req).await?;
    if resp.status() != 200 {
        return Err(format!("Can not subscribe to telemetry API at {}, got {}", url, resp.status()).into());
    }

    Ok(())
}

fn lambda_api_url(path: &str) -> Result<String, BoxError> {
    let base_api = std::env::var("AWS_LAMBDA_RUNTIME_API")
        .map_err(|e| format!("Unable to read AWS_LAMBDA_RUNTIME_API: {:?}", e))?;

    Ok(format!("http://{}{}", base_api, path))
}
