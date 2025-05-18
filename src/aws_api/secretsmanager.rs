use crate::aws_api::arn::AwsArn;
use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::client::AwsClient;
use crate::aws_api::error::Error;
use crate::aws_api::SECRETS_MANAGER_SERVICE;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method, Uri};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use tracing::error;

pub struct SecretsManager<'a> {
    client: &'a AwsClient,
    service_name: &'static str,
}

#[derive(Debug, Deserialize)]
pub struct BatchResponse {
    #[serde(rename = "Errors")]
    pub errors: Vec<BatchResponseError>,

    #[serde(rename = "SecretValues")]
    pub secret_values: Vec<ResponseSecret>,
}

#[derive(Debug, Deserialize)]
pub struct BatchResponseError {
    // #[serde(rename = "ErrorCode")]
    // pub error_code: String,
    //
    #[serde(rename = "Message")]
    pub message: String,

    #[serde(rename = "SecretId")]
    pub secret_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ResponseSecret {
    #[serde(rename = "ARN")]
    pub arn: Option<String>,

    #[serde(rename = "CreatedDate")]
    pub created_date: f64,

    #[serde(rename = "Name")]
    pub name: String,

    //
    // #[serde(rename = "SecretBinary")]
    // pub secret_binary: Option<Base64>,
    #[serde(rename = "SecretString")]
    pub secret_string: String,

    #[serde(rename = "VersionId")]
    pub version_id: String,
    // #[serde(rename = "VersionStages")]
    // pub version_stages: Vec<String>,
}

impl<'a> SecretsManager<'a> {
    pub(crate) fn new(client: &'a AwsClient) -> Self {
        Self {
            client,
            service_name: SECRETS_MANAGER_SERVICE,
        }
    }

    pub async fn batch_get_secret(
        &self,
        secret_arns: &[AwsArn],
    ) -> Result<HashMap<String, ResponseSecret>, Error> {
        let mut arns_by_endpoint = HashMap::new();
        for arn in secret_arns {
            if arn.service != self.service_name {
                return Err(Error::ArnParseError(arn.to_string()));
            }

            arns_by_endpoint
                .entry(arn.get_endpoint())
                .or_insert_with(|| Vec::new())
                .push(arn);
        }

        let mut res = HashMap::new();
        for (endpoint, arns) in &arns_by_endpoint {
            let endpoint = endpoint.parse::<Uri>()?;

            let payload = json!({
                "SecretIdList": arns.iter().map(|arn| arn.to_string()).collect::<Vec<String>>(),
            });

            let payload_bytes = serde_json::to_vec(&payload)?;

            let mut hdrs = HeaderMap::new();
            hdrs.insert(
                "X-Amz-Target",
                HeaderValue::from_static("secretsmanager.BatchGetSecretValue"),
            );
            hdrs.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-amz-json-1.1"),
            );

            // Sign the request
            let signer = AwsRequestSigner::new(
                self.service_name,
                &arns[0].region,
                &self.client.config.aws_access_key_id,
                &self.client.config.aws_secret_access_key,
                self.client.config.aws_session_token.as_deref(),
                SystemClock,
            );
            let signed_request = signer.sign(endpoint, Method::POST, hdrs, payload_bytes)?;

            // Send the request
            let response = self.client.perform(signed_request).await?;

            let result: BatchResponse = serde_json::from_slice(response.as_ref())?;

            if !result.errors.is_empty() {
                let arns = result
                    .errors
                    .into_iter()
                    .map(|e| (e.secret_id, e.message))
                    .collect::<Vec<(String, String)>>();
                error!(arns = ?arns, "Unable to lookup secrets");
                return Err(Error::InvalidSecrets(
                    arns.into_iter().map(|arn| arn.0).collect(),
                ));
            }

            for secret in result.secret_values {
                if secret.arn.is_none() {
                    error!(secret = secret.name, "Secret was missing ARN");
                    return Err(Error::InvalidSecrets(
                        secret_arns.into_iter().map(|arn| arn.to_string()).collect(),
                    ));
                }

                let arn = secret.arn.clone().unwrap();
                res.insert(arn, secret);
            }
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws_api::config::AwsConfig;
    use crate::aws_api::parse_test_arns;
    use crate::exporters::crypto_init_tests::init_crypto;

    #[tokio::test]
    async fn test_basic_secret_retrieval() {
        // TEST_SECRETSMANAGER_ARNS should be set to a comma-separated list of k=v pairs,
        // where k is an ARN of a secret and v is the secret value to test against.
        let test_secret_arns = std::env::var("TEST_SECRETSMANAGER_ARNS");
        if !test_secret_arns.is_ok() {
            println!("Skipping test_basic_secret_retrieval due to unset envvar");
            return;
        }

        let mut test_arns = parse_test_arns(test_secret_arns.unwrap());

        init_crypto();

        let client = AwsClient::new(AwsConfig::from_env()).unwrap();

        let ss = client.secrets_manager();

        let parsed_arns: Vec<AwsArn> = test_arns
            .iter()
            .map(|(arn, _)| arn.parse::<AwsArn>().unwrap())
            .collect();
        let res = ss.batch_get_secret(&parsed_arns).await.unwrap();

        for (test_arn, test_value) in &test_arns {
            let entry = res.get(test_arn).unwrap();
            assert_eq!(*test_value, entry.secret_string);
        }

        // Test for non-existent ARN
        test_arns.push((
            "arn:aws:secretsmanager:us-east-1:123345654789:secret:does-not-exist".to_string(),
            "foobar".to_string(),
        ));

        let parsed_arns: Vec<AwsArn> = test_arns
            .iter()
            .map(|(arn, _)| arn.parse::<AwsArn>().unwrap())
            .collect();
        let res = ss.batch_get_secret(&parsed_arns).await;

        assert!(res.is_err());
    }
}
