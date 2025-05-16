use crate::aws_api::PARAM_STORE_SERVICE;
use crate::aws_api::arn::AwsArn;
use crate::aws_api::auth::{AwsRequestSigner, SystemClock};
use crate::aws_api::client::AwsClient;
use crate::aws_api::error::Error;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, Method, Uri};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use tracing::error;

pub struct ParameterStore<'a> {
    client: &'a AwsClient,
    service_name: &'static str,
}

#[derive(Debug, Deserialize)]
pub struct GetParametersResponse {
    /// The parameter object.
    #[serde(rename = "Parameters")]
    pub parameters: Vec<Parameter>,

    #[serde(rename = "InvalidParameters")]
    pub invalid_parameters: Vec<InvalidParameters>,
}

#[derive(Debug, Deserialize)]
pub struct InvalidParameters {
    #[serde(rename = "Name")]
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct Parameter {
    /// The Amazon Resource Name (ARN) of the parameter.
    #[serde(rename = "ARN")]
    pub arn: Option<String>,

    // /// The data type of the parameter, such as text, aws:ec2:image, or aws:tag-specification.
    // #[serde(rename = "DataType")]
    // pub data_type: Option<String>,
    /// The last modification date of the parameter.
    #[serde(rename = "LastModifiedDate")]
    pub last_modified_date: Option<f64>,

    /// The name of the parameter.
    #[serde(rename = "Name")]
    pub name: String,

    // /// The unique identifier for the parameter version.
    // #[serde(rename = "Selector")]
    // pub selector: Option<String>,

    // /// The parameter source.
    // #[serde(rename = "SourceResult")]
    // pub source_result: Option<String>,
    /// The parameter type.
    #[serde(rename = "Type")]
    pub type_: String,

    /// The parameter value.
    #[serde(rename = "Value")]
    pub value: String,

    /// The parameter version.
    #[serde(rename = "Version")]
    pub version: Option<i64>,
    // /// Tags associated with the parameter.
    // #[serde(rename = "Tags")]
    // pub tags: Option<HashMap<String, String>>,
}

impl<'a> ParameterStore<'a> {
    pub(crate) fn new(client: &'a AwsClient) -> Self {
        Self {
            client,
            service_name: PARAM_STORE_SERVICE,
        }
    }

    pub async fn get_parameters(
        &self,
        param_arns: &[AwsArn],
    ) -> Result<HashMap<String, Parameter>, Error> {
        let mut arns_by_endpoint = HashMap::new();
        for arn in param_arns {
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
                "Names": arns.iter().map(|arn| arn.to_string()).collect::<Vec<String>>(),
                "WithDecryption": true,
            });

            let payload_bytes = serde_json::to_vec(&payload)?;

            let mut hdrs = HeaderMap::new();
            hdrs.insert(
                "X-Amz-Target",
                HeaderValue::from_static("AmazonSSM.GetParameters"),
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

            let result: GetParametersResponse = serde_json::from_slice(response.as_ref())?;

            if !result.invalid_parameters.is_empty() {
                return Err(Error::InvalidSecrets(
                    result
                        .invalid_parameters
                        .into_iter()
                        .map(|i| i.name)
                        .collect(),
                ));
            }

            for param in result.parameters {
                if param.arn.is_none() {
                    error!(parameter = param.name, "Parameter was missing ARN");
                    return Err(Error::InvalidSecrets(
                        arns.into_iter().map(|arn| arn.to_string()).collect(),
                    ));
                }

                let arn = param.arn.clone().unwrap();
                res.insert(arn, param);
            }
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws_api::config::AwsConfig;
    use crate::test_util::{init_crypto, parse_test_arns};

    #[tokio::test]
    async fn test_basic_paramstore_retrieval() {
        // TEST_PARAMSTORE_ARNS should be set to a comma-separated list of k=v pairs,
        // where k is an ARN of a secret and v is the secret value to test against.
        let test_paramstore_arns = std::env::var("TEST_PARAMSTORE_ARNS");
        if !test_paramstore_arns.is_ok() {
            println!("Skipping test_basic_paramstore_retrieval due to unset envvar");
            return;
        }

        let mut test_arns = parse_test_arns(test_paramstore_arns.unwrap());

        init_crypto();

        let client = AwsClient::new(AwsConfig::from_env()).unwrap();

        let ps = client.parameter_store();

        let arn_values: Vec<AwsArn> = test_arns
            .iter()
            .map(|(arn, _)| arn.parse::<AwsArn>().unwrap())
            .collect();
        let res = ps.get_parameters(&arn_values).await.unwrap();

        for test_arn in &test_arns {
            let entry = res.get(&test_arn.0).unwrap();

            assert_eq!(test_arn.1, entry.value);
        }

        // Test for non-existent ARN
        test_arns.push((
            "arn:aws:ssm:us-east-1:123374564789:parameter/invalid-param".to_string(),
            "foobar".to_string(),
        ));

        let arn_values: Vec<AwsArn> = test_arns
            .iter()
            .map(|(arn, _)| arn.parse::<AwsArn>().unwrap())
            .collect();
        let res = ps.get_parameters(&arn_values).await;

        assert!(res.is_err());
    }
}
