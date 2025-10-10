#[cfg(feature = "aws_iam")]
use {
    aws_config::Region as AwsRegion, aws_config::meta::region::ProvideRegion,
    aws_credential_types::provider::SharedCredentialsProvider,
};

use thiserror::Error;

pub enum AwsCredsProvider {
    #[cfg(feature = "aws_iam")]
    Dynamic(SharedCredentialsProvider),

    Static(AwsCreds),
}

#[derive(Clone)]
pub struct AwsCreds {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl AwsCreds {
    pub fn new(
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    ) -> Self {
        Self {
            access_key_id,
            secret_access_key,
            session_token,
        }
    }

    pub fn access_key_id(&self) -> &String {
        &self.access_key_id
    }

    pub fn secret_access_key(&self) -> &String {
        &self.secret_access_key
    }

    pub fn session_token(&self) -> &Option<String> {
        &self.session_token
    }

    #[cfg(not(feature = "aws_iam"))]
    pub fn from_env() -> Self {
        Self {
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
            session_token: std::env::var("AWS_SESSION_TOKEN")
                .map(|s| Some(s))
                .unwrap_or(None),
        }
    }
}

#[derive(Debug, Error)]
pub enum AwsCredsError {
    #[error("No credentials provider configured")]
    NoProvider,

    #[error("Unable to identify the AWS region, try setting AWS_DEFAULT_REGION")]
    NoRegion,

    #[error("Failed to provide credentials: {0}")]
    ProviderError(String),
}

impl AwsCredsProvider {
    //
    // The implementation of new() switches based on the feature flag "aws_iam"
    //

    #[cfg(feature = "aws_iam")]
    pub async fn new() -> Result<Self, AwsCredsError> {
        let region = resolve_region().await.ok_or(AwsCredsError::NoRegion)?;

        let cfg = aws_config::defaults(aws_config::BehaviorVersion::v2025_08_07())
            .region(region)
            .load()
            .await;

        let provider = cfg
            .credentials_provider()
            .ok_or(AwsCredsError::NoProvider)?;

        Ok(AwsCredsProvider::Dynamic(provider))
    }

    #[cfg(not(feature = "aws_iam"))]
    pub async fn new() -> Result<Self, AwsCredsError> {
        let creds = AwsCreds::from_env();

        Ok(AwsCredsProvider::Static(creds))
    }

    // Mostly for testing
    pub fn from_static(creds: AwsCreds) -> Self {
        AwsCredsProvider::Static(creds)
    }

    pub async fn get_creds(&self) -> Result<AwsCreds, AwsCredsError> {
        match self {
            #[cfg(feature = "aws_iam")]
            AwsCredsProvider::Dynamic(provider) => {
                use aws_credential_types::provider::ProvideCredentials;

                let credentials = provider
                    .provide_credentials()
                    .await
                    .map_err(|e| AwsCredsError::ProviderError(e.to_string()))?;

                Ok(AwsCreds::new(
                    credentials.access_key_id().to_string(),
                    credentials.secret_access_key().to_string(),
                    credentials.session_token().map(|s| s.to_string()),
                ))
            }

            AwsCredsProvider::Static(creds) => Ok(creds.clone()),
        }
    }
}
// Concept from Vector: resolve region early so that we can fail fast
#[cfg(feature = "aws_iam")]
async fn resolve_region() -> Option<AwsRegion> {
    region_provider().region().await
}

#[cfg(feature = "aws_iam")]
fn region_provider() -> impl ProvideRegion {
    let config = aws_config::provider_config::ProviderConfig::default();

    aws_config::meta::region::RegionProviderChain::first_try(
        aws_config::environment::EnvironmentVariableRegionProvider::new(),
    )
    .or_else(aws_config::profile::ProfileFileRegionProvider::builder().build())
    .or_else(
        aws_config::imds::region::ImdsRegionProvider::builder()
            .configure(&config)
            .build(),
    )
}
