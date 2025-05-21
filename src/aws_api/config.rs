#[allow(dead_code)]
#[derive(Clone)]
pub struct AwsConfig {
    pub(crate) region: String,
    pub(crate) aws_access_key_id: String,
    pub(crate) aws_secret_access_key: String,
    pub(crate) aws_session_token: Option<String>,
}

impl AwsConfig {
    pub fn from_env() -> Self {
        Self {
            region: std::env::var("AWS_DEFAULT_REGION").unwrap_or("us-east-1".to_string()),
            aws_access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
            aws_secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
            aws_session_token: std::env::var("AWS_SESSION_TOKEN")
                .map(|s| Some(s))
                .unwrap_or(None),
        }
    }
}
