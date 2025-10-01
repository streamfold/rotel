#[allow(dead_code)]
#[derive(Clone)]
pub struct AwsConfig {
    pub(crate) aws_access_key_id: String,
    pub(crate) aws_secret_access_key: String,
    pub(crate) aws_session_token: Option<String>,
}

impl AwsConfig {
    pub fn new(
        aws_access_key_id: String,
        aws_secret_access_key: String,
        aws_session_token: Option<String>,
    ) -> Self {
        AwsConfig {
            aws_access_key_id,
            aws_secret_access_key,
            aws_session_token,
        }
    }

    pub fn from_env() -> Self {
        Self {
            aws_access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
            aws_secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
            aws_session_token: std::env::var("AWS_SESSION_TOKEN")
                .map(|s| Some(s))
                .unwrap_or(None),
        }
    }
}
