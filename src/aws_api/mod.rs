pub mod arn;
mod auth;
pub mod client;
pub mod config;
mod error;
mod paramstore;
mod secretsmanager;

pub const SECRETS_MANAGER_SERVICE: &str = "secretsmanager";
pub const PARAM_STORE_SERVICE: &str = "ssm";

// This is the minimum of what SecretsManager and ParamStore supports for
// batch calls. It would be surprising to have > 10 secrets.
pub const MAX_LOOKUP_LEN: usize = 10;

pub fn parse_test_arns(test_arns: String) -> Vec<(String, String)> {
    test_arns
        .split(",")
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() == 2 {
                Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
            } else {
                None // Skip malformed pairs that don't have an equals sign
            }
        })
        .collect()
}
