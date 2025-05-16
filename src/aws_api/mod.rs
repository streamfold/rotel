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
