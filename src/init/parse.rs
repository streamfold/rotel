use serde::{Deserialize, Deserializer};
use std::error::Error;
use std::net::SocketAddr;
use tower::BoxError;

/// Parse a single key-value pair
pub(crate) fn parse_key_val<T, U>(s: &str) -> Result<(T, U), BoxError>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

/// Parse a single key-value pair
pub(crate) fn parse_key_vals<T, U>(s: &str) -> Result<Vec<(T, U)>, BoxError>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    Ok(s.trim()
        .split(",")
        .filter(|s| !s.is_empty())
        .map(|s| parse_key_val::<T, U>(s))
        .collect::<Result<Vec<_>, _>>()?)
}

/// Parse an endpoint
pub fn parse_endpoint(s: &str) -> Result<SocketAddr, Box<dyn Error + Send + Sync + 'static>> {
    // Use actual localhost address instead of localhost name
    let s = if s.starts_with("localhost:") {
        s.replace("localhost:", "127.0.0.1:")
    } else {
        s.to_string()
    };
    let sa: SocketAddr = s.parse()?;
    Ok(sa)
}

pub(crate) fn parse_bool_value(val: &String) -> Result<bool, BoxError> {
    match val.to_lowercase().as_str() {
        "0" | "false" => Ok(false),
        "1" | "true" => Ok(true),
        _ => Err(format!("Unable to parse bool value: {}", val).into()),
    }
}

// Support deser into a string from multiple value types. This allows a string
// environment variable to have a value that is a number or bool
pub(crate) fn deser_into_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Number(num) => Ok(num.to_string()),
        serde_json::Value::Bool(b) => Ok(b.to_string()),
        serde_json::Value::String(s) => Ok(s),
        _ => Err(serde::de::Error::custom(
            "unexpected value for string parameter",
        )),
    }
}

pub(crate) fn deser_into_string_opt<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(deser_into_string(deserializer).map(|v| Some(v))?)
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::Deserialize;
    use serde_json;
    use tokio_test::assert_ok;

    #[test]
    fn endpoint_parse() {
        let sa = parse_endpoint("localhost:4317");
        assert_ok!(sa);
        let sa = sa.unwrap();
        assert!(sa.is_ipv4());
        assert_eq!("127.0.0.1", sa.ip().to_string());
        assert_eq!(4317, sa.port());

        let sa = parse_endpoint("[::1]:4317");
        assert_ok!(sa);
        let sa = sa.unwrap();
        assert!(sa.is_ipv6());
        assert_eq!("::1", sa.ip().to_string());

        let sa = parse_endpoint("0.0.0.0:1234");
        assert_ok!(sa);
        let sa = sa.unwrap();
        assert_eq!("0.0.0.0", sa.ip().to_string());
    }

    #[derive(Deserialize, Debug)]
    struct StringConfig {
        #[serde(deserialize_with = "deser_into_string")]
        value: String,
    }

    #[test]
    fn test_deser_into_string_from_string() {
        let json = r#"{"value": "hello"}"#;
        let config: StringConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value, "hello");
    }

    #[test]
    fn test_deser_into_string_from_integer() {
        let json = r#"{"value": 42}"#;
        let config: StringConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value, "42");
    }

    #[test]
    fn test_deser_into_string_from_negative_integer() {
        let json = r#"{"value": -123}"#;
        let config: StringConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value, "-123");
    }

    #[test]
    fn test_deser_into_string_from_float() {
        let json = r#"{"value": 3.14}"#;
        let config: StringConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value, "3.14");
    }

    #[test]
    fn test_deser_into_string_from_bool_true() {
        let json = r#"{"value": true}"#;
        let config: StringConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.value, "true");
    }

    #[test]
    fn test_deser_into_string_from_null_fails() {
        let json = r#"{"value": null}"#;
        let result: Result<StringConfig, _> = serde_json::from_str(json);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unexpected value for string parameter")
        );
    }

    #[test]
    fn test_deser_into_string_from_array_fails() {
        let json = r#"{"value": [1, 2, 3]}"#;
        let result: Result<StringConfig, _> = serde_json::from_str(json);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unexpected value for string parameter")
        );
    }

    #[test]
    fn test_parse_key_vals_empty_string() {
        let result = parse_key_vals::<String, String>("").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_key_vals_whitespace_only() {
        let result = parse_key_vals::<String, String>("   ").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_key_vals_single_pair() {
        let result = parse_key_vals::<String, String>("key=value").unwrap();
        assert_eq!(result, vec![("key".to_string(), "value".to_string())]);
    }

    #[test]
    fn test_parse_key_vals_multiple_pairs() {
        let result = parse_key_vals::<String, String>("service.name=myapp,env=prod").unwrap();
        assert_eq!(
            result,
            vec![
                ("service.name".to_string(), "myapp".to_string()),
                ("env".to_string(), "prod".to_string())
            ]
        );
    }

    #[test]
    fn test_parse_key_vals_trailing_comma() {
        let result = parse_key_vals::<String, String>("key=value,").unwrap();
        assert_eq!(result, vec![("key".to_string(), "value".to_string())]);
    }

    #[test]
    fn test_parse_key_vals_multiple_trailing_commas() {
        let result = parse_key_vals::<String, String>("key=value,,,").unwrap();
        assert_eq!(result, vec![("key".to_string(), "value".to_string())]);
    }

    #[test]
    fn test_parse_key_vals_leading_comma() {
        let result = parse_key_vals::<String, String>(",key=value").unwrap();
        assert_eq!(result, vec![("key".to_string(), "value".to_string())]);
    }

    #[test]
    fn test_parse_key_vals_invalid_no_equals() {
        let result = parse_key_vals::<String, String>("invalid");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no `=` found"));
    }

    #[test]
    fn test_parse_key_vals_invalid_in_list() {
        let result = parse_key_vals::<String, String>("valid=value,invalid");
        assert!(result.is_err());
    }
}
