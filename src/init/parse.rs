use serde::{
    Deserialize, Deserializer,
    de::{self, Visitor},
};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use tower::BoxError;

/// Parse a single key-value pair
pub(crate) fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
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

// Parse comma-separated, key value pairs: apple=orange,dog=cat
pub(crate) fn deserialize_key_value_pairs<'de, D>(
    deserializer: D,
) -> Result<Vec<(String, String)>, D::Error>
where
    D: Deserializer<'de>,
{
    struct KeyValueVisitor;

    impl<'de> Visitor<'de> for KeyValueVisitor {
        type Value = Vec<(String, String)>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string containing comma-separated key=value pairs")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let mut pairs = Vec::new();

            if value.is_empty() {
                return Ok(pairs);
            }

            for pair in value.split(',') {
                let pair = pair.trim();

                if pair.is_empty() {
                    continue;
                }

                // Check if the pair contains exactly one '='
                let parts: Vec<&str> = pair.split('=').collect();
                if parts.len() != 2 {
                    return Err(E::custom(format!(
                        "Invalid key=value pair: '{}'. Expected format: key=value",
                        pair
                    )));
                }

                let key = parts[0].trim().to_string();
                let value = parts[1].trim().to_string();

                if key.is_empty() {
                    return Err(E::custom(format!("Empty key in pair: '{}'", pair)));
                }

                pairs.push((key, value));
            }

            Ok(pairs)
        }
    }

    deserializer.deserialize_str(KeyValueVisitor)
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
    struct Config {
        #[serde(deserialize_with = "deserialize_key_value_pairs")]
        properties: Vec<(String, String)>,
    }

    #[test]
    fn test_valid_key_value_pairs() {
        let json = r#"{"properties": "apple=orange,dog=cat"}"#;
        let config: Config = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.properties,
            vec![
                ("apple".to_string(), "orange".to_string()),
                ("dog".to_string(), "cat".to_string())
            ]
        );
    }

    #[test]
    fn test_empty_string() {
        let json = r#"{"properties": ""}"#;
        let config: Config = serde_json::from_str(json).unwrap();

        assert!(config.properties.is_empty());
    }

    #[test]
    fn test_single_pair() {
        let json = r#"{"properties": "key=value"}"#;
        let config: Config = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.properties,
            vec![("key".to_string(), "value".to_string())]
        );
    }

    #[test]
    fn test_whitespace_handling() {
        let json = r#"{"properties": " key1 = value1 , key2 = value2 "}"#;
        let config: Config = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.properties,
            vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string())
            ]
        );
    }

    #[test]
    fn test_preserves_order() {
        let json = r#"{"properties": "first=1,second=2,third=3"}"#;
        let config: Config = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.properties,
            vec![
                ("first".to_string(), "1".to_string()),
                ("second".to_string(), "2".to_string()),
                ("third".to_string(), "3".to_string())
            ]
        );
    }

    #[test]
    fn test_allows_duplicate_keys() {
        let json = r#"{"properties": "key=value1,key=value2"}"#;
        let config: Config = serde_json::from_str(json).unwrap();

        assert_eq!(
            config.properties,
            vec![
                ("key".to_string(), "value1".to_string()),
                ("key".to_string(), "value2".to_string())
            ]
        );
    }

    #[test]
    fn test_invalid_no_equals() {
        let json = r#"{"properties": "apple=orange,dog"}"#;
        let result: Result<Config, _> = serde_json::from_str(json);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid key=value pair: 'dog'")
        );
    }

    #[test]
    fn test_invalid_multiple_equals() {
        let json = r#"{"properties": "apple=orange=fruit"}"#;
        let result: Result<Config, _> = serde_json::from_str(json);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid key=value pair")
        );
    }

    #[test]
    fn test_empty_key() {
        let json = r#"{"properties": "=value"}"#;
        let result: Result<Config, _> = serde_json::from_str(json);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Empty key"));
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
}
