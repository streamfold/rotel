use crate::aws_api::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AwsArn {
    pub(crate) partition: String,
    pub(crate) service: String,
    pub(crate) region: String,
    pub(crate) account_id: String,
    pub(crate) resource_type: String,
    pub(crate) resource_id: String,
    pub(crate) resource_field: String,
}

impl FromStr for AwsArn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Split one more than we need to verify it's valid
        let mut parts: Vec<String> = s.splitn(9, ":").map(|s| s.to_string()).collect();
        let num_parts = parts.len();
        if num_parts < 6 || num_parts >= 8 {
            return Err(Error::ArnParseError(s.to_string()));
        }

        for part in &parts {
            if part.is_empty() {
                return Err(Error::ArnParseError(s.to_string()));
            }
        }

        let mut arn = AwsArn {
            partition: "".to_string(),
            service: "".to_string(),
            region: "".to_string(),
            account_id: "".to_string(),
            resource_type: "".to_string(),
            resource_id: "".to_string(),
            resource_field: "".to_string(),
        };

        let resource_id = parts.pop().unwrap();
        let res_parts: Vec<&str> = resource_id.splitn(2, "#").collect();
        if res_parts.len() > 1 {
            if res_parts[0].is_empty() || res_parts[1].is_empty() {
                return Err(Error::ArnParseError(s.to_string()));
            }

            arn.resource_id = res_parts[0].to_string();
            arn.resource_field = res_parts[1].to_string();
        } else {
            arn.resource_id = resource_id;
        }
        if num_parts == 7 {
            arn.resource_type = parts.pop().unwrap();
        }
        arn.account_id = parts.pop().unwrap();
        arn.region = parts.pop().unwrap();
        arn.service = parts.pop().unwrap();
        arn.partition = parts.pop().unwrap();

        if parts.pop().unwrap() != "arn" {
            return Err(Error::ArnParseError(s.to_string()));
        }

        Ok(arn)
    }
}

impl Display for AwsArn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::with_capacity(7);
        parts.push("arn");
        parts.push(self.partition.as_str());
        parts.push(self.service.as_str());
        parts.push(self.region.as_str());
        parts.push(self.account_id.as_str());
        if self.resource_type != "" {
            parts.push(self.resource_type.as_str());
        }
        let s = if self.resource_field != "" {
            &format!("{}#{}", self.resource_id, self.resource_field)
        } else {
            &self.resource_id
        };
        parts.push(s.as_str());

        write!(f, "{}", parts.join(":"))
    }
}

impl AwsArn {
    pub fn get_endpoint(&self) -> String {
        let domain = if self.region.starts_with("cn-") {
            "amazonaws.com.cn"
        } else {
            "amazonaws.com"
        };

        format!("https://{}.{}.{}/", self.service, self.region, domain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_secrets_arn_valid() {
        let input = "arn:aws:secretsmanager:us-east-2:891477334659:secret:test-ohio-secret-L86lpn";

        let arn = input.parse::<AwsArn>().unwrap();

        assert_eq!(input, arn.to_string());
        assert_eq!("aws", arn.partition);
        assert_eq!("secretsmanager", arn.service);
        assert_eq!("us-east-2", arn.region);
        assert_eq!("891477334659", arn.account_id);
        assert_eq!("secret", arn.resource_type);
        assert_eq!("test-ohio-secret-L86lpn", arn.resource_id);
        assert_eq!("", arn.resource_field);

        let input =
            "arn:aws:secretsmanager:us-east-2:891477334659:secret:test-ohio-secret-L86lpn#key-name";
        let arn = input.parse::<AwsArn>().unwrap();

        assert_eq!(input, arn.to_string());
        assert_eq!("test-ohio-secret-L86lpn", arn.resource_id);
        assert_eq!("key-name", arn.resource_field);
    }

    #[test]
    fn test_parse_ssm_arn_valid() {
        let input = "arn:aws:ssm:us-east-1:123377354456:parameter/ci-test-value";

        let arn = input.parse::<AwsArn>().unwrap();

        assert_eq!(input, arn.to_string());

        assert_eq!("aws", arn.partition);
        assert_eq!("ssm", arn.service);
        assert_eq!("us-east-1", arn.region);
        assert_eq!("123377354456", arn.account_id);
        assert_eq!("", arn.resource_type);
        assert_eq!("parameter/ci-test-value", arn.resource_id);
    }

    #[test]
    fn test_parse_arn_invalid() {
        assert!(
            !"arn:aws:secretsmanager:us-east-2:891477334659:secret:test-ohio-secret-L86lpn:extra"
                .parse::<AwsArn>()
                .is_ok()
        );
        assert!(
            !"arn:aws:secretsmanager::891477334659:secret:test-ohio-secret-L86lpn"
                .parse::<AwsArn>()
                .is_ok()
        );
        assert!(
            !"arn:aws:secretsmanager891477334659:secret:test-ohio-secret-L86lpn"
                .parse::<AwsArn>()
                .is_ok()
        );
        assert!(
            !"arn:aws:secretsmanager:us-east-2:891477334659:secret:test-ohio-secret-L86lpn#"
                .parse::<AwsArn>()
                .is_ok()
        );
        assert!(
            !"arn:aws:secretsmanager:us-east-2:891477334659:secret:#"
                .parse::<AwsArn>()
                .is_ok()
        );
        assert!(
            !"arn:aws:secretsmanager:us-east-2:891477334659:secret:#key-name"
                .parse::<AwsArn>()
                .is_ok()
        );
    }
}
