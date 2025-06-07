use regex::Regex;

#[derive(Debug, PartialEq)]
pub struct AwsService {
    pub service: String,
    pub region: String,
}

pub fn parse_aws_hostname(hostname: &str) -> Option<AwsService> {
    // Pattern to match: service.region.amazonaws.com
    let re = Regex::new(r"^([a-zA-Z0-9-]+)\.([a-zA-Z0-9-]+)\.amazonaws\.com$").unwrap();

    re.captures(hostname).map(|caps| AwsService {
        service: caps[1].to_string(),
        region: caps[2].to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_hostnames() {
        assert_eq!(
            parse_aws_hostname("logs.us-west-2.amazonaws.com"),
            Some(AwsService {
                service: "logs".to_string(),
                region: "us-west-2".to_string(),
            })
        );

        assert_eq!(
            parse_aws_hostname("xray.us-west-2.amazonaws.com"),
            Some(AwsService {
                service: "xray".to_string(),
                region: "us-west-2".to_string(),
            })
        );
    }

    #[test]
    fn test_invalid_hostnames() {
        assert_eq!(parse_aws_hostname("invalid-hostname.com"), None);
        assert_eq!(parse_aws_hostname("not.amazonaws.com"), None);
        assert_eq!(parse_aws_hostname("service.amazonaws.com"), None);
    }
}
