use crate::exporters::awsemf::{Region};
use clap::Args;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(default)]
pub struct AwsEmfExporterArgs {
    /// AWS EMF Exporter Region
    #[arg(
        id("AWS_EMF_REGION"),
        value_enum,
        long("awsemf-exporter-region"),
        env = "ROTEL_AWSEMF_EXPORTER_REGION",
        default_value = "us-east-1"
    )]
    pub region: Region,

    /// AWS EMF Exporter custom endpoint override
    #[arg(
        id("AWS_EMF_CUSTOM_ENDPOINT"),
        long("awsemf-exporter-custom-endpoint"),
        env = "ROTEL_AWSEMF_EXPORTER_CUSTOM_ENDPOINT"
    )]
    pub custom_endpoint: Option<String>,

    /// CloudWatch log group name
    #[arg(
        long("awsemf-exporter-log-group-name"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_GROUP_NAME",
        default_value = "/rotel/metrics"
    )]
    pub log_group_name: String,

    /// CloudWatch log stream name
    #[arg(
        long("awsemf-exporter-log-stream-name"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_STREAM_NAME"
    )]
    pub log_stream_name: Option<String>,

    /// CloudWatch metrics namespace
    #[arg(
        long("awsemf-exporter-namespace"),
        env = "ROTEL_AWSEMF_EXPORTER_NAMESPACE",
        default_value = "Rotel/Metrics"
    )]
    pub namespace: String,

    /// Retain initial value of delta metric
    #[arg(
        long("awsemf-exporter-retain-initial-value-of-delta-metric"),
        env = "ROTEL_AWSEMF_EXPORTER_RETAIN_INITIAL_VALUE_OF_DELTA_METRIC",
        action = clap::ArgAction::SetTrue
    )]
    pub retain_initial_value_of_delta_metric: bool,

    /// Log retention in days (0 for never expire)
    #[arg(
        long("awsemf-exporter-log-retention"),
        env = "ROTEL_AWSEMF_EXPORTER_LOG_RETENTION",
        default_value = "0"
    )]
    pub log_retention: i32,

    /// CloudWatch Log Group tags (key=value pairs, comma-separated)
    #[arg(
        long("awsemf-exporter-tags"),
        env = "ROTEL_AWSEMF_EXPORTER_TAGS",
        value_parser = parse_key_value_pairs,
        help = "CloudWatch Log Group tags in key=value format, comma-separated"
    )]
    pub tags: Option<HashMap<String, String>>,

    /// Parse JSON-encoded attribute values (comma-separated list of attribute keys)
    #[arg(
        long("awsemf-exporter-parse-json-encoded-attr-values"),
        env = "ROTEL_AWSEMF_EXPORTER_PARSE_JSON_ENCODED_ATTR_VALUES",
        value_delimiter = ',',
        help = "List of attribute keys whose values are JSON-encoded strings"
    )]
    pub parse_json_encoded_attr_values: Vec<String>,

    /// Output destination (cloudwatch or stdout)
    #[arg(
        long("awsemf-exporter-output-destination"),
        env = "ROTEL_AWSEMF_EXPORTER_OUTPUT_DESTINATION",
        default_value = "cloudwatch",
        help = "Output destination: cloudwatch or stdout"
    )]
    pub output_destination: String,

    /// Enable EKS Fargate Container Insights formatting
    #[arg(
        long("awsemf-exporter-eks-fargate-container-insights-enabled"),
        env = "ROTEL_AWSEMF_EXPORTER_EKS_FARGATE_CONTAINER_INSIGHTS_ENABLED",
        action = clap::ArgAction::SetTrue
    )]
    pub eks_fargate_container_insights_enabled: bool,

    /// Enable resource to telemetry conversion
    #[arg(
        long("awsemf-exporter-resource-to-telemetry-conversion-enabled"),
        env = "ROTEL_AWSEMF_EXPORTER_RESOURCE_TO_TELEMETRY_CONVERSION_ENABLED",
        action = clap::ArgAction::SetTrue
    )]
    pub resource_to_telemetry_conversion_enabled: bool,

    /// Enable detailed metrics (preserve quantile population for summaries)
    #[arg(
        long("awsemf-exporter-detailed-metrics"),
        env = "ROTEL_AWSEMF_EXPORTER_DETAILED_METRICS",
        action = clap::ArgAction::SetTrue
    )]
    pub detailed_metrics: bool,

    /// EMF version (0 or 1)
    #[arg(
        long("awsemf-exporter-version"),
        env = "ROTEL_AWSEMF_EXPORTER_VERSION",
        default_value = "1",
        help = "EMF version: 0 (without _aws) or 1 (with _aws)"
    )]
    pub version: String,

    /// Metric declarations (JSON file path for complex metric declarations)
    #[arg(
        long("awsemf-exporter-metric-declarations-file"),
        env = "ROTEL_AWSEMF_EXPORTER_METRIC_DECLARATIONS_FILE",
        help = "Path to JSON file containing metric declarations"
    )]
    pub metric_declarations_file: Option<String>,

    /// Metric descriptors (JSON file path for metric descriptor overrides)
    #[arg(
        long("awsemf-exporter-metric-descriptors-file"),
        env = "ROTEL_AWSEMF_EXPORTER_METRIC_DESCRIPTORS_FILE",
        help = "Path to JSON file containing metric descriptors"
    )]
    pub metric_descriptors_file: Option<String>,
}

impl Default for AwsEmfExporterArgs {
    fn default() -> Self {
        Self {
            region: Region::UsEast1,
            custom_endpoint: None,
            log_group_name: "/rotel/metrics".to_string(),
            log_stream_name: None,
            namespace: "Rotel/Metrics".to_string(),
            retain_initial_value_of_delta_metric: false,
            log_retention: 0,
            tags: None,
            parse_json_encoded_attr_values: Vec::new(),
            output_destination: "cloudwatch".to_string(),
            eks_fargate_container_insights_enabled: false,
            resource_to_telemetry_conversion_enabled: false,
            detailed_metrics: false,
            version: "1".to_string(),
            metric_declarations_file: None,
            metric_descriptors_file: None,
        }
    }
}

/// Parse key=value pairs from a comma-separated string
fn parse_key_value_pairs(s: &str) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    
    if s.trim().is_empty() {
        return Ok(map);
    }
    
    for pair in s.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        
        let parts: Vec<&str> = pair.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid key=value pair: '{}'", pair));
        }
        
        let key = parts[0].trim();
        let value = parts[1].trim();
        
        if key.is_empty() {
            return Err(format!("Empty key in pair: '{}'", pair));
        }
        
        // Validate key length (1-128 characters as per AWS requirements)
        if key.len() > 128 {
            return Err(format!("Key '{}' exceeds maximum length of 128 characters", key));
        }
        
        // Validate value length (1-256 characters as per AWS requirements) 
        if value.len() > 256 {
            return Err(format!("Value for key '{}' exceeds maximum length of 256 characters", key));
        }
        
        map.insert(key.to_string(), value.to_string());
    }
    
    // Validate total number of tags (max 50 as per AWS requirements)
    if map.len() > 50 {
        return Err(format!("Too many tags: {} (maximum is 50)", map.len()));
    }
    
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_key_value_pairs() {
        // Test valid input
        let result = parse_key_value_pairs("key1=value1,key2=value2").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("key1"), Some(&"value1".to_string()));
        assert_eq!(result.get("key2"), Some(&"value2".to_string()));

        // Test empty input
        let result = parse_key_value_pairs("").unwrap();
        assert_eq!(result.len(), 0);

        // Test whitespace handling
        let result = parse_key_value_pairs("  key1 = value1 ,  key2 = value2  ").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("key1"), Some(&"value1".to_string()));
        assert_eq!(result.get("key2"), Some(&"value2".to_string()));

        // Test invalid format
        assert!(parse_key_value_pairs("invalid").is_err());
        assert!(parse_key_value_pairs("key1=value1,invalid").is_err());
        
        // Test empty key
        assert!(parse_key_value_pairs("=value1").is_err());
    }

    #[test]
    fn test_default_values() {
        let args = AwsEmfExporterArgs::default();
        assert_eq!(args.region, Region::UsEast1);
        assert_eq!(args.log_group_name, "/rotel/metrics");
        assert_eq!(args.namespace, "Rotel/Metrics");
        assert_eq!(args.output_destination, "cloudwatch");
        assert!(!args.retain_initial_value_of_delta_metric);
        assert!(!args.detailed_metrics);
        assert_eq!(args.version, "1");
    }
}