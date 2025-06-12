use std::collections::HashMap;
use std::time::Duration;

pub(crate) fn build_table_name(
    database: &String,
    table_prefix: &String,
    table_name: &str,
) -> String {
    format!("{}.{}_{}", database, table_prefix, table_name)
}

pub(crate) fn build_cluster_string(cluster: &Option<String>) -> String {
    match cluster {
        None => "".to_string(),
        Some(name) => format!("ON CLUSTER {}", name),
    }
}

pub(crate) fn build_ttl_string(ttl: &Duration, time_field: &str) -> String {
    let ttl_secs = ttl.as_secs();
    if ttl_secs == 0 {
        return "".to_string();
    }

    if ttl_secs % 86400 == 0 {
        return format!("TTL {} + toIntervalDay({})", time_field, ttl_secs / 86400);
    }
    if ttl_secs % 3600 == 0 {
        return format!("TTL {} + toIntervalHour({})", time_field, ttl_secs / 3600);
    }
    if ttl_secs % 60 == 0 {
        return format!("TTL {} + toIntervalMinute({})", time_field, ttl_secs / 60);
    }

    format!("TTL {} + toIntervalSecond({})", time_field, ttl_secs)
}

pub(crate) fn replace_placeholders(template: &str, replacements: &HashMap<&str, &str>) -> String {
    let mut result = template.to_string();

    for (key, value) in replacements {
        let placeholder = format!("%%{}%%", key);
        result = result.replace(&placeholder, value);
    }

    result
}

pub(crate) fn get_json_col_type<'a>(use_json: bool) -> &'a str {
    match use_json {
        true => "JSON",
        false => "Map(LowCardinality(String), String)",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_zero_ttl() {
        let duration = Duration::from_secs(0);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "");
    }

    #[test]
    fn test_day_interval() {
        // One day (86400 seconds)
        let duration = Duration::from_secs(86400);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalDay(1)");

        // Multiple days
        let duration = Duration::from_secs(86400 * 7);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalDay(7)");
    }

    #[test]
    fn test_hour_interval() {
        // One hour (3600 seconds)
        let duration = Duration::from_secs(3600);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalHour(1)");

        // Multiple hours (but not a full day)
        let duration = Duration::from_secs(3600 * 23);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalHour(23)");
    }

    #[test]
    fn test_minute_interval() {
        // One minute (60 seconds)
        let duration = Duration::from_secs(60);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalMinute(1)");

        // Multiple minutes (but not a full hour)
        let duration = Duration::from_secs(60 * 59);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalMinute(59)");
    }

    #[test]
    fn test_second_interval() {
        // A few seconds (not a full minute)
        let duration = Duration::from_secs(45);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalSecond(45)");

        // A large number of seconds (not divisible by 60)
        let duration = Duration::from_secs(3601); // 1 hour and 1 second
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalSecond(3601)");
    }

    #[test]
    fn test_different_time_field() {
        // Test with a different time field name
        let duration = Duration::from_secs(86400);
        let result = build_ttl_string(&duration, "created_at");
        assert_eq!(result, "TTL created_at + toIntervalDay(1)");
    }

    #[test]
    fn test_edge_cases() {
        // Test with very large duration
        let duration = Duration::from_secs(86400 * 365 * 10); // ~10 years
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalDay(3650)");

        // Test with one second
        let duration = Duration::from_secs(1);
        let result = build_ttl_string(&duration, "timestamp");
        assert_eq!(result, "TTL timestamp + toIntervalSecond(1)");
    }
}
