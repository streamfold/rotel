#[cfg(test)]
mod tests {
    use crate::ottl::*;
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue;
    use utilities::otlp::FakeOTLP;

    #[test]
    fn test_logs_kql_keep_all() {
        let mut logs = FakeOTLP::logs_service_request_with_logs(1, 2);

        logs.resource_logs[0].scope_logs[0].log_records[0].body = Some(AnyValue {
            value: Some(StringValue("This is log message1".to_string())),
        });
        logs.resource_logs[0].scope_logs[0].log_records[1].body = Some(AnyValue {
            value: Some(StringValue("This is log message2".to_string())),
        });

        println!("logs: {:?}", logs);

        let pipeline = parse_ksql_query_into_pipeline("s | where Body has \"message1\"").unwrap();

        let (kept, dropped) = parse_logs_export_request_with_pipeline(&pipeline, logs).unwrap();

        println!("kept: {:?}", kept);
        println!("dropped: {:?}", dropped);

        assert!(kept.resource_logs[0].scope_logs[0].log_records.len() == 1);
        assert!(dropped.resource_logs[0].scope_logs[0].log_records.len() == 1);
    }
}
