use bytes::BytesMut;
use data_engine_expressions::PipelineExpression;
use data_engine_kql_parser::{KqlParser, Parser};
use data_engine_parser_abstractions::{
    ParserError, ParserMapKeySchema, ParserMapSchema, ParserOptions,
};
use data_engine_recordset_otlp_bridge::process_protobuf_otlp_export_logs_service_request_using_pipeline_expression;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use tower::BoxError;

mod kql_test;

pub(crate) fn parse_ksql_query_into_pipeline(
    query: &str,
) -> Result<PipelineExpression, Vec<ParserError>> {
    let options = ParserOptions::new()
        .with_attached_data_names(&["resource", "instrumentation_scope", "scope"])
        .with_source_map_schema(
            ParserMapSchema::new()
                .set_default_map_key("Attributes")
                .with_key_definition("Timestamp", ParserMapKeySchema::DateTime)
                .with_key_definition("ObservedTimestamp", ParserMapKeySchema::DateTime)
                .with_key_definition("SeverityNumber", ParserMapKeySchema::Integer)
                .with_key_definition("SeverityText", ParserMapKeySchema::String)
                .with_key_definition("Body", ParserMapKeySchema::Any)
                .with_key_definition("Attributes", ParserMapKeySchema::Map)
                .with_key_definition("TraceId", ParserMapKeySchema::Array)
                .with_key_definition("SpanId", ParserMapKeySchema::Array)
                .with_key_definition("TraceFlags", ParserMapKeySchema::Integer)
                .with_key_definition("EventName", ParserMapKeySchema::String),
        );

    KqlParser::parse_with_options(query, options)
}

pub(crate) fn parse_logs_export_request_with_pipeline(
    pipeline: &PipelineExpression,
    logs: ExportLogsServiceRequest,
) -> Result<(ExportLogsServiceRequest, ExportLogsServiceRequest), BoxError> {
    let mut as_pb = BytesMut::with_capacity(1024);
    logs.encode(&mut as_pb).unwrap();

    let (keep, dropped) =
        process_protobuf_otlp_export_logs_service_request_using_pipeline_expression(
            pipeline,
            data_engine_recordset::RecordSetEngineDiagnosticLevel::Info,
            &as_pb[..],
        )?;

    let keep_requests = ExportLogsServiceRequest::decode(&keep[..])?;
    let dropped_requests = ExportLogsServiceRequest::decode(&dropped[..])?;

    Ok((keep_requests, dropped_requests))
}
