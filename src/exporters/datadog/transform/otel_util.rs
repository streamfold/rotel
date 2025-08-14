// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::transform::attributes::find_first_in_resource;
use crate::exporters::datadog::transform::attributes::{
    find_key_in_attrlist, find_with_resource_precedence,
};
use crate::exporters::datadog::transform::db_types;
use crate::otlp::cvattr::{ConvertedAttrMap, ConvertedAttrValue};
use crate::semconv::misc::MESSAGING_DESTINATION;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::trace::v1::Span as OTelSpan;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
use opentelemetry_semantic_conventions::attribute;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::LazyLock;

pub const TAG_STATUS_CODE: &str = "http.status_code";
const MAX_RESOURCE_LEN: usize = 5_000;

pub static SPAN_KIND_NAMES: LazyLock<HashMap<SpanKind, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();

    m.insert(SpanKind::Unspecified, "unspecified");
    m.insert(SpanKind::Internal, "internal");
    m.insert(SpanKind::Server, "server");
    m.insert(SpanKind::Client, "client");
    m.insert(SpanKind::Producer, "producer");
    m.insert(SpanKind::Consumer, "consumer");

    m
});

pub fn get_otel_span_type(
    span: &OTelSpan,
    scope: Option<Rc<InstrumentationScope>>,
    res_attrs: &ConvertedAttrMap,
) -> String {
    if let Some(typ) =
        find_with_resource_precedence(&vec!["span.type"], span, scope.clone(), res_attrs, false)
    {
        return typ.to_string();
    }

    match span.kind() {
        SpanKind::Server => "web".to_string(),
        SpanKind::Client => {
            find_with_resource_precedence(&vec![attribute::DB_SYSTEM], span, scope, res_attrs, true)
                .map(|v| v.to_string())
                .map(|s| {
                    db_types::DB_TYPES
                        .get(s.as_str())
                        .copied()
                        .unwrap_or(db_types::DB_TYPE_DB)
                        .to_string()
                })
                .unwrap_or("http".to_string())
        }
        _ => "custom".to_string(),
    }
}

#[allow(deprecated)]
pub fn get_otel_operation_name_v2(span: &OTelSpan) -> String {
    let find_attr = |keys: &Vec<&str>, _normalize: bool| -> Option<ConvertedAttrValue> {
        for key in keys {
            let attr = find_key_in_attrlist(key, &span.attributes);
            if attr.is_some() {
                return attr;
            }
        }
        None
    };

    if let Some(op_name) = find_attr(&vec!["operation.name"], false) {
        return op_name.to_string();
    }

    let is_client = span.kind() == SpanKind::Client;
    let is_server = span.kind() == SpanKind::Server;

    // http
    if find_attr(
        &vec![attribute::HTTP_REQUEST_METHOD, attribute::HTTP_METHOD],
        false,
    )
    .is_some()
    {
        return if is_server {
            "http.server.request".to_string()
        } else {
            "http.client.request".to_string()
        };
    }

    // database
    if is_client {
        if let Some(db) = find_attr(&vec![attribute::DB_SYSTEM], true) {
            return format!("{}.query", db);
        }
    }

    // messaging
    let msg_sys = find_attr(&vec![attribute::MESSAGING_SYSTEM], true);
    let msg_op = find_attr(&vec![attribute::MESSAGING_OPERATION_NAME], true);
    if msg_sys.is_some() && msg_op.is_some() {
        match span.kind() {
            SpanKind::Server | SpanKind::Client | SpanKind::Producer | SpanKind::Consumer => {
                return format!("{}.{}", msg_sys.unwrap(), msg_op.unwrap());
            }
            _ => {}
        }
    }

    // RPC & AWS
    let rpc_value = find_attr(&vec![attribute::RPC_SYSTEM], true).map(|cv| cv.to_string());
    let is_aws = rpc_value.as_ref().is_some_and(|v| v == "aws-api");
    if is_aws && is_client {
        if let Some(service) = find_attr(&vec![attribute::RPC_SERVICE], true) {
            return format!("aws.{}.request", service);
        }
        return "aws.client.request".to_string();
    }

    // RPC client
    if rpc_value.is_some() {
        if is_client {
            return format!("{}.client.request", rpc_value.unwrap());
        }
        if is_server {
            return format!("{}.server.request", rpc_value.unwrap());
        }
    }

    // FAAS client
    if is_client {
        let provider = find_attr(&vec![attribute::FAAS_INVOKED_PROVIDER], true);
        let invoked_name = find_attr(&vec![attribute::FAAS_INVOKED_NAME], true);
        if provider.is_some() && invoked_name.is_some() {
            return format!("{}.{}.invoke", provider.unwrap(), invoked_name.unwrap());
        }
    }

    // FAAS server
    if is_server {
        if let Some(trigger) = find_attr(&vec![attribute::FAAS_TRIGGER], true) {
            return format!("{}.invoke", trigger);
        }
    }

    // GraphQL
    if find_attr(&vec!["graphql.operation.type"], true).is_some() {
        return "graphql.server.request".to_string();
    }

    // check for generic http server/client
    if is_server {
        if let Some(protocol) = find_attr(&vec!["network.protocol.name"], true) {
            return format!("{}.server.request", protocol);
        }
        return "server.request".to_string();
    } else if is_client {
        if let Some(protocol) = find_attr(&vec!["network.protocol.name"], true) {
            return format!("{}.client.request", protocol);
        }
        return "client.request".to_string();
    }

    if span.kind() != SpanKind::Unspecified {
        return span.kind().as_str_name().to_string();
    }

    SpanKind::Internal.as_str_name().to_string()
}

pub fn get_otel_resource_v2(
    span: &OTelSpan,
    scope: Option<Rc<InstrumentationScope>>,
    res_attrs: &ConvertedAttrMap,
) -> String {
    let mut resource = get_resource_name_unchecked(span, scope, res_attrs);
    resource.truncate(MAX_RESOURCE_LEN);

    resource
}

#[allow(deprecated)]
fn get_resource_name_unchecked(
    span: &OTelSpan,
    scope: Option<Rc<InstrumentationScope>>,
    res_attrs: &ConvertedAttrMap,
) -> String {
    let find_attr = |keys: &Vec<&str>, normalize: bool| -> Option<ConvertedAttrValue> {
        find_with_resource_precedence(keys, span, scope.clone(), res_attrs, normalize)
    };

    if let Some(res_name) = find_attr(&vec!["resource.name"], false) {
        return res_name.to_string();
    }

    if let Some(method) = find_attr(
        &vec![attribute::HTTP_REQUEST_METHOD, attribute::HTTP_METHOD],
        false,
    ) {
        let mut method_name = method.to_string();
        if method_name == *"_OTHER" {
            method_name = "HTTP".to_string();
        }

        if span.kind() == SpanKind::Server {
            if let Some(route) = find_attr(&vec![attribute::HTTP_ROUTE], false) {
                return format!("{} {}", method_name, route);
            }
        }

        return method_name;
    }

    if let Some(msg_op) = find_attr(&vec![attribute::MESSAGING_OPERATION_NAME], false) {
        if let Some(dest) = find_attr(
            &vec![MESSAGING_DESTINATION, attribute::MESSAGING_DESTINATION_NAME],
            false,
        ) {
            return format!("{} {}", msg_op, dest);
        }
        return msg_op.to_string();
    }

    if let Some(rpc) = find_attr(&vec![attribute::RPC_METHOD], false) {
        if let Some(svc) = find_attr(&vec![attribute::RPC_SERVICE], false) {
            return format!("{} {}", rpc, svc);
        }
        return rpc.to_string();
    }

    if let Some(gql_op) = find_attr(&vec![attribute::GRAPHQL_OPERATION_TYPE], false) {
        if let Some(gql_name) = find_attr(&vec![attribute::GRAPHQL_OPERATION_NAME], false) {
            return format!("{} {}", gql_op, gql_name);
        }
        return gql_op.to_string();
    }

    if find_attr(&vec![attribute::DB_SYSTEM], false).is_some() {
        if let Some(db_query) = find_attr(&vec![attribute::DB_QUERY_TEXT], false) {
            return db_query.to_string();
        }
        // Fallback to the deprecated attribute
        if let Some(db_stmt) = find_attr(&vec![attribute::DB_STATEMENT], false) {
            return db_stmt.to_string();
        }
    }

    span.name.clone()
}

pub fn get_otel_service(
    span: &OTelSpan,
    scope: Option<Rc<InstrumentationScope>>,
    res_attrs: &ConvertedAttrMap,
    normalize: bool,
) -> String {
    let svc = find_with_resource_precedence(
        &vec![attribute::SERVICE_NAME],
        span,
        scope,
        res_attrs,
        false,
    )
    .map(|f| f.to_string())
    .unwrap_or("otlpresourcenoservicename".to_string());

    if normalize {
        // todo
    }

    svc
}

pub fn span_kind_name(kind: SpanKind) -> String {
    SPAN_KIND_NAMES
        .get(&kind)
        .copied()
        .map(|s| s.to_string())
        .unwrap_or("unspecified".to_string())
}

#[allow(deprecated)]
pub fn status_code(span: &OTelSpan) -> Option<u32> {
    if let Some(code) = find_key_in_attrlist(attribute::HTTP_RESPONSE_STATUS_CODE, &span.attributes)
    {
        if let Ok(code) = code.to_string().parse() {
            return Some(code);
        }
    }
    // Check against the deprecated status code in case
    if let Some(code) = find_key_in_attrlist(attribute::HTTP_STATUS_CODE, &span.attributes) {
        if let Ok(code) = code.to_string().parse() {
            return Some(code);
        }
    }

    None
}

// Falls back to deprecated environment name
#[allow(deprecated)]
pub fn get_otel_env(res_attributes: &ConvertedAttrMap) -> String {
    find_first_in_resource(
        res_attributes,
        vec![
            attribute::DEPLOYMENT_ENVIRONMENT_NAME,
            attribute::DEPLOYMENT_ENVIRONMENT,
        ],
        true,
    )
}

#[cfg(test)]
mod tests {
    use crate::exporters::datadog::transform::otel_util::{
        get_otel_resource_v2, get_otel_service, get_otel_span_type,
    };
    use crate::otlp::cvattr::ConvertedAttrMap;
    use crate::semconv::db_system::{CASSANDRA, COUCHDB, ELASTICSEARCH, OPENSEARCH, POSTGRESQL};
    use opentelemetry_proto::tonic::trace::v1::Span;
    use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
    #[allow(deprecated)]
    use opentelemetry_semantic_conventions::attribute::{
        DB_QUERY_TEXT, DB_STATEMENT, DB_SYSTEM, HTTP_METHOD, HTTP_ROUTE,
    };
    use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
    use std::collections::HashMap;
    use utilities::otlp::{FakeOTLP, string_attr};

    // Constants for span types matching the Go file
    const MAX_SERVICE_LEN: usize = 100;
    const DEFAULT_SERVICE_NAME: &str = "unnamed-service";
    const SPAN_TYPE_SQL: &str = "sql";
    const SPAN_TYPE_REDIS: &str = "redis";
    const SPAN_TYPE_MEMCACHED: &str = "memcached";
    const SPAN_TYPE_ELASTICSEARCH: &str = "elasticsearch";
    const SPAN_TYPE_OPENSEARCH: &str = "opensearch";
    const SPAN_TYPE_CASSANDRA: &str = "cassandra";
    const SPAN_TYPE_DB: &str = "db";

    #[test]
    fn test_get_otel_span_type() {
        struct TestCase<'a> {
            name: &'a str,
            span_kind: SpanKind,
            rattrs: Vec<(&'a str, &'a str)>,
            expected: &'a str,
        }

        // Define test cases similar to the Go tests
        let test_cases = vec![
            TestCase {
                name: "override with span.type attr",
                span_kind: SpanKind::Internal,
                rattrs: vec![("span.type", "my-type")],
                expected: "my-type",
            },
            TestCase {
                name: "web span",
                span_kind: SpanKind::Server,
                rattrs: vec![],
                expected: "web",
            },
            TestCase {
                name: "redis span",
                span_kind: SpanKind::Client,
                rattrs: vec![(DB_SYSTEM, "redis")],
                expected: SPAN_TYPE_REDIS,
            },
            TestCase {
                name: "memcached span",
                span_kind: SpanKind::Client,
                rattrs: vec![(DB_SYSTEM, "memcached")],
                expected: SPAN_TYPE_MEMCACHED,
            },
            TestCase {
                name: "sql db client span",
                span_kind: SpanKind::Client,
                rattrs: vec![(DB_SYSTEM, POSTGRESQL)],
                expected: SPAN_TYPE_SQL,
            },
            TestCase {
                name: "elastic db client span",
                span_kind: SpanKind::Client,
                rattrs: vec![(DB_SYSTEM, ELASTICSEARCH)],
                expected: SPAN_TYPE_ELASTICSEARCH,
            },
            TestCase {
                name: "opensearch db client span",
                span_kind: SpanKind::Client,
                rattrs: vec![(DB_SYSTEM, OPENSEARCH)],
                expected: SPAN_TYPE_OPENSEARCH,
            },
            TestCase {
                name: "cassandra db client span",
                span_kind: SpanKind::Client,
                rattrs: vec![(DB_SYSTEM, CASSANDRA)],
                expected: SPAN_TYPE_CASSANDRA,
            },
            TestCase {
                name: "other db client span",
                span_kind: SpanKind::Client,
                rattrs: vec![(DB_SYSTEM, COUCHDB)],
                expected: SPAN_TYPE_DB,
            },
            TestCase {
                name: "http client span",
                span_kind: SpanKind::Client,
                rattrs: vec![],
                expected: "http",
            },
            TestCase {
                name: "other custom span",
                span_kind: SpanKind::Internal,
                rattrs: vec![],
                expected: "custom",
            },
        ];

        // Run each test case
        for tc in test_cases {
            println!("Running test: {}", tc.name);

            let span = create_test_span(tc.span_kind);
            let resource = create_resattrs_map(&tc.rattrs);

            let actual = get_otel_span_type(&span, None, &resource);
            assert_eq!(tc.expected, actual, "Test case: {}", tc.name);
        }
    }

    #[test]
    fn test_get_otel_service() {
        struct TestCase<'a> {
            name: &'a str,
            rattrs: Vec<(String, String)>,
            normalize: bool,
            expected: String,
        }

        // Define test cases similar to the Go tests
        let test_cases = vec![
            TestCase {
                name: "service not set",
                rattrs: vec![],
                normalize: false,
                expected: "otlpresourcenoservicename".to_string(),
            },
            TestCase {
                name: "normal service",
                rattrs: vec![(SERVICE_NAME.to_string(), "svc".to_string())],
                normalize: false,
                expected: "svc".to_string(),
            },
            TestCase {
                // todo
                name: "[SKIP] truncate long service",
                rattrs: vec![(SERVICE_NAME.to_string(), "a".repeat(MAX_SERVICE_LEN + 1))],
                normalize: true,
                expected: "a".repeat(MAX_SERVICE_LEN),
            },
            TestCase {
                // todo
                name: "[SKIP] invalid service",
                rattrs: vec![(SERVICE_NAME.to_string(), "%$^".to_string())],
                normalize: true,
                expected: DEFAULT_SERVICE_NAME.to_string(),
            },
        ];

        // Run each test case
        for tc in test_cases {
            if let Some(rem) = tc.name.strip_prefix("[SKIP] ") {
                println!("Skipping test: {}", rem);
                continue;
            }
            println!("Running test: {}", tc.name);

            let resource = create_resattrs_map(&tc.rattrs);

            let span = create_test_span(SpanKind::Internal);
            let actual = get_otel_service(&span, None, &resource, tc.normalize);
            assert_eq!(tc.expected, actual, "Test case: {}", tc.name);
        }
    }

    #[test]
    fn test_get_otel_resource() {
        struct TestCase<'a> {
            name: &'a str,
            span_attributes: HashMap<&'a str, &'a str>,
            resource_attributes: HashMap<&'a str, &'a str>,
            expected_v2: &'a str,
        }

        let tests = vec![
            TestCase {
                name: "resource not set",
                span_attributes: HashMap::new(),
                resource_attributes: HashMap::new(),
                expected_v2: "span_name",
            },
            TestCase {
                name: "normal resource",
                span_attributes: vec![("resource.name", "res")].into_iter().collect(),
                resource_attributes: HashMap::new(),
                expected_v2: "res",
            },
            TestCase {
                name: "HTTP request method resource",
                span_attributes: vec![("http.request.method", "GET")].into_iter().collect(),
                resource_attributes: HashMap::new(),
                expected_v2: "GET",
            },
            #[allow(deprecated)]
            TestCase {
                name: "HTTP method and route resource",
                span_attributes: vec![(HTTP_METHOD, "GET"), (HTTP_ROUTE, "/")]
                    .into_iter()
                    .collect(),
                resource_attributes: HashMap::new(),
                expected_v2: "GET",
            },
            TestCase {
                name: "GraphQL with type and name",
                span_attributes: vec![
                    ("graphql.operation.type", "query"),
                    ("graphql.operation.name", "myQuery"),
                ]
                .into_iter()
                .collect(),
                resource_attributes: HashMap::new(),
                expected_v2: "query myQuery",
            },
            #[allow(deprecated)]
            TestCase {
                name: "SQL statement resource",
                span_attributes: HashMap::new(),
                resource_attributes: vec![
                    (DB_SYSTEM, "mysql"),
                    (DB_STATEMENT, "SELECT * FROM table WHERE id = 12345"),
                ]
                .into_iter()
                .collect(),
                expected_v2: "SELECT * FROM table WHERE id = 12345",
            },
            TestCase {
                name: "Redis command resource",
                span_attributes: HashMap::new(),
                resource_attributes: vec![(DB_SYSTEM, "redis"), (DB_QUERY_TEXT, "SET key value")]
                    .into_iter()
                    .collect(),
                expected_v2: "SET key value",
            },
        ];

        for test in tests.iter() {
            let mut span = create_test_span(SpanKind::Internal);
            span.name = "span_name".to_string();
            span.attributes.clear();
            for (k, v) in &test.span_attributes {
                span.attributes.push(string_attr(k, v));
            }

            let res_attrs: HashMap<&str, String> = test
                .resource_attributes
                .iter()
                .map(|(k, v)| (*k, v.to_string()))
                .collect();

            let actual_v2 = get_otel_resource_v2(&span, None, &res_attrs.into());

            assert_eq!(
                actual_v2, test.expected_v2,
                "failed test '{}', expected_v2 '{}', got '{}'",
                test.name, test.expected_v2, actual_v2
            );
        }
    }

    //
    // Helpers
    //

    // Create a test span with the given kind
    fn create_test_span(kind: SpanKind) -> Span {
        let mut span = FakeOTLP::trace_spans(1).pop().unwrap();
        span.kind = kind.into();
        span
    }

    // Create a test resource with the given attributes
    fn create_resattrs_map<T: AsRef<str>>(attrs: &[(T, T)]) -> ConvertedAttrMap {
        let mut resource_attrs = HashMap::new();
        for (k, v) in attrs {
            resource_attrs.insert(k.as_ref().to_string(), v.as_ref().to_string());
        }
        resource_attrs.into()
    }
}
