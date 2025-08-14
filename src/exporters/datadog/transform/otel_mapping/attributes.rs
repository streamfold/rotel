// SPDX-License-Identifier: Apache-2.0

use crate::otlp::cvattr::ConvertedAttrMap;
use crate::semconv::containers::CONTAINER_IMAGE_TAG;
use opentelemetry_semantic_conventions::attribute;
use std::collections::HashMap;
use std::sync::LazyLock;

pub const CUSTOM_CONTAINER_TAG_PREFIX: &str = "datadog.container.tag.";

pub static CONTAINER_MAPPINGS: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();

        // Containers
        m.insert(attribute::CONTAINER_ID, "container_id");
        m.insert(attribute::CONTAINER_NAME, "container_name");
        m.insert(attribute::CONTAINER_IMAGE_NAME, "image_name");
        m.insert(CONTAINER_IMAGE_TAG, "image_tag");
        m.insert(attribute::CONTAINER_RUNTIME, "runtime");

        // Cloud conventions
        // https://www.datadoghq.com/blog/tagging-best-practices/
        m.insert(attribute::CLOUD_PROVIDER, "cloud_provider");
        m.insert(attribute::CLOUD_REGION, "region");
        m.insert(attribute::CLOUD_AVAILABILITY_ZONE, "zone");

        // ECS conventions
        // https://github.com/DataDog/datadog-agent/blob/e081bed/pkg/tagger/collectors/ecs_extract.go
        m.insert(attribute::AWS_ECS_TASK_FAMILY, "task_family");
        m.insert(attribute::AWS_ECS_TASK_ARN, "task_arn");
        m.insert(attribute::AWS_ECS_CLUSTER_ARN, "ecs_cluster_name");
        m.insert(attribute::AWS_ECS_TASK_REVISION, "task_version");
        m.insert(attribute::AWS_ECS_CONTAINER_ARN, "ecs_container_name");

        // Kubernetes resource name (via semantic conventions)
        // https://github.com/DataDog/datadog-agent/blob/e081bed/pkg/util/kubernetes/const.go
        m.insert(attribute::K8S_CONTAINER_NAME, "kube_container_name");
        m.insert(attribute::K8S_CLUSTER_NAME, "kube_cluster_name");
        m.insert(attribute::K8S_DEPLOYMENT_NAME, "kube_deployment");
        m.insert(attribute::K8S_REPLICASET_NAME, "kube_replica_set");
        m.insert(attribute::K8S_STATEFULSET_NAME, "kube_stateful_set");
        m.insert(attribute::K8S_DAEMONSET_NAME, "kube_daemon_set");
        m.insert(attribute::K8S_JOB_NAME, "kube_job");
        m.insert(attribute::K8S_CRONJOB_NAME, "kube_cronjob");
        m.insert(attribute::K8S_NAMESPACE_NAME, "kube_namespace");
        m.insert(attribute::K8S_POD_NAME, "pod_name");

        m
    });

pub static HTTP_MAPPINGS: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();

    m.insert(attribute::CLIENT_ADDRESS, "http.client_ip");
    m.insert(
        attribute::HTTP_RESPONSE_BODY_SIZE,
        "http.response.content_length",
    );
    m.insert(attribute::HTTP_RESPONSE_STATUS_CODE, "http.status_code");
    m.insert(
        attribute::HTTP_REQUEST_BODY_SIZE,
        "http.request.content_length",
    );
    m.insert("http.request.header.referrer", "http.referrer");
    m.insert(attribute::HTTP_REQUEST_METHOD, "http.method");
    m.insert(attribute::HTTP_ROUTE, "http.route");
    m.insert(attribute::NETWORK_PROTOCOL_VERSION, "http.version");
    m.insert(attribute::SERVER_ADDRESS, "http.server_name");
    m.insert(attribute::URL_FULL, "http.url");
    m.insert(attribute::USER_AGENT_ORIGINAL, "http.useragent");

    m
});

pub fn container_tags_from_resource_attributes(
    res_attrs: &ConvertedAttrMap,
) -> HashMap<String, String> {
    let mut dd_tags = HashMap::new();

    for (k, v) in res_attrs.as_ref() {
        let vstr = v.to_string();
        if vstr.is_empty() {
            continue;
        }

        if let Some(dd_key) = CONTAINER_MAPPINGS.get(k.as_str()).map(|s| s.to_string()) {
            dd_tags.insert(dd_key, vstr.clone());
            continue;
        }

        if let Some(custom_key) = k
            .strip_prefix(CUSTOM_CONTAINER_TAG_PREFIX)
            .map(|s| s.to_string())
        {
            if !custom_key.is_empty() && !dd_tags.contains_key(&custom_key) {
                dd_tags.insert(custom_key, vstr);
            }
        }
    }

    dd_tags
}

#[cfg(test)]
mod tests {
    use crate::exporters::datadog::transform::otel_mapping::attributes::container_tags_from_resource_attributes;
    use opentelemetry_semantic_conventions::{
        resource, // For resource attributes
    };
    use std::collections::HashMap;

    // For older conventions equivalent to v1.6.1
    mod conventions {
        //pub const ATTRIBUTE_DEPLOYMENT_ENVIRONMENT: &str = "deployment.environment";
        pub const ATTRIBUTE_CONTAINER_IMAGE_TAG: &str = "container.image.tag";
    }

    /*
    #[test]
    fn test_tags_from_attributes() {
        let mut attribute_map = HashMap::new();
        attribute_map.insert(resource::PROCESS_EXECUTABLE_NAME, "otelcol");
        attribute_map.insert(resource::PROCESS_EXECUTABLE_PATH, "/usr/bin/cmd/otelcol");
        attribute_map.insert(resource::PROCESS_COMMAND, "cmd/otelcol");
        attribute_map.insert(
            resource::PROCESS_COMMAND_LINE,
            "cmd/otelcol --config=\"/path/to/config.yaml\"",
        );
        attribute_map.insert(resource::PROCESS_PID, 1);
        attribute_map.insert(resource::PROCESS_OWNER, "root");
        attribute_map.insert(resource::OS_TYPE, "linux");
        attribute_map.insert(resource::K8S_DAEMONSET_NAME, "daemon_set_name");
        attribute_map.insert(resource::AWS_ECS_CLUSTER_ARN, "cluster_arn");
        attribute_map.insert(resource::CONTAINER_RUNTIME, "cro");
        attribute_map.insert("tags.datadoghq.com/service", "service_name");
        attribute_map.insert(conventions::ATTRIBUTE_DEPLOYMENT_ENVIRONMENT, "prod");
        attribute_map.insert(resource::CONTAINER_NAME, "custom");
        attribute_map.insert("datadog.container.tag.custom.team", "otel");
        attribute_map.insert("kube_cronjob", "cron");

        let tags = tags_from_attributes(&attribute_map.into());

        // Verify all expected tags are present
        let expected_tags = vec![
            format!("{}:{}", resource::PROCESS_EXECUTABLE_NAME, "otelcol"),
            format!("{}:{}", resource::OS_TYPE, "linux"),
            format!("{}:{}", "kube_daemon_set", "daemon_set_name"),
            format!("{}:{}", "ecs_cluster_name", "cluster_arn"),
            format!("{}:{}", "service", "service_name"),
            format!("{}:{}", "runtime", "cro"),
            format!("{}:{}", "env", "prod"),
            format!("{}:{}", "container_name", "custom"),
            format!("{}:{}", "custom.team", "otel"),
            format!("{}:{}", "kube_cronjob", "cron"),
        ];

        assert_eq!(tags.len(), expected_tags.len());
        for tag in expected_tags {
            assert!(tags.contains(&tag), "Missing tag: {}", tag);
        }
    }

    #[test]
    fn test_new_deployment_environment_name_convention() {
        let mut attrs = AttributeMap::new();
        attrs.put_str("deployment.environment.name", "staging");

        let tags = tags_from_attributes(&attrs);

        assert_eq!(tags, vec!["env:staging"]);
    }

    #[test]
    fn test_tags_from_attributes_empty() {
        let attrs = AttributeMap::new();

        let tags = tags_from_attributes(&attrs);

        assert!(tags.is_empty());
    }

     */

    #[test]
    fn test_container_tag_from_resource_attributes() {
        // Valid test
        {
            let mut attr_map = HashMap::new();
            attr_map.insert(resource::CONTAINER_NAME, "sample_app");
            attr_map.insert(
                conventions::ATTRIBUTE_CONTAINER_IMAGE_TAG,
                "sample_app_image_tag",
            );
            attr_map.insert(resource::CONTAINER_RUNTIME, "cro");
            attr_map.insert(resource::K8S_CONTAINER_NAME, "kube_sample_app");
            attr_map.insert(resource::K8S_REPLICASET_NAME, "sample_replica_set");
            attr_map.insert(resource::K8S_DAEMONSET_NAME, "sample_daemonset_name");
            attr_map.insert(resource::K8S_POD_NAME, "sample_pod_name");
            attr_map.insert(resource::CLOUD_PROVIDER, "sample_cloud_provider");
            attr_map.insert(resource::CLOUD_REGION, "sample_region");
            attr_map.insert(resource::CLOUD_AVAILABILITY_ZONE, "sample_zone");
            attr_map.insert(resource::AWS_ECS_TASK_FAMILY, "sample_task_family");
            attr_map.insert(resource::AWS_ECS_CLUSTER_ARN, "sample_ecs_cluster_name");
            attr_map.insert(resource::AWS_ECS_CONTAINER_ARN, "sample_ecs_container_name");
            attr_map.insert("datadog.container.tag.custom.team", "otel");

            let container_tags = container_tags_from_resource_attributes(&attr_map.into());

            let mut expected_tags = HashMap::new();
            expected_tags.insert("container_name".to_string(), "sample_app".to_string());
            expected_tags.insert("image_tag".to_string(), "sample_app_image_tag".to_string());
            expected_tags.insert("runtime".to_string(), "cro".to_string());
            expected_tags.insert(
                "kube_container_name".to_string(),
                "kube_sample_app".to_string(),
            );
            expected_tags.insert(
                "kube_replica_set".to_string(),
                "sample_replica_set".to_string(),
            );
            expected_tags.insert(
                "kube_daemon_set".to_string(),
                "sample_daemonset_name".to_string(),
            );
            expected_tags.insert("pod_name".to_string(), "sample_pod_name".to_string());
            expected_tags.insert(
                "cloud_provider".to_string(),
                "sample_cloud_provider".to_string(),
            );
            expected_tags.insert("region".to_string(), "sample_region".to_string());
            expected_tags.insert("zone".to_string(), "sample_zone".to_string());
            expected_tags.insert("task_family".to_string(), "sample_task_family".to_string());
            expected_tags.insert(
                "ecs_cluster_name".to_string(),
                "sample_ecs_cluster_name".to_string(),
            );
            expected_tags.insert(
                "ecs_container_name".to_string(),
                "sample_ecs_container_name".to_string(),
            );
            expected_tags.insert("custom.team".to_string(), "otel".to_string());

            assert_eq!(container_tags, expected_tags);
        }

        // Conventions vs custom test
        {
            let mut attr_map = HashMap::new();
            attr_map.insert(resource::CONTAINER_NAME, "ok");
            attr_map.insert("datadog.container.tag.container_name", "nok");

            let container_tags = container_tags_from_resource_attributes(&attr_map.into());

            let mut expected_tags = HashMap::new();
            expected_tags.insert("container_name".to_string(), "ok".to_string());

            assert_eq!(container_tags, expected_tags);
        }

        // Invalid test
        {
            let mut attr_map = HashMap::new();
            attr_map.insert("empty_string_val", "");
            attr_map.insert("", "empty_string_key");
            attr_map.insert("custom_tag", "example_custom_tag");

            let container_tags = container_tags_from_resource_attributes(&attr_map.into());

            assert!(container_tags.is_empty());
        }

        // Empty test
        {
            let attr_map: HashMap<&str, &str> = HashMap::new();
            let container_tags = container_tags_from_resource_attributes(&attr_map.into());

            assert!(container_tags.is_empty());
        }
    }

    /*
    #[test]
    fn test_origin_id_from_attributes() {
        // Pod UID and container ID
        {
            let mut attributes = AttributeMap::new();
            let mut attr_map = HashMap::new();
            attr_map.insert(resource::CONTAINER_ID, "container_id_goes_here");
            attr_map.insert(resource::K8S_POD_UID, "k8s_pod_uid_goes_here");
            attributes.from_raw(attr_map);

            let origin_id = origin_id_from_attributes(&attributes);

            assert_eq!(origin_id, "container_id://container_id_goes_here");
        }

        // Only container ID
        {
            let mut attributes = AttributeMap::new();
            let mut attr_map = HashMap::new();
            attr_map.insert(resource::CONTAINER_ID, "container_id_goes_here");
            attributes.from_raw(attr_map);

            let origin_id = origin_id_from_attributes(&attributes);

            assert_eq!(origin_id, "container_id://container_id_goes_here");
        }

        // Only pod UID
        {
            let mut attributes = AttributeMap::new();
            let mut attr_map = HashMap::new();
            attr_map.insert(resource::K8S_POD_UID, "k8s_pod_uid_goes_here");
            attributes.from_raw(attr_map);

            let origin_id = origin_id_from_attributes(&attributes);

            assert_eq!(origin_id, "kubernetes_pod_uid://k8s_pod_uid_goes_here");
        }

        // None
        {
            let attributes = AttributeMap::new();

            let origin_id = origin_id_from_attributes(&attributes);

            assert!(origin_id.is_empty());
        }
    }

     */
}
