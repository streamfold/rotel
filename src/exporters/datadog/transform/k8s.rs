// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::transform::cloud;
use opentelemetry_semantic_conventions::attribute;
use crate::exporters::datadog::transform::cv_attributes::ConvertedAttrMap;

pub(crate) fn hostname_from_attrs(attrs: &ConvertedAttrMap) -> Option<String> {
    attrs
        .as_ref()
        .get(attribute::K8S_NODE_NAME)
        .map(|node| node.to_string())
        .map(|node| {
            get_cluster_name(attrs)
                .map(|cluster| format!("{}-{}", node, cluster))
                .unwrap_or(node)
        })
}

fn get_cluster_name(attrs: &ConvertedAttrMap) -> Option<String> {
    if let Some(cluster_name) = attrs.as_ref().get(attribute::K8S_CLUSTER_NAME) {
        return Some(cluster_name.to_string());
    }

    if let Some(provider) = cloud::provider_from_attrs(attrs) {
        if let Some(cluster_name) = provider.cluster_name_from_attrs(attrs) {
            return Some(cluster_name);
        }
    }

    None
}
