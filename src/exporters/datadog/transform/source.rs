// SPDX-License-Identifier: Apache-2.0

use crate::exporters::datadog::transform::cloud;
use crate::exporters::datadog::transform::k8s;
use crate::semconv;
use opentelemetry_semantic_conventions::attribute;
use std::fmt::{Display, Formatter};
use crate::otlp::cvattr::ConvertedAttrMap;

#[derive(Clone, Debug)]
pub(crate) struct Source {
    pub(crate) kind: SourceKind,
    pub(crate) identifier: String,
}

impl Source {
    pub(crate) fn from_hostname(identifier: String) -> Self {
        Self {
            kind: SourceKind::Hostname,
            identifier,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SourceKind {
    AWSECSFargateKind,
    Hostname,
}

impl Display for SourceKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceKind::AWSECSFargateKind => write!(f, "task_arn"),
            SourceKind::Hostname => write!(f, "host"),
        }
    }
}

pub(crate) fn from_attributes(attrs: &ConvertedAttrMap) -> Option<Source> {
    if cloud::attrs_is_on_aws_fargate(attrs) {
        if let Some(task_arn) = attrs.as_ref().get(attribute::AWS_ECS_TASK_ARN) {
            return Some(Source {
                kind: SourceKind::AWSECSFargateKind,
                identifier: task_arn.to_string(),
            });
        }
    }

    if let Some(hostname) = hostname_from_attrs(attrs) {
        return Some(Source {
            kind: SourceKind::Hostname,
            identifier: hostname,
        });
    }

    None
}

fn hostname_from_attrs(attrs: &ConvertedAttrMap) -> Option<String> {
    let unchecked = unchecked_hostname_from_attrs(attrs)?;

    // Make sure we ignore localhost variants
    match unchecked.clone().as_str() {
        "0.0.0.0" => None,
        "127.0.0.1" => None,
        "localhost" => None,
        "localhost.localdomain" => None,
        "localhost6.localdomain6" => None,
        "ip6-localhost" => None,
        _ => Some(unchecked),
    }
}

// Walk through a precedence order of hostname lookups
fn unchecked_hostname_from_attrs(attrs: &ConvertedAttrMap) -> Option<String> {
    if let Some(host) = attrs.as_ref().get(semconv::misc::HOST) {
        return Some(host.to_string());
    }

    if let Some(custom_host) = attrs.as_ref().get(semconv::misc::DATADOG_HOST_NAME) {
        return Some(custom_host.to_string());
    }

    if cloud::attrs_is_on_aws_fargate(attrs) {
        // We check for a source on fargate already using the task arn
        return None;
    }

    if let Some(provider) = cloud::provider_from_attrs(attrs) {
        return provider.hostname_from_attrs(attrs);
    }

    if let Some(k8s_name) = k8s::hostname_from_attrs(attrs) {
        return Some(k8s_name);
    }

    if let Some(host_id) = attrs.as_ref().get(attribute::HOST_ID) {
        return Some(host_id.to_string());
    }

    if let Some(host_name) = attrs.as_ref().get(attribute::HOST_NAME) {
        return Some(host_name.to_string());
    }

    None
}
