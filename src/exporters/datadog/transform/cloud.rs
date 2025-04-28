// SPDX-License-Identifier: Apache-2.0

use crate::semconv;
use opentelemetry_semantic_conventions::attribute;
use std::str::FromStr;
use crate::otlp::cvattr::{ConvertedAttrMap, ConvertedAttrValue};

#[allow(dead_code)]
const EC2_TAG_PREFIX: &str = "ec2.tag.";
const CLUSTER_TAG_PREFIX: &str = "ec2.tag.kubernetes.io/cluster";

#[derive(Debug, Clone)]
pub(crate) enum CloudProvider {
    Aws,
    Gcp,
    Azure,
}

impl CloudProvider {
    pub(crate) fn hostname_from_attrs(&self, attrs: &ConvertedAttrMap) -> Option<String> {
        match self {
            CloudProvider::Aws => attrs
                .as_ref()
                .get(attribute::HOST_ID)
                .map(|s| s.to_string()),
            CloudProvider::Gcp => {
                // https://github.com/DataDog/opentelemetry-mapping-go/blob/be767393c2c16c4e6252cabd91dc88946c51e8c6/pkg/otlp/attributes/gcp/gcp.go#L33
                todo!()
            }
            CloudProvider::Azure => attrs
                .as_ref()
                .get(attribute::HOST_ID)
                .or_else(|| attrs.as_ref().get(attribute::HOST_NAME))
                .map(|s| s.to_string()),
        }
    }

    pub(crate) fn cluster_name_from_attrs(&self, attrs: &ConvertedAttrMap) -> Option<String> {
        match self {
            CloudProvider::Aws => attrs
                .as_ref()
                .iter()
                .find(|(k, _)| k.starts_with(CLUSTER_TAG_PREFIX))
                .map(|(_, v)| {
                    let s = v.to_string();
                    let sp: Vec<_> = s.split("/").collect();
                    sp[2].to_string()
                }),
            CloudProvider::Gcp => {
                todo!()
            }
            CloudProvider::Azure => {
                todo!()
            }
        }
    }
}

#[allow(dead_code)]
pub(crate) struct CloudProviderParseError(String);

impl FromStr for CloudProvider {
    type Err = CloudProviderParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "aws" => Ok(CloudProvider::Aws),
            "gcp" => Ok(CloudProvider::Gcp),
            "azure" => Ok(CloudProvider::Azure),
            _ => Err(CloudProviderParseError(format!(
                "unknown cloud provider:{}",
                s
            ))),
        }
    }
}

pub(crate) fn provider_from_attrs(attrs: &ConvertedAttrMap) -> Option<CloudProvider> {
    if let Some(cloud) = attrs.as_ref().get(attribute::CLOUD_PROVIDER) {
        if let Ok(c) = cloud.to_string().parse() {
            return Some(c);
        }
    }

    None
}

pub(crate) fn attrs_is_on_aws_fargate(attrs: &ConvertedAttrMap) -> bool {
    attrs
        .as_ref()
        .get(attribute::AWS_ECS_LAUNCHTYPE)
        .and_then(|v| match v {
            ConvertedAttrValue::String(s) => Some(s),
            _ => None,
        })
        .is_some_and(|s| s == semconv::cloud::AWS_ECS_LAUNCHTYPE_FARGATE)
}
