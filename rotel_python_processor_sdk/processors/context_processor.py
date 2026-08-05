"""
ContextProcessor - Generic processor for extracting HTTP/gRPC headers from request
context and adding them as span/log/metric attributes.

This is a reusable library. To use with rotel, create a config file that imports
this module and sets up the specific headers you need.

Usage:
    from context_processor import ContextProcessor, ContextProcessorConfig

    config = ContextProcessorConfig(
        headers=["x-request-id", "x-correlation-id", "traceparent"],
        attribute_pattern="http.request.header.{header}",
    )

    processor = ContextProcessor(config)

    # Then expose module-level functions for rotel:
    def process_spans(resource_spans):
        return processor.process_spans(resource_spans)

    def process_metrics(resource_metrics):
        return processor.process_metrics(resource_metrics)

    def process_logs(resource_logs):
        return processor.process_logs(resource_logs)

For rotel receiver configuration, ensure headers are included:
    HTTP:
        --otlp-http-include-metadata
        --otlp-http-headers-to-include x-request-id,x-correlation-id

    gRPC:
        --otlp-grpc-include-metadata
        --otlp-grpc-headers-to-include x-request-id,x-correlation-id
"""

from typing import List, Optional

from rotel_sdk.open_telemetry.common.v1 import AnyValue, KeyValue
from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs
from rotel_sdk.open_telemetry.metrics.v1 import ResourceMetrics
from rotel_sdk.open_telemetry.request import RequestContext
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans


class ContextProcessorConfig:
    """Configuration for the ContextProcessor."""

    def __init__(
        self,
        headers: List[str] = None,
        attribute_pattern: str = "http.request.header.{header}",
        span_attribute_target: str = "span",
        log_attribute_target: str = "log",
        metric_attribute_target: str = "resource",
    ):
        """
        Initialize the ContextProcessor configuration.

        Args:
            headers: List of header names to extract from request context.
                     Headers are matched case-insensitively.
            attribute_pattern: Pattern for naming attributes. Use {header} as placeholder.
                              Default: "http.request.header.{header}"
            span_attribute_target: Where to add attributes for spans - "span" or "resource"
            log_attribute_target: Where to add attributes for logs - "log" or "resource"
            metric_attribute_target: Where to add attributes for metrics - "resource"
        """
        self.headers = [h.lower() for h in (headers or [])]
        self.attribute_pattern = attribute_pattern

        if span_attribute_target not in ("span", "resource"):
            raise ValueError(
                f"span_attribute_target must be 'span' or 'resource', got '{span_attribute_target}'"
            )
        self.span_attribute_target = span_attribute_target

        if log_attribute_target not in ("log", "resource"):
            raise ValueError(
                f"log_attribute_target must be 'log' or 'resource', got '{log_attribute_target}'"
            )
        self.log_attribute_target = log_attribute_target

        if metric_attribute_target != "resource":
            raise ValueError(
                f"metric_attribute_target must be 'resource', got '{metric_attribute_target}'"
            )
        self.metric_attribute_target = metric_attribute_target


def get_header_from_context(
    request_context: Optional[RequestContext], header_name: str
) -> Optional[str]:
    """
    Get a header value from request context (HTTP headers or gRPC metadata).

    Headers are stored with lowercase keys in both HTTP and gRPC contexts.
    """
    if request_context is not None:
        if isinstance(request_context, RequestContext.HttpContext):
            return request_context.http_context.headers.get(header_name.lower())
        elif isinstance(request_context, RequestContext.GrpcContext):
            return request_context.grpc_context.metadata.get(header_name.lower())
    return None


def get_all_headers_from_context(
    request_context: Optional[RequestContext], header_names: List[str]
) -> dict:
    """
    Get multiple header values from request context.

    Returns a dict of {header_name: value} for headers that exist.
    """
    result = {}
    for header_name in header_names:
        value = get_header_from_context(request_context, header_name)
        if value is not None:
            result[header_name] = value
    return result


class ContextProcessor:
    """
    Processor that extracts headers from request context and adds them as attributes.
    """

    def __init__(self, config: ContextProcessorConfig):
        """
        Initialize the processor with configuration.

        Args:
            config: ContextProcessorConfig specifying which headers to extract
        """
        self.config = config

    def _create_attribute(self, header_name: str, value: str) -> KeyValue:
        """Create a KeyValue attribute for a header."""
        attr_name = self.config.attribute_pattern.format(header=header_name)
        return KeyValue(key=attr_name, value=AnyValue(value))

    def _create_attributes(self, headers: dict) -> List[KeyValue]:
        """Create KeyValue attributes for all extracted headers."""
        return [self._create_attribute(name, value) for name, value in headers.items()]

    def process_spans(self, resource_spans: ResourceSpans):
        """
        Process ResourceSpans by extracting configured headers from context
        and adding them as attributes.
        """
        headers = get_all_headers_from_context(
            resource_spans.request_context, self.config.headers
        )

        if not headers:
            return

        attributes = self._create_attributes(headers)

        if self.config.span_attribute_target == "resource":
            if resource_spans.resource:
                for attr in attributes:
                    resource_spans.resource.attributes.append(attr)
        else:
            for scope_spans in resource_spans.scope_spans:
                for span in scope_spans.spans:
                    for attr in attributes:
                        span.attributes.append(attr)

    def process_metrics(self, resource_metrics: ResourceMetrics):
        """
        Process ResourceMetrics by extracting configured headers from context
        and adding them as resource attributes.
        """
        headers = get_all_headers_from_context(
            resource_metrics.request_context, self.config.headers
        )

        if not headers:
            return

        attributes = self._create_attributes(headers)

        if resource_metrics.resource:
            for attr in attributes:
                resource_metrics.resource.attributes.append(attr)

    def process_logs(self, resource_logs: ResourceLogs):
        """
        Process ResourceLogs by extracting configured headers from context
        and adding them as attributes.
        """
        headers = get_all_headers_from_context(
            resource_logs.request_context, self.config.headers
        )

        if not headers:
            return

        attributes = self._create_attributes(headers)

        if self.config.log_attribute_target == "resource":
            if resource_logs.resource:
                for attr in attributes:
                    resource_logs.resource.attributes.append(attr)
        else:
            for scope_logs in resource_logs.scope_logs:
                for log_record in scope_logs.log_records:
                    for attr in attributes:
                        log_record.attributes.append(attr)
