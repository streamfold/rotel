"""
ContextProcessor demonstrates how to pull HTTP/gRPC headers from message metadata
(context) and add them as span attributes. This follows the OTel Collector
pattern where headers/metadata are stored in context by the receiver and processors pull
from context to add attributes.

This processor extracts headers like "my-custom-header" from context and adds
them as span attributes following OpenTelemetry semantic conventions
(http.request.header.*).

Usage:
    For HTTP, configure the receiver to include metadata:
        ROTEL_OTLP_HTTP_INCLUDE_METADATA=true
        ROTEL_OTLP_HTTP_HEADERS_TO_INCLUDE=my-custom-header,another-header

    For gRPC, configure the receiver to include metadata:
        ROTEL_OTLP_GRPC_INCLUDE_METADATA=true
        ROTEL_OTLP_GRPC_METADATA_KEYS_TO_INCLUDE=my-custom-header,another-header

    Then use this processor to add those headers as span attributes.

Message metadata is now exposed to Python processors. Processors can
access headers via:
    resource_spans.message_metadata  # Returns dict[str, str] or None

This processor works with both HTTP headers and gRPC metadata - they are
both exposed as the same dictionary format to Python processors.

This processor demonstrates how to extract headers from context and add
them as span attributes following OpenTelemetry semantic conventions.
"""

from typing import Optional

from rotel_sdk.open_telemetry.common.v1 import AnyValue, KeyValue
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans


def _get_header_from_context(
    resource_spans: ResourceSpans, header_name: str
) -> Optional[str]:
    """
    Get a header value from message metadata (context).

    Accesses headers via:
        resource_spans.message_metadata.get(header_name)

    The pattern:
        - ResourceSpans/ResourceMetrics/ResourceLogs have a
          `.message_metadata` property
        - This returns a dict[str, str] (or None if no metadata)
        - Headers/metadata keys are stored with lowercase keys
        - Works for both HTTP headers and gRPC metadata
    """
    if resource_spans.message_metadata:
        return resource_spans.message_metadata.get(header_name.lower())
    return None


def process_spans(resource_spans: ResourceSpans):
    """
    Process ResourceSpans by extracting headers from context and adding as
    span attributes.

    This function extracts "my-custom-header" from context and adds it as a
    span attribute following OTel semantic convention:
    http.request.header.my-custom-header

    Example: If the receiver is configured with:
        ROTEL_OTLP_HTTP_INCLUDE_METADATA=true
        ROTEL_OTLP_HTTP_HEADERS_TO_INCLUDE=my-custom-header
    OR
        ROTEL_OTLP_GRPC_INCLUDE_METADATA=true
        ROTEL_OTLP_GRPC_METADATA_KEYS_TO_INCLUDE=my-custom-header

    And a request includes header/metadata "my-custom-header: example-value", then
    this processor will add the attribute
    "http.request.header.my-custom-header" = "example-value" to all spans.
    """
    # Header to extract from context (configure this based on your needs)
    header_name = "my-custom-header"

    # Get header value from context
    header_value = _get_header_from_context(resource_spans, header_name)

    if header_value:
        # Create attribute following OTel semantic convention
        attr = KeyValue(
            key=f"http.request.header.{header_name}",
            value=AnyValue(header_value),
        )

        # Add attribute to all spans
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                span.attributes.append(attr)

    # Example: You can also add to resource attributes instead:
    # if header_value and resource_spans.resource:
    #     resource_spans.resource.attributes.append(attr)


def process_metrics(resource_metrics):
    """
    Process metrics - add context headers to resource attributes.
    Metrics typically use resource attributes rather than per-metric
    attributes.
    """
    header_name = "my-custom-header"
    header_value = _get_header_from_context(resource_metrics, header_name)

    if header_value and resource_metrics.resource:
        attr = KeyValue(
            key=f"http.request.header.{header_name}",
            value=AnyValue(header_value),
        )
        resource_metrics.resource.attributes.append(attr)


def process_logs(resource_logs):
    """
    Process logs - add context headers to log record attributes.
    """
    header_name = "my-custom-header"
    header_value = _get_header_from_context(resource_logs, header_name)

    if header_value:
        attr = KeyValue(
            key=f"http.request.header.{header_name}",
            value=AnyValue(header_value),
        )

        for scope_logs in resource_logs.scope_logs:
            for log_record in scope_logs.log_records:
                log_record.attributes.append(attr)
