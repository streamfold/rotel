"""
ContextProcessor demonstrates how to pull HTTP/gRPC headers from message metadata
(context) and add them as span attributes. This follows the OTel Collector
pattern where headers/metadata are stored in context by the receiver and processors pull
from context to add attributes.

This processor extracts custom headers from context and adds them as span
attributes following OpenTelemetry semantic conventions (http.request.header.*).

Usage:
    For HTTP, configure the receiver to include metadata:
        ROTEL_OTLP_HTTP_INCLUDE_METADATA=true
        ROTEL_OTLP_HTTP_HEADERS_TO_INCLUDE=my-custom-header

    For gRPC, configure the receiver to include metadata:
        ROTEL_OTLP_GRPC_INCLUDE_METADATA=true
        ROTEL_OTLP_GRPC_HEADERS_TO_INCLUDE=my-custom-header

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
from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs
from rotel_sdk.open_telemetry.metrics.v1 import ResourceMetrics
from rotel_sdk.open_telemetry.request import RequestContext
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans


def _get_header_from_context(
    request_context: Optional[RequestContext], header_name: str
) -> Optional[str]:
    """
    Get a header value from message metadata (context).

    Accesses headers via:
        resource_spans.request_context.get(header_name)

    The pattern:
        - Headers/metadata keys are stored with lowercase keys
        - Works for both HTTP headers and gRPC metadata
    """
    if request_context is not None:
        if isinstance(request_context, RequestContext.HttpContext):
            return request_context.http_context.headers.get(header_name.lower())
        elif isinstance(request_context, RequestContext.GrpcContext):
            return request_context.grpc_context.metadata.get(header_name.lower())
    return None


class ContextProcessor:
    def process_spans(self, resource_spans: ResourceSpans):
        """
        Process ResourceSpans by extracting a custom header from context
        and adding it as a span attribute.

        This function extracts "my-custom-header" from context and adds it as
        a span attribute following OTel semantic convention: http.request.header.*

        Example: If the receiver is configured with:
            ROTEL_OTLP_HTTP_INCLUDE_METADATA=true
            ROTEL_OTLP_HTTP_HEADERS_TO_INCLUDE=my-custom-header

        Or for gRPC:
            ROTEL_OTLP_GRPC_INCLUDE_METADATA=true
            ROTEL_OTLP_GRPC_HEADERS_TO_INCLUDE=my-custom-header

        And the request includes "my-custom-header: test-value-123", then
        this processor will add the attribute
        "http.request.header.my-custom-header" = "test-value-123" to all spans.
        """
        # Header to extract from context
        header_name = "my-custom-header"

        # Get header value from context
        header_value = _get_header_from_context(
            resource_spans.request_context, header_name
        )

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

    def process_metrics(self, resource_metrics: ResourceMetrics):
        """
        Process metrics - add custom header to resource attributes.
        Metrics typically use resource attributes rather than per-metric
        attributes.
        """
        header_name = "my-custom-header"
        header_value = _get_header_from_context(
            resource_metrics.request_context, header_name
        )

        if header_value and resource_metrics.resource:
            attr = KeyValue(
                key=f"http.request.header.{header_name}",
                value=AnyValue(header_value),
            )
            resource_metrics.resource.attributes.append(attr)

    def process_logs(self, resource_logs: ResourceLogs):
        """
        Process logs - add custom header to log record attributes.
        """
        header_name = "my-custom-header"
        header_value = _get_header_from_context(
            resource_logs.request_context, header_name
        )
        if header_value:
            attr = KeyValue(
                key=f"http.request.header.{header_name}",
                value=AnyValue(header_value),
            )

            for scope_logs in resource_logs.scope_logs:
                for log_record in scope_logs.log_records:
                    log_record.attributes.append(attr)
