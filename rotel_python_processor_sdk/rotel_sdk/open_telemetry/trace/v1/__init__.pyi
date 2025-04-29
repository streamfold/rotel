from rotel_sdk.open_telemetry.common.v1 import KeyValue
from rotel_sdk.open_telemetry.resource.v1 import Resource, InstrumentationScope
from typing import Optional


class ResourceSpans:
    """
    A collection of ScopeSpans from a Resource.
    """
    resource: Optional[Resource]
    """
    The resource for the spans in this message.
    If this field is not set then no resource info is known.
    """
    scope_spans: list[ScopeSpans]
    """
    A list of ScopeSpans that originate from a resource.
    """
    schema_url: str
    """
    The Schema URL, if known. This is the identifier of the Schema that the resource data
    is recorded in. Notably, the last part of the URL path is the version number of the
    schema: http\[s\]://server\[:port\]/path/<version>. To learn more about Schema URL see
    <https://opentelemetry.io/docs/specs/otel/schemas/#schema-url>
    This schema_url applies to the data in the "resource" field. It does not apply
    to the data in the "scope_spans" field which have their own schema_url field.
    """


class ScopeSpans:
    """
    A collection of Spans produced by an InstrumentationScope.
    """
    scope: Optional[InstrumentationScope]
    """
    The instrumentation scope information for the spans in this message.
    Semantically when InstrumentationScope isn't set, it is equivalent with
    an empty instrumentation scope name (unknown).
    """
    spans: list[Span]
    """
    A list of Spans that originate from an instrumentation scope.
    """
    schema_url: str
    """
    The Schema URL, if known. This is the identifier of the Schema that the span data
    is recorded in. Notably, the last part of the URL path is the version number of the
    schema: http\[s\]://server\[:port\]/path/<version>. To learn more about Schema URL see
    <https://opentelemetry.io/docs/specs/otel/schemas/#schema-url>
    This schema_url applies to all spans and span events in the "spans" field.
    """


class Span:
    trace_id: bytes
    """
    A unique identifier for a trace. All spans from the same trace share
    the same `trace_id`. The ID is a 16-byte array. An ID with all zeroes OR
    of length other than 16 bytes is considered invalid (empty string in OTLP/JSON
    is zero-length and thus is also invalid).
    
    This field is required. 
    """
    span_id: bytes
    """
    A unique identifier for a span within a trace, assigned when the span
    is created. The ID is an 8-byte array. An ID with all zeroes OR of length
    other than 8 bytes is considered invalid (empty string in OTLP/JSON
    is zero-length and thus is also invalid).
    
    This field is required.
    """
    trace_state: str
    parent_span_id: bytes
    flags: int
    name: str
    kind: int
    start_time_unix_nano: int
    end_time_unix_nano: int
    attributes: list[KeyValue]
    dropped_attributes_count: int
    events: list[Event]
    dropped_events_count: int
    links: List[Link]
    dropped_links_count: int
    status: Optional[Status]
