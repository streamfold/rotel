from rotel_sdk.open_telemetry.common.v1 import InstrumentationScope, KeyValue
from rotel_sdk.open_telemetry.resource.v1 import Resource
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
    """
    A Span represents a single operation performed by a single component of the system.
    """
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
    """
    trace_state conveys information about request position in multiple distributed tracing graphs.
    It is a trace_state in w3c-trace-context format: <https://www.w3.org/TR/trace-context/#tracestate-header>
    See also <https://github.com/w3c/distributed-tracing> for more details about this field.
    """
    parent_span_id: bytes
    """
    The `span_id` of this span's parent span. If this is a root span, then this
    field must be empty. The ID is an 8-byte array.
    """
    flags: int
    """
    Flags, a bit field.
    
    Bits 0-7 (8 least significant bits) are the trace flags as defined in W3C Trace
    Context specification. To read the 8-bit W3C trace flag, use
    `flags & SPAN_FLAGS_TRACE_FLAGS_MASK`.
    
    See <https://www.w3.org/TR/trace-context-2/#trace-flags> for the flag definitions.
    
    Bits 8 and 9 represent the 3 states of whether a span's parent
    is remote. The states are (unknown, is not remote, is remote).
    To read whether the value is known, use `(flags & SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK) != 0`.
    To read whether the span is remote, use `(flags & SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK) != 0`.
    
    When creating span messages, if the message is logically forwarded from another source
    with an equivalent flags fields (i.e., usually another OTLP span message), the field SHOULD
    be copied as-is. If creating from a source that does not have an equivalent flags field
    (such as a runtime representation of an OpenTelemetry span), the high 22 bits MUST
    be set to zero.
    Readers MUST NOT assume that bits 10-31 (22 most significant bits) will be zero.
    
    Optional.
    """
    name: str
    """
    A description of the span's operation.
    
    For example, the name can be a qualified method name or a file name
    and a line number where the operation is called. A best practice is to use
    the same display name at the same call point in an application.
    This makes it easier to correlate spans in different traces.
    
    This field is semantically required to be set to non-empty string.
    Empty value is equivalent to an unknown span name.
    
    This field is required.
    """
    kind: int
    """
    Distinguishes between spans generated in a particular context. For example,
    two spans with the same name may be distinguished using `CLIENT` (caller)
    and `SERVER` (callee) to identify queueing latency associated with the span.
    """
    start_time_unix_nano: int
    """
    start_time_unix_nano is the start time of the span. On the client side, this is the time
    kept by the local machine where the span execution starts. On the server side, this
    is the time when the server's application handler starts running.
    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    
    This field is semantically required and it is expected that end_time >= start_time.
    """
    end_time_unix_nano: int
    """
    end_time_unix_nano is the end time of the span. On the client side, this is the time
    kept by the local machine where the span execution ends. On the server side, this
    is the time when the server application handler stops running.
    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
    
    This field is semantically required and it is expected that end_time >= start_time.
    """
    attributes: list[KeyValue]
    """
    attributes is a collection of key/value pairs. Note, global attributes
    like server name can be set using the resource API. Examples of attributes:
    
         "/http/user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
         "/http/server_latency": 300
         "example.com/myattribute": true
         "example.com/score": 10.239
    
    The OpenTelemetry API specification further restricts the allowed value types:
    <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/common/README.md#attribute>
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key).
    """
    dropped_attributes_count: int
    """
    dropped_attributes_count is the number of attributes that were discarded. Attributes
    can be discarded because their keys are too long or because there are too many
    attributes. If this value is 0, then no attributes were dropped.
    """
    events: list[Event]
    """
    events is a collection of Event items.
    """
    dropped_events_count: int
    """
    dropped_events_count is the number of dropped events. If the value is 0, then no
    events were dropped.
    """
    links: List[Link]
    """
    links is a collection of Links, which are references from this span to a span
    in the same or different trace.
    """
    dropped_links_count: int
    """
    dropped_links_count is the number of dropped links after the maximum size was
    enforced. If this value is 0, then no links were dropped. 
    """
    status: Optional[Status]
    """
    An optional final status for this span. Semantically when Status isn't set, it means
    span's status code is unset, i.e. assume STATUS_CODE_UNSET (code = 0).
    """


class Event:
    """
    Event is a time-stamped annotation of the span, consisting of user-supplied
    text description and key-value pairs.
    """
    time_unix_nano: int
    """
    time_unix_nano is the time the event occurred.
    """
    name: str
    """
    name of the event.
    This field is semantically required to be set to non-empty string.
    """
    attributes: List[KeyValue]
    """
    attributes is a collection of attribute key/value pairs on the event.
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key). 
    """
    dropped_attributes_count: int
    """
    dropped_attributes_count is the number of dropped attributes. If the value is 0,
    then no attributes were dropped.
    """


class Link:
    """
    A pointer from the current span to another span in the same trace or in a
    different trace. For example, this can be used in batching operations,
    where a single batch handler processes multiple requests from different
    traces or when the handler receives a request from a different project.
    """
    trace_id: bytes
    """
    A unique identifier of a trace that this linked span is part of. The ID is a
    16-byte array.
    """
    span_id: bytes
    """
    A unique identifier for the linked span. The ID is an 8-byte array.
    """
    trace_state: str
    """
    The trace_state associated with the link.
    """
    attributes: List[KeyValue]
    """
    attributes is a collection of attribute key/value pairs on the link.
    Attribute keys MUST be unique (it is not allowed to have more than one
    attribute with the same key).
    """
    dropped_attributes_count: int
    """
    dropped_attributes_count is the number of dropped attributes. If the value is 0,
    then no attributes were dropped.
    """
    flags: int
    """
    Flags, a bit field.
    
    Bits 0-7 (8 least significant bits) are the trace flags as defined in W3C Trace
    Context specification. To read the 8-bit W3C trace flag, use
    `flags & SPAN_FLAGS_TRACE_FLAGS_MASK`.
    
    See <https://www.w3.org/TR/trace-context-2/#trace-flags> for the flag definitions.
    
    Bits 8 and 9 represent the 3 states of whether the link is remote.
    The states are (unknown, is not remote, is remote).
    To read whether the value is known, use `(flags & SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK) != 0`.
    To read whether the link is remote, use `(flags & SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK) != 0`.
    
    Readers MUST NOT assume that bits 10-31 (22 most significant bits) will be zero.
    When creating new spans, bits 10-31 (most-significant 22-bits) MUST be zero.
    
    Optional
    """


class Status:
    """
    The Status type defines a logical error model that is suitable for different
    programming environments, including REST APIs and RPC APIs.
    """
    message: str
    """
    A developer-facing human readable error message.
    """
    code: int
    """
    The status code.
    """
