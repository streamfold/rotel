from rotel_sdk.open_telemetry.common.v1 import InstrumentationScope, KeyValue, AnyValue
from rotel_sdk.open_telemetry.resource.v1 import Resource
from typing import Optional


class ResourceLogs:
    """
    A collection of ScopeLogs from a Resource.
    """
    resource: Optional[Resource]
    """
    The resource for the logs in this message.
    If this field is not set then resource info is unknown. 
    """
    scope_logs: list[ScopeLogs]
    """
    A list of ScopeLogs that originate from a resource.
    """
    schema_url: string
    """
    The Schema URL, if known. This is the identifier of the Schema that the resource data is recorded in. 
    Notably, the last part of the URL path is the version number of the schema: http[s]://server[:port]/path/. \
    To learn more about Schema URL see https://opentelemetry.io/docs/specs/otel/schemas/#schema-url   
    This schema_url applies to the data in the "resource" field. 
    It does not apply to the data in the "scope_logs" field which have their own schema_url field. 
    """


class ScopeLogs:
    """
    A collection of Logs produced by a Scope.
    """
    scope: Optional[InstrumentationScope]
    """
    The instrumentation scope information for the logs in this message. 
    Semantically when InstrumentationScope isn't set, it is equivalent with an empty instrumentation 
    scope name (unknown).
    """
    log_records: list[LogRecord]
    """
    A list of log records.
    """
    schema_url: string
    """
    The Schema URL, if known. This is the identifier of the Schema that the log data is recorded in. 
    Notably, the last part of the URL path is the version number of the schema: http[s]://server[:port]/path/. 
    To learn more about Schema URL see https://opentelemetry.io/docs/specs/otel/schemas/#schema-url   
    This schema_url applies to all logs in the "logs" field. 
    """


class LogRecord:
    """
    A log record according to OpenTelemetry Log Data Model:
    https://github.com/open-telemetry/oteps/blob/main/text/logs/0097-log-data-model.md
    """
    time_unix_nano: int
    """
    time_unix_nano is the time when the event occurred. Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970. 
    Value of 0 indicates unknown or missing timestamp.
    """
    observed_time_unix_nano: int
    """
    Time when the event was observed by the collection system. For events that originate in OpenTelemetry (e. g. using OpenTelemetry Logging SDK) this timestamp is typically set at the generation time and is equal to Timestamp. For events originating externally and collected by OpenTelemetry (e. g. using Collector) this is the time when OpenTelemetry's code observed the event measured by the clock of the OpenTelemetry code. This field MUST be set once the event is observed by OpenTelemetry.
    For converting OpenTelemetry log data to formats that support only one timestamp or when receiving OpenTelemetry log data by recipients that support only one timestamp internally the following logic is recommended:
    Use time_unix_nano if it is present, otherwise use observed_time_unix_nano.
    Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970. Value of 0 indicates unknown or missing timestamp.
    """
    severity_number: int
    """
    Numerical value of the severity, normalized to values described in Log Data Model. [Optional].
    """
    severity_text: str
    """
    The severity text (also known as log level). The original string representation as it is known at the source. [Optional].
    """
    body: Optional[AnyValue]
    """
    A value containing the body of the log record. 
    Can be for example a human-readable string message (including multi-line) describing the event in a free form or it can be a structured data composed of arrays and maps of other values. 
    [Optional].
    """
    attributes: list[KeyValue]
    """
    Additional attributes that describe the specific event occurrence. [Optional]. 
    Attribute keys MUST be unique (it is not allowed to have more than one attribute with the same key).
    """
    dropped_attributes_count: int
    """
    dropped_attributes_count is the number of attributes that were discarded. Attributes
    can be discarded because their keys are too long or because there are too many
    attributes. If this value is 0, then no attributes were dropped.
    """
    flags: int
    """
    Flags, a bit field. 8 least significant bits are the trace flags as defined in W3C Trace Context specification. 
    24 most significant bits are reserved and must be set to 0. Readers must not assume that 24 most significant bits 
    will be zero and must correctly mask the bits when reading 8-bit trace flag 
    (use flags & LOG_RECORD_FLAGS_TRACE_FLAGS_MASK). [Optional].
    """
    trace_id: bytes
    """
    A unique identifier for a trace. All logs from the same trace share the same trace_id. The ID is a 16-byte array. 
    An ID with all zeroes OR of length other than 16 bytes is considered invalid (empty string in OTLP/ JSON is zero-length and thus is also invalid).

    This field is optional.

    The receivers SHOULD assume that the log record is not associated with a trace if any of the following is true:
    - the field is not present,
    - the field contains an invalid value.
    """
    span_id: bytes
    """
    A unique identifier for a span within a trace, assigned when the span is created. The ID is an 8-byte array. 
    An ID with all zeroes OR of length other than 8 bytes is considered invalid (empty string in OTLP/ JSON is zero-length and thus is also invalid).
    
    This field is optional. If the sender specifies a valid span_id then it SHOULD also specify a valid trace_id.
    
    The receivers SHOULD assume that the log record is not associated with a span if any of the following is true:
    - the field is not present,
    - the field contains an invalid value.
    """
    event_name: str
    """
    A unique identifier of event category/type. All events with the same event_name are expected to conform to the same schema for both their attributes and their body.
    Recommended to be fully qualified and short (no longer than 256 characters).
    Presence of event_name on the log record identifies this record as an event.
    [Optional].
    Status: [Development]
    """
