from rotel_sdk.open_telemetry.resource.v1 import Resource
from rotel_sdk.open_telemetry.common.v1 import InstrumentationScope
from typing import Optional


class ResourceSpans:
    resource: Optional[Resource]
    scope_spans: list[ScopeSpans]
    schema_url: str


class ScopeSpans: ...
    scope: Optional[InstrumentationScope]
    spans: list[Span]
    schema_url: str

class Span: ...