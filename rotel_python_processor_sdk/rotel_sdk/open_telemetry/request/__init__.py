class RequestContext:
    """
    Union type representing different metric data types.
    """
    HttpContext = HttpContext
    GrpcContext = GrpcContext

    @property
    def http_context(self) -> HttpContext: ...

    @property
    def grpc_context(self) -> GrpcContext: ...


class HttpContext:
    headers: dict[str, str]


class GrpcContext:
    metadata: dict[str, str]
