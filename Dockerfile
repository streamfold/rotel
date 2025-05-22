FROM ubuntu:22.04

ARG TARGETARCH

RUN apt-get update && apt-get install -y ca-certificates && apt-get clean

COPY target/${TARGETARCH}/rotel /rotel
RUN chmod 0755 /rotel

EXPOSE 4317
EXPOSE 4318

ENTRYPOINT ["/rotel", "start", "--otlp-grpc-endpoint", "0.0.0.0:4317", "--otlp-http-endpoint", "0.0.0.0:4318"]
