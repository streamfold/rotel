# Datadog Exporter

The Datadog exporter is an experimental exporter that supports exporting OTLP trace span data to Datadog, without having to
run the Datadog agent. It is a port of the Go implementation contained across the following repos:

* [opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib)
* [datadog-agent](https://github.com/DataDog/datadog-agent)
* [opentelemetry-mapping-go](https://github.com/DataDog/opentelemetry-mapping-go)

At the moment this exporter only supports **traces**, so enabling it will disable receiving of metrics and logs at the
OTLP receiver. In the future it will be possible to split exporting of telemetry across multiple exporters.

## Remaining support

The trace export support is early, but should produce working traces. This is a list of the known remaining large pieces
to support, in no particular order. If you find issues with the current support, please open an issue!

* Trace stats/metrics
* Normalizing tag names and values
* Support for sampling at the exporter
* GCP and Azure attributes, it supports AWS today
* Span events and links
