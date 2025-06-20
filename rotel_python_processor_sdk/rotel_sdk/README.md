# rotel-sdk 🌶️ 🍅

Python type hints package for the Rotel processor SDK.

## Description

This package provides type hints for the Rotel processor SDK and is intended for use with your Python LSP (pyright or
other) in your IDE of choice. [Rotel](https://github.com/streamfold/rotel/) is an efficient, high-performance solution
for collecting, processing, and
exporting telemetry data. Rotel is ideal for resource-constrained environments and applications where minimizing
overhead is critical.

When using a [Python processor enabled](https://github.com/streamfold/rotel/releases/) release of Rotel, you can write
native Python
code to process and filter your telemetry data before sending to an exporter. Rotel
provides Rust binding for Python with the [pyo3](https://crates.io/crates/pyo3) create to provide a high-performance
OpenTelemetry processor API bundled as a Python extension.

## Supported Telemetry Types

| Telemetry Type | Support |
|----------------|---------|
| Traces         | Alpha   |
| Logs           | Alpha   |
| Metrics        | Alpha   |

## Modules and Classes Provided

| Module                               | Classes                                                                                                                                                                                                                                                                                                            |
|--------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| rotel_sdk.open_telemetry.common.v1   | AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,                                                                                                                                                                                                                                                |
| rotel_sdk.open_telemetry.resource.v1 | Resource                                                                                                                                                                                                                                                                                                           |
| rotel_sdk.open_telemetry.trace.v1    | ResourceSpans, ScopeSpans, Span, Event, Link, Status                                                                                                                                                                                                                                                               |
| rotel_sdk.open_telemetry.logs.v1     | ResourceLogs, ScopeLogs, LogRecord                                                                                                                                                                                                                                                                                 |
| rotel_sdk.open_telemetry.metrics.v1  | ResourceMetrics, ScopeMetrics, Metric, MetricData, Gauge, Sum, Histogram, ExponentialHistogram, Summary, ExponentialHistogramBuckets, NumberDataPoint, HistogramDataPoint, ExponentialHistogramDataPoint, SummaryDataPoint, ValueAtQuantile, Exemplar, AggregationTemporality, NumberDataPointValue, ExemplarValue |

## Getting Started

In order to use the Rotel Python processor SDK you will need to either build from source using the `--features pyo3`
flag or download the latest [Python processor enabled version of Rotel](https://github.com/streamfold/rotel/releases/).
Python
processor versions of Rotel are prefixed with `rotel_py_processor`. Choose the release that matches your system
architecture and the version of Python you have installed .

For example is you are going to run Rotel and write processors on `x86_64` with `Python 3.13` download and install...

[rotel_py_processor_3.13_v0.0.1-alpha5_x86_64-unknown-linux-gnu.tar.gz](https://github.com/streamfold/rotel/releases/download/v0.0.1-alpha5/rotel_py_processor_3.13_v0.0.1-alpha5_x86_64-unknown-linux-gnu.tar.gz)

### Setting up a rotel processor development environment

Create a new virtual environment and install the rotel-sdk

```commandline
mkdir /tmp/rotel_processors_example; cd /tmp/rotel_processors_example
python -m venv ./.venv
source ./.venv/bin/activate
pip install rotel-sdk --pre
```

### Writing a trace processor with the `process(resource_spans: ResourceSpans)` function.

Your processor must implement a function called `process` in order for rotel to execute your processor. Each time
process is called
your processor will be handed a instance of the `ResourceSpan` class for you to manipulate as you like.

## Trace processor example

The following is an example OTel ResourceSpan processor called `append_resource_attributes_to_spans.py` which adds the
OS name, version, and a timestamp named `rotel.process.time` to the Resource Attributes of a batch of Spans. Open up
your editor or Python IDE and paste the following into a file called `append_resource_attributes_to_spans.py` and run
with the following command.

```python title="append_resource_attributes_to_spans.py"
import platform
from datetime import datetime

from rotel_sdk.open_telemetry.resource.v1 import Resource
from rotel_sdk.open_telemetry.common.v1 import KeyValue, AnyValue
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans


def process_spans(resource_spans: ResourceSpans):
    resource = resource_spans.resource
    # If resource is None, we'll create a new one to store our attributes, otherwise we'll
    # append to the existing Resource
    if resource is None:
        resource = Resource()

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os_name = platform.system()
    os_version = platform.release()
    # Add attributes
    resource.attributes.append(KeyValue("os.name", AnyValue(os_name)))
    resource.attributes.append(KeyValue("os.version", AnyValue(os_version)))
    resource.attributes.append(KeyValue("rotel.process.time", AnyValue(current_time)))
```

Start rotel and the processor with the following command.

```commandline
./rotel start --exporter blackhole --otlp-with-trace-processor ./append_resource_attributes.py --debug-log traces --debug-log-verbosity detailed
```

If you haven't installed `telemetrygen` yet, use the following command to install

```commandline
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest`
```

Now open a new terminal and run the following telemetrygen command

```commandline
telemetrygen traces --otlp-endpoint 127.0.0.1:4317 --otlp-insecure
```

With `debug-log` enabled rotel will print out the ResourceSpans before and after executing your processor.

```
INFO OTLP payload before processing
ResourceSpans #0
Resource SchemaURL: https://opentelemetry.io/schemas/1.4.0
Resource attributes:
     -> service.name: Str(telemetrygen)
ScopeSpans #0
ScopeSpans SchemaURL: 
InstrumentationScope telemetrygen 
Span #0
    Trace ID       : e199fc80864d844728ebad88ccfadc67
    Parent ID      : 4e4fb0523f19a0d7
    ID             : a94838a53a9b92c0
    Name           : okey-dokey-0
    Kind           : SPAN_KIND_SERVER
Start time: 1750218567597408000
End time: 1750218567597531000
    Status code    : STATUS_CODE_UNSET
    Status message : 
Attributes:
     -> net.peer.ip: Str(1.2.3.4)
     -> peer.service: Str(telemetrygen-client)
Span #1
    Trace ID       : e199fc80864d844728ebad88ccfadc67
    Parent ID      : 
    ID             : 4e4fb0523f19a0d7
    Name           : lets-go
    Kind           : SPAN_KIND_CLIENT
Start time: 1750218567597408000
End time: 1750218567597531000
    Status code    : STATUS_CODE_UNSET
    Status message : 
Attributes:
     -> net.peer.ip: Str(1.2.3.4)
     -> peer.service: Str(telemetrygen-server)

INFO OTLP payload after processing
ResourceSpans #0
Resource SchemaURL: https://opentelemetry.io/schemas/1.4.0
Resource attributes:
     -> service.name: Str(telemetrygen)
     -> os.name: Str(Darwin)
     -> os.version: Str(23.3.0)
     -> rotel.process.time: Str(2025-06-17 20:49:27)
ScopeSpans #0
ScopeSpans SchemaURL: 
InstrumentationScope telemetrygen 
Span #0
    Trace ID       : e199fc80864d844728ebad88ccfadc67
    Parent ID      : 4e4fb0523f19a0d7
    ID             : a94838a53a9b92c0
    Name           : okey-dokey-0
    Kind           : SPAN_KIND_SERVER
Start time: 1750218567597408000
End time: 1750218567597531000
    Status code    : STATUS_CODE_UNSET
    Status message : 
Attributes:
     -> net.peer.ip: Str(1.2.3.4)
     -> peer.service: Str(telemetrygen-client)
Span #1
    Trace ID       : e199fc80864d844728ebad88ccfadc67
    Parent ID      : 
    ID             : 4e4fb0523f19a0d7
    Name           : lets-go
    Kind           : SPAN_KIND_CLIENT
Start time: 1750218567597408000
End time: 1750218567597531000
    Status code    : STATUS_CODE_UNSET
    Status message : 
Attributes:
     -> net.peer.ip: Str(1.2.3.4)
     -> peer.service: Str(telemetrygen-server)

```

## Logs processor example

For logs let's try something a little different. The following is an example OTel log processor called
`redact_emails_in_logs.py`. This script checks to see if LogRecords have a body that contains email addresses.
If an email is found, we redact the email and replace it with the string `[email redacted]`.
Open up your editor or Python IDE and paste the following into a file called `redact_emails_in_logs.py` and run with the
following command.

```python title="redact_emails_in_logs.py"
import re
import itertools

from rotel_sdk.open_telemetry.common.v1 import AnyValue
from rotel_sdk.open_telemetry.logs.v1 import ResourceLogs

email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'


def process_logs(resource_logs: ResourceLogs):
    for log_record in itertools.chain.from_iterable(
            scope_log.log_records for scope_log in resource_logs.scope_logs
    ):
        if hasattr(log_record, 'body') and log_record.body and hasattr(log_record.body, 'value'):
            if log_record.body.value and re.search(email_pattern, log_record.body.value):
                if log_record.body is not None:
                    log_record.body = redact_emails(log_record.body)


def redact_emails(body: AnyValue) -> AnyValue:
    """
    Searches for email addresses in a string and replaces them with '[email redacted]'
    
    Args:
        text (str): The input string to search for email addresses
        
    Returns:
        str: The string with email addresses replaced by '[email redacted]'
    """
    if body.value is None or not isinstance(body.value, str):
        return body
    redacted_body, matches = re.subn(email_pattern, '[email redacted]', body.value)
    if matches == 0:
        return body
    return AnyValue(redacted_body)
```

Now start rotel and the processor with the following command.

```commandline
./rotel start --exporter blackhole  --otlp-with-logs-processor ./redact_email_in_logs.py --debug-log logs --debug-log-verbosity detailed
```

If you haven't installed `telemetrygen` yet, use the following command to install

```commandline
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest`
```

Next run the following `telemetrygen` command.

```text
telemetrygen logs --otlp-endpoint 127.0.0.1:4317 --otlp-insecure --body '192.168.1.45 - - [23/May/2025:14:32:17 +0000] "POST /contact-form HTTP/1.1" 200 1247 "https://example.com/contact" "Mozilla/5.0 (Windows NT 10.0; Win64; x6     4) AppleWebKit/537.36" "email=john.doe@company.com&subject=Support Request&message=Need help with login issues"'
```

With `debug-log` enabled rotel will print out the ResourceLogs before and after executing your processor.

```
2025-06-18T04:00:17.219929Z  INFO Starting Rotel. grpc_endpoint="127.0.0.1:4317" http_endpoint="127.0.0.1:4318"
INFO OTLP payload before processing
ResourceLog #0
Resource SchemaURL: https://opentelemetry.io/schemas/1.4.0
ScopeLogs #0
ScopeLogs SchemaURL: 
LogRecord #0
ObservedTimestamp: 0
Timestamp: 1750219220720478000
SeverityText: Info
SeverityNumber: SEVERITY_NUMBER_INFO(9)
Body: Str(192.168.1.45 - - [23/May/2025:14:32:17 +0000] "POST /contact-form HTTP/1.1" 200 1247 "https://example.com/contact" "Mozilla/5.0 (Windows NT 10.0; Win64; x6     4) AppleWebKit/537.36" "email=john.doe@company.com&subject=Support Request&message=Need help with login issues")
Attributes:
     -> app: Str(server)
    Trace ID       : 
    Span ID        : 
Flags: 0

INFO OTLP payload after processing
ResourceLog #0
Resource SchemaURL: https://opentelemetry.io/schemas/1.4.0
ScopeLogs #0
ScopeLogs SchemaURL: 
LogRecord #0
ObservedTimestamp: 0
Timestamp: 1750219220720478000
SeverityText: Info
SeverityNumber: SEVERITY_NUMBER_INFO(9)
Body: Str(192.168.1.45 - - [23/May/2025:14:32:17 +0000] "POST /contact-form HTTP/1.1" 200 1247 "https://example.com/contact" "Mozilla/5.0 (Windows NT 10.0; Win64; x6     4) AppleWebKit/537.36" "email=[email redacted]&subject=Support Request&message=Need help with login issues")
Attributes:
     -> app: Str(server)
    Trace ID       : 
    Span ID        : 
Flags: 0
```

## Community and Getting Help

Want to chat about this project, share feedback, or suggest improvements? Join
our [Discord server](https://discord.gg/reUqNWTSGC)! Whether you're a user of this project or not, we'd love to hear
your thoughts and ideas. See you there! 🚀



