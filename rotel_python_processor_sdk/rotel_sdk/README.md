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
./rotel start --exporter blackhole --otlp-with-logs-processor ./redact_email_in_logs.py --debug-log logs --debug-log-verbosity detailed
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

## Metrics processor example

Finally, let's build a metrics processor. The `telemetrygen` tool doesn't give you a lot of options for customizing the
metrics it generates. You can
send either a Sum or Gauge metric that is named `gen`. Let's build a processor that filters out the `gen` metric and
creates a more interesting exponential
histogram.

Open up your editor or Python IDE and paste the following into a file called `metrics_processor.py`.

```python
import time
from rotel_sdk.open_telemetry.metrics.v1 import (
    MetricData,
    ExponentialHistogramBuckets,
    ExponentialHistogram,
    ExponentialHistogramDataPoint,
    ResourceMetrics,
    Exemplar,
    AggregationTemporality,
    ExemplarValue,
    ResourceMetrics,
    Metric
)

from rotel_sdk.open_telemetry.common.v1 import KeyValue, AnyValue


def process_metrics(resource_metrics: ResourceMetrics):
    scope_metrics = resource_metrics.scope_metrics
    for scope_metric in scope_metrics:
        filtered_metrics = []
        for metric in scope_metric.metrics:
            if metric.name != "gen":
                filtered_metrics.append(metric)

        # Let's create a more useful metric
        if len(filtered_metrics) == 0:
            histo = create_http_latency_exponential_histogram()
            metric = Metric()
            metric.name = "rotel_created_metric"
            metric.unit = "milliseconds"
            metric.description = "Example of how to create a metric with Rotel's Python Processor SDK"
            metric.metadata = [KeyValue("rotel-metadata-example", AnyValue("hello-from-metadata"))]
            metric.data = MetricData.ExponentialHistogram(histo)
            filtered_metrics.append(metric)

        scope_metric.metrics = filtered_metrics


def create_http_latency_exponential_histogram() -> ExponentialHistogram:
    """
    Creates an ExponentialHistogram representing HTTP request latency distribution.
    
    The histogram uses:
    - Scale of 0 (base = 2^(2^0) = 2^1 = 2)
    - Most buckets concentrated in the 5-20ms range
    - Some data in higher latency buckets
    - Exemplars showing specific slow requests
    """

    # Create the main exponential histogram
    histogram = ExponentialHistogram()
    histogram.aggregation_temporality = AggregationTemporality.Cumulative

    # Create a data point for our latency measurements
    data_point = ExponentialHistogramDataPoint()

    # Set timestamps (current time for this example)
    current_time_nano = int(time.time() * 1_000_000_000)
    data_point.start_time_unix_nano = current_time_nano - (60 * 1_000_000_000)  # 1 minute ago
    data_point.time_unix_nano = current_time_nano

    # Add some identifying attributes
    attributes = [
        KeyValue(key="service.name", value=AnyValue("web-api")),
        KeyValue(key="http.method", value=AnyValue("GET")),
        KeyValue(key="http.route", value=AnyValue("/api/users"))
    ]
    data_point.attributes = attributes

    # Set the scale - using scale=0 means base=2, giving us good resolution
    # For latency measurements in milliseconds
    data_point.scale = 0  # base = 2^(2^0) = 2

    # Total count of all measurements
    data_point.count = 10000  # 10k requests in this time window

    # Sum of all latency values in milliseconds
    # Most requests 5-20ms, some 100-500ms, few >1000ms
    # Rough calculation: 9500*12ms + 450*200ms + 50*1500ms = 264,000ms
    data_point.sum = 264000.0  # Total latency in milliseconds

    # Zero count - requests that took essentially 0ms (very rare for HTTP)
    data_point.zero_count = 5

    # Create positive buckets for the main distribution
    positive_buckets = ExponentialHistogramBuckets()

    # With scale=0 (base=2), bucket boundaries are:
    # Bucket index i covers range (2^i, 2^(i+1)]
    # For milliseconds:
    # index 2: (4ms, 8ms]     - some fast requests
    # index 3: (8ms, 16ms]    - most normal requests  
    # index 4: (16ms, 32ms]   - slower normal requests
    # index 5: (32ms, 64ms]   - getting slower
    # index 6: (64ms, 128ms]  - slow requests
    # index 7: (128ms, 256ms] - very slow requests
    # index 8: (256ms, 512ms] - problematic requests
    # index 9: (512ms, 1024ms] - very problematic
    # index 10: (1024ms, 2048ms] - extremely slow

    positive_buckets.offset = 2  # Start from bucket index 2 (4-8ms range)

    # Bucket counts representing our latency distribution
    positive_buckets.bucket_counts = [
        850,  # index 2: (4ms, 8ms] - 850 requests
        4200,  # index 3: (8ms, 16ms] - 4200 requests (peak)
        3800,  # index 4: (16ms, 32ms] - 3800 requests 
        600,  # index 5: (32ms, 64ms] - 600 requests
        200,  # index 6: (64ms, 128ms] - 200 requests
        150,  # index 7: (128ms, 256ms] - 150 requests
        100,  # index 8: (256ms, 512ms] - 100 requests
        60,  # index 9: (512ms, 1024ms] - 60 requests
        30,  # index 10: (1024ms, 2048ms] - 30 requests (>1 second!)
        10,  # index 11: (2048ms, 4096ms] - 10 very slow requests
    ]

    data_point.positive = positive_buckets

    # No negative buckets for latency (can't have negative time)
    data_point.negative = None

    # Add some exemplars to show specific slow requests
    exemplars = []

    # Exemplar 1: A normal request
    exemplar1 = Exemplar()
    exemplar1.time_unix_nano = current_time_nano - (30 * 1_000_000_000)  # 30 seconds ago
    exemplar1.value = ExemplarValue.AsDouble(12.5)  # 12.5ms - typical request
    exemplar1.filtered_attributes = [
        KeyValue(key="user.id", value=AnyValue("user123")),
        KeyValue(key="request.id", value=AnyValue("req-normal-456"))
    ]
    # Add trace context for this exemplar
    exemplar1.trace_id = b'\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10'
    exemplar1.span_id = b'\x11\x12\x13\x14\x15\x16\x17\x18'
    exemplars.append(exemplar1)

    # Exemplar 2: A slow database query
    exemplar2 = Exemplar()
    exemplar2.time_unix_nano = current_time_nano - (45 * 1_000_000_000)  # 45 seconds ago
    exemplar2.value = ExemplarValue.AsDouble(285.7)  # 285.7ms - slow request
    exemplar2.filtered_attributes = [
        KeyValue(key="user.id", value=AnyValue("user789")),
        KeyValue(key="request.id", value=AnyValue("req-slow-789")),
        KeyValue(key="db.operation", value=AnyValue("complex_join"))
    ]
    exemplar2.trace_id = b'\x21\x22\x23\x24\x25\x26\x27\x28\x29\x2a\x2b\x2c\x2d\x2e\x2f\x30'
    exemplar2.span_id = b'\x31\x32\x33\x34\x35\x36\x37\x38'
    exemplars.append(exemplar2)

    # Exemplar 3: An extremely slow request (>1 second)
    exemplar3 = Exemplar()
    exemplar3.time_unix_nano = current_time_nano - (10 * 1_000_000_000)  # 10 seconds ago
    exemplar3.value = ExemplarValue.AsDouble(1847.3)  # 1.85 seconds - very problematic!
    exemplar3.filtered_attributes = [
        KeyValue(key="user.id", value=AnyValue("user999")),
        KeyValue(key="request.id", value=AnyValue("req-timeout-999")),
        KeyValue(key="error.type", value=AnyValue("timeout")),
        KeyValue(key="db.operation", value=AnyValue("full_table_scan"))
    ]
    exemplar3.trace_id = b'\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50'
    exemplar3.span_id = b'\x51\x52\x53\x54\x55\x56\x57\x58'
    exemplars.append(exemplar3)

    data_point.exemplars = exemplars

    # Set min/max values for the observation period
    data_point.min = 3.2  # Fastest request was 3.2ms
    data_point.max = 2847.1  # Slowest request was 2.85 seconds

    # Zero threshold - anything below this is considered "zero"
    data_point.zero_threshold = 0.1  # 0.1ms

    # Add the data point to the histogram
    histogram.data_points = [data_point]

    return histogram
```

Next, run rotel with the following command.

```commandline
./rotel start --exporter blackhole --debug-log metrics --debug-log-verbosity detailed --otlp-with-metrics-processor ./metrics_processor.py
```

Now run the following `telemetrygen` command.

```text
telemetrygen metrics --otlp-endpoint 127.0.0.1:4317 --otlp-insecure
```

With `debug-log` enabled rotel will print out the ResourceMetrics before and after executing your processor.

```commandline
2025-06-21T21:21:48.058316Z  INFO Starting Rotel. grpc_endpoint="127.0.0.1:4317" http_endpoint="127.0.0.1:4318"                                                                                                                                                                                                                                                                     
2025-06-21T21:21:50.535744Z  INFO OTLP payload before processing                                                                 
ResourceMetrics #0                                                                                                                               
Resource SchemaURL: https://opentelemetry.io/schemas/1.13.0                                                                                                                               
ScopeMetrics #0                                                                                                                  
ScopeMetrics SchemaURL:            
InstrumentationScope          
Metric #0                                                                                                                                                                                                  
Descriptor:                                                                                                                                                    
     -> Name: gen                                                                                    
     -> Description:                                                                                                                                           
     -> Unit:                                                                                        
     -> DataType: Gauge                                                                                                                                                                                    
NumberDataPoints #0                                                                                                                                                                                        
StartTimestamp: 0                                                                                                                                                                                          
Timestamp: 1750540910526748000                                                                                 
Value: 0                                                                                                                                                                                                   
2025-06-21T21:21:50.537292Z  INFO OTLP payload after processing                                                        
ResourceMetrics #0                                                                                   
Resource SchemaURL: https://opentelemetry.io/schemas/1.13.0                                                                                                    
ScopeMetrics #0                                                                                                                  
ScopeMetrics SchemaURL:                                                                                                          
InstrumentationScope                                                                                           
Metric #0                                                                                                              
Descriptor:                              
     -> Name: rotel_created_metric                                                                                                               
     -> Description: Example of how to create a metric with Rotel's Python Processor SDK                                                                                                  
     -> Unit: milliseconds                
     -> DataType: ExponentialHistogram                                                                                           
     -> AggregationTemporality: AGGREGATION_TEMPORALITY_CUMULATIVE                                                                               
ExponentialHistogramDataPoints #0                                                                              
Data point attributes:                                                                                                 
     -> service.name: Str(web-api)       
     -> http.method: Str(GET)                                                                        
     -> http.route: Str(/api/users)         
StartTimestamp: 1750540850536904960                                                                  
Timestamp: 1750540910536904960                                                                                                   
Count: 10000                                                                                                                                                   
Sum: 264000                                                                                                                                                    
Min: 3.2                                                                                                       
Max: 2847.1                                                                                                            
Bucket [0, 0], Count: 5                    
Bucket (4, 7.999999999999998], Count: 850                                                            
Bucket (7.999999999999998, 15.999999999999998], Count: 4200                                                                                                                               
Bucket (15.999999999999998, 32], Count: 3800                                                                                                     
Bucket (32, 63.99999999999998], Count: 600                                                                                       
Bucket (63.99999999999998, 127.99999999999997], Count: 200                                                                                                                                
Bucket (127.99999999999997, 255.99999999999994], Count: 150                                                                                                                               
Bucket (255.99999999999994, 511.99999999999994], Count: 100                                                                                                                               
Bucket (511.99999999999994, 1024], Count: 60                                                                           
Bucket (1024, 2048], Count: 30                                                                                                                                                                                                                 
Bucket (2048, 4095.999999999997], Count: 10                                                                                                                                                                                                    
Exemplars:                                                                                                                                                     
Exemplar #0                                                                                                                                      
     -> Trace ID: 0102030405060708090a0b0c0d0e0f10                                                                               
     -> Span ID: 1112131415161718                                                                                                                                                                                                                                 
     -> Timestamp: 1750540880536904960                                                                                                                                                                                                                            
     -> Value: 12.5                                                     
     -> FilteredAttributes:                                             
          -> user.id: Str(user123)                                      
          -> request.id: Str(req-normal-456)                            
Exemplar #1                                                                                                                                                    
     -> Trace ID: 2122232425262728292a2b2c2d2e2f30                                                                                               
     -> Span ID: 3132333435363738                                                                                                                                                                                                                                                                  
     -> Timestamp: 1750540865536904960                                                                                                                                                                                                                                                             
     -> Value: 285.7                                                           
     -> FilteredAttributes:                                                    
          -> user.id: Str(user789)                                             
          -> request.id: Str(req-slow-789)                                     
          -> db.operation: Str(complex_join)                                   
Exemplar #2                                                                                                                                                    
     -> Trace ID: 4142434445464748494a4b4c4d4e4f50                                                                                                                                                                                                                                                                             
     -> Span ID: 5152535455565758                                                                                                                                                                                                                                                                                              
     -> Timestamp: 1750540900536904960                                                       
     -> Value: 1847.3                                                                        
     -> FilteredAttributes:                                                                  
          -> user.id: Str(user999)                                                           
          -> request.id: Str(req-timeout-999)                                                
          -> error.type: Str(timeout)                                                        
          -> db.operation: Str(full_table_scan)                                              
```

## Community and Getting Help

Want to chat about this project, share feedback, or suggest improvements? Join
our [Discord server](https://discord.gg/reUqNWTSGC)! Whether you're a user of this project or not, we'd love to hear
your thoughts and ideas. See you there! 🚀



