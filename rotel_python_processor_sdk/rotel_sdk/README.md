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

| Telemetry Type | Support     |
|----------------|-------------|
| Traces         | Alpha       |
| Metrics        | Coming Soon |
| Logs           | Coming Soon |

## Modules and Classes Provided

| Module                               | Classes                                                             |
|--------------------------------------|---------------------------------------------------------------------|
| rotel_sdk.open_telemetry.common.v1   | AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, |
| rotel_sdk.open_telemetry.resource.v1 | Resource                                                            |
| rotel_sdk.open_telemetry.trace.v1    | ResourceSpans, ScopeSpans, Span, Event, Link, Status                |

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

The following is an example OTel trace processor called `append_resource_attributes.py` which adds the OS name, version,
and a timestamp named
`rotel.process.time` to the Resource Attributes of a batch of Spans. Open up your editor or Python IDE and paste the
following into a file called `append_resource_attributes.py` and run with the following command.

```python
import platform
from datetime import datetime

from rotel_sdk.open_telemetry.resource.v1 import Resource
from rotel_sdk.open_telemetry.common.v1 import KeyValue
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans


def process(resource_spans: ResourceSpans):
    resource = resource_spans.resource
    # If resource is None, we'll create a new one to store our attributes, otherwise we'll append to the existing Resource
    if resource is None:
        resource = Resource()

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os_name = platform.system()
    os_version = platform.release()
    # Add attributes
    resource.attributes.append(KeyValue.new_string_value("os.name", os_name))
    resource.attributes.append(KeyValue.new_string_value("os.version", os_version))
    resource.attributes.append(KeyValue.new_string_value("rotel.process.time", current_time))

```

Now start rotel and the processor with the following command.

`./rotel start --otlp-exporter-endpoint <otlp-endpoint-url> --otlp-with-trace-processor ./append_resource_attributes.py`

## Community and Getting Help

Want to chat about this project, share feedback, or suggest improvements? Join
our [Discord server](https://discord.gg/reUqNWTSGC)! Whether you're a user of this project or not, we'd love to hear
your thoughts and ideas. See you there! 🚀



