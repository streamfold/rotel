#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build a minimal OTel Collector for benchmarking
#
# Prerequisites:
#   - Go 1.22+ installed
#   - ocb (OpenTelemetry Collector Builder) installed
#
# To install ocb:
#   go install go.opentelemetry.io/collector/cmd/builder@v0.115.0

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check for Go
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed"
    echo "Install Go from https://golang.org/dl/"
    exit 1
fi

GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
echo "Go version: $GO_VERSION"

# Check/install ocb
OCB_VERSION="0.115.0"
if ! command -v builder &> /dev/null; then
    echo "Installing OpenTelemetry Collector Builder (ocb)..."
    go install go.opentelemetry.io/collector/cmd/builder@v${OCB_VERSION}
fi

# Verify builder is available
BUILDER_PATH=$(go env GOPATH)/bin/builder
if [[ ! -x "$BUILDER_PATH" ]]; then
    echo "Error: builder not found at $BUILDER_PATH"
    echo "Try running: go install go.opentelemetry.io/collector/cmd/builder@v${OCB_VERSION}"
    exit 1
fi

echo ""
echo "Building custom OTel Collector..."
echo "================================="

# Run the builder
"$BUILDER_PATH" --config=builder-config.yaml

# Check if build succeeded
if [[ -f "./otelcol-benchmark/otelcol-benchmark" ]]; then
    echo ""
    echo "Build successful!"
    echo ""
    echo "Binary: $SCRIPT_DIR/otelcol-benchmark/otelcol-benchmark"
    echo ""
    echo "To run:"
    echo "  ./otelcol-benchmark/otelcol-benchmark --config=collector-config.yaml"
    echo ""

    # Show binary size
    ls -lh "./otelcol-benchmark/otelcol-benchmark"
else
    echo "Build failed!"
    exit 1
fi
