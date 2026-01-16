.PHONY: build run clean install-builder

OCB_VERSION := 0.115.0
BUILDER := $(shell go env GOPATH)/bin/builder
BINARY := ./otelcol-benchmark/otelcol-benchmark
CONFIG := collector-config.yaml

# Install the OpenTelemetry Collector Builder
install-builder:
	@echo "Installing OpenTelemetry Collector Builder v$(OCB_VERSION)..."
	go install go.opentelemetry.io/collector/cmd/builder@v$(OCB_VERSION)

# Build the custom collector
build: install-builder
	@echo "Building custom OTel Collector..."
	$(BUILDER) --config=builder-config.yaml
	@echo ""
	@echo "Build complete: $(BINARY)"
	@ls -lh $(BINARY)

# Run the collector with the benchmark config
run: $(BINARY)
	$(BINARY) --config=$(CONFIG)

# Run with a custom config
run-config: $(BINARY)
	@if [ -z "$(CFG)" ]; then \
		echo "Usage: make run-config CFG=path/to/config.yaml"; \
		exit 1; \
	fi
	$(BINARY) --config=$(CFG)

# Clean build artifacts
clean:
	rm -rf otelcol-benchmark

# Show help
help:
	@echo "OTel Collector Benchmark Build"
	@echo ""
	@echo "Targets:"
	@echo "  build          - Build the custom OTel Collector"
	@echo "  run            - Run with default benchmark config"
	@echo "  run-config     - Run with custom config (CFG=path/to/config.yaml)"
	@echo "  clean          - Remove build artifacts"
	@echo "  install-builder - Install ocb (OpenTelemetry Collector Builder)"
	@echo ""
	@echo "Example:"
	@echo "  make build"
	@echo "  make run"
	@echo "  make run-config CFG=my-config.yaml"
