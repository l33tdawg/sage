# Makefile for natter — SAGE's libp2p connectivity service.
# This is a separate Go module; run make from within natter/.

BINARY      := natter
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS     := -ldflags "-s -w -X main.version=$(VERSION)"
GO          ?= go

.PHONY: all build test vet lint clean build-linux-arm64

all: build

build:
	$(GO) build $(LDFLAGS) -o bin/$(BINARY) .

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

# Uses golangci-lint when installed, falls back to go vet.
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not found, running go vet"; \
		$(GO) vet ./...; \
	fi

# Cross-compile for the Hetzner CAX11 (Ampere, linux/arm64).
build-linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build $(LDFLAGS) -o bin/$(BINARY)-linux-arm64 .

clean:
	rm -rf bin/
