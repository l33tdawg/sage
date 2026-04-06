.PHONY: build build-all test lint fmt proto init up up-full down down-clean status logs logs-abci integration byzantine benchmark benchmark-k6 sdk-test clean help

BINARY=bin/amid
COMPOSE_FILE=deploy/docker-compose.yml
COMPOSE_MON_FILE=deploy/docker-compose.monitoring.yml
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS  = -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the ABCI application binary
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/amid

build-all: ## Build all binaries (amid, sage-gui, sage-cli)
	go build -ldflags "$(LDFLAGS)" -o bin/amid ./cmd/amid
	go build -ldflags "$(LDFLAGS)" -o bin/sage-gui ./cmd/sage-gui
	go build -ldflags "$(LDFLAGS)" -o bin/sage-cli ./cmd/sage-cli

test: ## Run unit tests
	go test ./... -v -count=1 -race

lint: ## Run linter
	golangci-lint run ./...

fmt: ## Format Go code
	gofmt -w .

vet: ## Run go vet
	go vet ./...

proto: ## Generate protobuf code
	buf generate api/proto

init: ## Initialize 4-node testnet configuration
	bash deploy/init-testnet.sh

up: ## Start 4-validator network
	docker compose -f $(COMPOSE_FILE) up -d --build

up-full: ## Start network with monitoring (Prometheus + Grafana)
	docker compose -f $(COMPOSE_FILE) -f $(COMPOSE_MON_FILE) up -d --build

down: ## Stop network
	docker compose -f $(COMPOSE_FILE) down

down-clean: ## Stop network and wipe all data
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans

status: ## Check network status
	@echo "==> Node 0 (localhost:26657):" && curl -s http://localhost:26657/status | python3 -m json.tool 2>/dev/null | grep -E 'latest_block_height|catching_up' || echo "  Not running"
	@echo "==> Node 1 (localhost:26757):" && curl -s http://localhost:26757/status | python3 -m json.tool 2>/dev/null | grep -E 'latest_block_height|catching_up' || echo "  Not running"
	@echo "==> Node 2 (localhost:26857):" && curl -s http://localhost:26857/status | python3 -m json.tool 2>/dev/null | grep -E 'latest_block_height|catching_up' || echo "  Not running"
	@echo "==> Node 3 (localhost:26957):" && curl -s http://localhost:26957/status | python3 -m json.tool 2>/dev/null | grep -E 'latest_block_height|catching_up' || echo "  Not running"

logs: ## View all container logs
	docker compose -f $(COMPOSE_FILE) logs -f

logs-abci: ## View ABCI application logs
	docker compose -f $(COMPOSE_FILE) logs -f abci0 abci1 abci2 abci3

integration: ## Run integration tests (requires running network)
	go test ./test/integration/... -v -count=1 -timeout 300s -tags=integration

byzantine: ## Run Byzantine fault tolerance tests (requires running network)
	go test ./test/byzantine/... -v -count=1 -timeout 120s -tags=byzantine

benchmark: ## Run authenticated load test (Python, Ed25519 signed)
	cd test/benchmark && pip install -q httpx pynacl && python load_test.py

benchmark-k6: ## Run k6 load test (requires pre-configured auth bypass or k6 Ed25519 extension)
	k6 run test/benchmark/load.js

sdk-test: ## Run Python SDK tests
	cd sdk/python && pip install -e ".[dev]" && pytest -v

clean: ## Remove build artifacts
	rm -rf bin/ deploy/genesis/

tidy: ## Run go mod tidy
	go mod tidy
