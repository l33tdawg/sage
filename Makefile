.PHONY: build build-all test test-cometbft-patch lint fmt proto init up up-full down down-clean status logs logs-abci integration byzantine determinism v119-chaos v119-state-sync v119-multiprocess-app benchmark benchmark-k6 sdk-test clean help

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

test-cometbft-patch: ## Run the local CometBFT state-sync hardening regression
	cd third_party/cometbft && go test ./statesync ./blocksync ./node ./state ./store -run '^(TestReceiveOversizedSnapshotWithoutActiveSyncDoesNotPanic|TestStateSyncSealAbort.*|TestStateSyncBootstrapRestart.*|TestOfflineStateSyncHeight.*|TestLoadStateFromDBOrGenesisDocProviderCachesOnlyGenesisBeforeStateSync|TestRecoverStateSyncGenesisDocDBResidue.*|TestStateSyncGenesisDocDBResidue.*|TestPersistStateSyncBootstrap.*|TestCompleteStateSyncBootstrap.*|TestBootstrapAtomicallyPersistsEffectiveStateSyncHeight|TestStateSyncBootstrapComplete.*|TestRecoverIncompleteStateSyncBootstrap.*)$$' -count=1 -race

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

determinism: ## Stand up an isolated 4-node devnet and assert byte-identical AppHash across nodes (audit residual #6; ~15-20 min, cold build)
	bash deploy/scripts/run-determinism.sh

v119-chaos: ## Run isolated real Comet/ABCI SIGKILL and P2P-firewall fault gates
	bash deploy/scripts/run-v11.9-chaos.sh

v119-state-sync: ## Run integrated authorized sage-gui/Comet state-sync wire gate
	bash deploy/scripts/run-v11.9-state-sync.sh

v119-multiprocess-app: ## Run the bounded app-v20 subprocess replay/reconfiguration oracle
	bash deploy/scripts/run-v11.9-multiprocess.sh

benchmark: ## Run authenticated load test (Python, Ed25519 signed)
	cd test/benchmark && pip install -q httpx pynacl && python load_test.py

benchmark-k6: ## Run k6 load test (requires pre-configured auth bypass or k6 Ed25519 extension)
	k6 run test/benchmark/load.js

bench-longmemeval-smoke: ## Smoke-test the LongMemEval-S harness against a running SAGE node (5 questions)
	pip install -q -r bench/longmemeval/requirements.txt && PYTHONUNBUFFERED=1 python3 bench/longmemeval/run.py --limit 5

bench-longmemeval: ## Run full LongMemEval-S benchmark - slow (hours); writes bench/results/longmemeval-<sha>.json
	pip install -q -r bench/longmemeval/requirements.txt && PYTHONUNBUFFERED=1 python3 bench/longmemeval/run.py

bench-locomo-fetch: ## Download the LoCoMo dataset from snap-research/locomo if not already present
	@mkdir -p bench/locomo/data && \
	if [ ! -s bench/locomo/data/locomo10.json ]; then \
		echo "fetching locomo10.json from snap-research/locomo..."; \
		curl -fsSL -o bench/locomo/data/locomo10.json \
			https://raw.githubusercontent.com/snap-research/locomo/main/data/locomo10.json; \
	else \
		echo "bench/locomo/data/locomo10.json already present, skipping fetch"; \
	fi

bench-locomo-smoke: bench-locomo-fetch ## Smoke-test the LoCoMo harness against a running SAGE node (5 questions)
	pip install -q -r bench/locomo/requirements.txt && \
		LOCOMO_DATA_PATH=bench/locomo/data/locomo10.json PYTHONUNBUFFERED=1 python3 bench/locomo/run.py --limit 5

bench-locomo: bench-locomo-fetch ## Run full LoCoMo benchmark; writes bench/results/locomo-<sha>.json
	pip install -q -r bench/locomo/requirements.txt && \
		LOCOMO_DATA_PATH=bench/locomo/data/locomo10.json PYTHONUNBUFFERED=1 python3 bench/locomo/run.py

sdk-test: ## Run Python SDK tests
	cd sdk/python && pip install -e ".[dev]" && pytest -v

clean: ## Remove build artifacts
	rm -rf bin/ deploy/genesis/

tidy: ## Run go mod tidy
	go mod tidy
