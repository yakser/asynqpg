.PHONY: help
help: ## Show all commands
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: migrate
migrate: ## Apply migrations
	@docker exec -i asynqpg-postgres psql -U postgres -d asynqpg < migrations/001_initial.sql > /dev/null 2>&1
	@echo "Migrations applied."

.PHONY: generate
generate: ## Regenerate all mocks (all modules)
	@echo "Regenerating mocks..."
	go generate ./...
	cd ui && go generate ./...
	cd bench && go generate ./...
	@echo "Mocks regenerated"

.PHONY: lint
lint: ## Run golangci-lint (all modules)
	@echo "Running linter..."
	golangci-lint run ./...
	cd ui && golangci-lint run ./...
	cd bench && golangci-lint run ./...
	@echo "Linter completed"

.PHONY: test
test: ## Run unit tests (all modules)
	@echo "Running unit tests..."
	go test -v -race -count=1 ./...
	cd ui && go test -v -race -count=1 ./...
	cd bench && go test -v -race -count=1 ./...
	@echo "Unit tests completed"

.PHONY: test-integration
test-integration: ## Run integration tests (all modules)
	@echo "Running integration tests..."
	go test -v -race -count=1 -tags=integration ./...
	cd ui && go test -v -race -count=1 -tags=integration ./...
	cd bench && go test -v -race -count=1 -tags=integration ./...
	@echo "Integration tests completed"

.PHONY: test-all
test-all: test test-integration ## Run all tests

.PHONY: bench
bench: ## Run benchmarks (integration, requires Docker)
	@echo "Running benchmarks..."
	go test -v -tags=integration -bench=. -benchmem -count=3 -timeout=30m ./...
	@echo "Benchmarks completed"

.PHONY: bench-build
bench-build: ## Build bench harness CLI
	@echo "Building bench harness..."
	cd bench && go build -o ../bin/asynqpg-bench ./cmd/asynqpg-bench
	@echo "Built bin/asynqpg-bench"

.PHONY: bench-smoke
bench-smoke: bench-build ## Quick smoke run (asynqpg, noop, 2 workers, 10s measure)
	@mkdir -p results
	./bin/asynqpg-bench \
		--campaign=bench/campaigns/smoke.yaml \
		--results-dir=results

.PHONY: bench-campaign
bench-campaign: bench-build ## Run a campaign file (usage: make bench-campaign C=bench/campaigns/scaling-asynqpg.yaml)
	@test -n "$(C)" || (echo "usage: make bench-campaign C=<path/to/campaign.yaml>" && exit 1)
	@mkdir -p results
	./bin/asynqpg-bench --campaign=$(C) --results-dir=results

.PHONY: bench-scaling-asynqpg
bench-scaling-asynqpg: bench-build ## Worker-scaling sweep for asynqpg
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/scaling-asynqpg.yaml --results-dir=results

.PHONY: bench-scaling-river
bench-scaling-river: bench-build ## Worker-scaling sweep for river
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/scaling-river.yaml --results-dir=results

.PHONY: bench-scaling-asynq
bench-scaling-asynq: bench-build ## Worker-scaling sweep for asynq
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/scaling-asynq.yaml --results-dir=results

.PHONY: bench-sustained-all
bench-sustained-all: bench-build ## Sustained-throughput campaign for all three libraries
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-asynqpg.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-river.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-asynq.yaml --results-dir=results

.PHONY: bench-burst
bench-burst: bench-build ## Burst scenario (asynqpg)
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/burst-all.yaml --results-dir=results

.PHONY: bench-backlog
bench-backlog: bench-build ## Backlog scenario (asynqpg, producer overshoots consumer)
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/backlog-all.yaml --results-dir=results

.PHONY: bench-thesis
bench-thesis: bench-build ## Thesis campaign: scaling + sustained + backlog + burst for asynqpg only (~60 min)
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/thesis-asynqpg-scaling.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/thesis-asynqpg-sustained.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/thesis-asynqpg-backlog.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/thesis-asynqpg-burst.yaml --results-dir=results

.PHONY: bench-canary
bench-canary: bench-build ## Stability canary (5 short fixed-workload runs)
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/canary.yaml --results-dir=results

.PHONY: bench-all
bench-all: bench-scaling-asynqpg bench-scaling-river bench-scaling-asynq bench-sustained-all bench-burst bench-backlog ## Full canonical campaign matrix (slow)

.PHONY: bench-quick
bench-quick: bench-build ## Fast matrix (~12 min): scaling + sustained across libs + burst, noop only
	@mkdir -p results
	./bin/asynqpg-bench --campaign=bench/campaigns/scaling-asynqpg-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/scaling-river-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/scaling-asynq-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-asynqpg-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-river-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-asynq-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-asynqpg-sleep-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-river-sleep-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/sustained-asynq-sleep-quick.yaml --results-dir=results
	./bin/asynqpg-bench --campaign=bench/campaigns/burst-quick.yaml --results-dir=results

.PHONY: analysis-setup
analysis-setup: ## Sync uv environment for analysis notebooks
	cd analysis && uv sync

.PHONY: analysis-serve
analysis-serve: ## Launch Jupyter Lab on analysis/notebooks
	cd analysis && uv run jupyter lab notebooks

.PHONY: analysis-smoke
analysis-smoke: ## Verify loader can read results/
	cd analysis && uv run python -c "from lib.loader import load_results; df = load_results('../results', refresh=True); print(df.shape); print(df[['library','workload','scenario','workers']].head())"

.PHONY: analysis-canary
analysis-canary: ## Summarize the most recent canary runs (median + IQR); warns if IQR > 10% of median
	cd analysis && uv run python scripts/canary.py

.PHONY: bench-charts-export
bench-charts-export: ## Re-execute analysis notebooks and copy PNGs into ../asynqpg-thesis/images/bench/
	cd analysis && uv run python scripts/export_charts.py

.PHONY: thesis-charts
thesis-charts: ## Execute asynqpg_thesis.ipynb and verify 4 PNGs in analysis/exports/thesis/
	cd analysis && uv run python scripts/export_thesis.py

.PHONY: fake-assets
fake-assets: ## Create placeholder dist/ for Go-only development
	@mkdir -p ui/frontend/dist/assets
	@echo '<!doctype html><html><body>Run make build-frontend</body></html>' > ui/frontend/dist/index.html

.PHONY: up
up: ## Run postgresql in docker
	@echo "Run PostgreSQL via Docker..."
	docker compose up -d

.PHONY: demo-up
demo-up: ## Start PostgreSQL + observability stack (Jaeger, Prometheus, Grafana, OTel Collector)
	@echo "Starting infrastructure..."
	docker compose -f docker-compose.yaml -f deploy/docker-compose.observability.yaml --env-file examples/demo/.env up -d --quiet-pull
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Infrastructure ready."

.PHONY: demo-down
demo-down: ## Stop all demo services
	docker compose -f docker-compose.yaml -f deploy/docker-compose.observability.yaml --env-file examples/demo/.env down

.PHONY: demo-run
demo-run: ## Run demo (accepts ARGS, e.g. make demo-run ARGS="--tasks 50 --no-auto")
	cd examples/demo && go run . $(ARGS)

.PHONY: demo
demo: demo-up migrate demo-run ## Full demo (accepts ARGS, e.g. make demo ARGS="--tasks 50 --no-auto")

.PHONY: build-frontend
build-frontend: ## Build frontend SPA (requires Node.js + npm)
	@echo "Building frontend..."
	cd ui/frontend && npm install && npm run build
	@echo "Frontend built: ui/frontend/dist/"

.PHONY: dev-frontend
dev-frontend: ## Start Vite dev server (HMR)
	cd ui/frontend && npm run dev

.PHONY: release-core
release-core: ## Release core module (usage: make release-core V=v0.5.0)
	python3 scripts/release.py core $(V)

.PHONY: release-ui
release-ui: ## Release UI module (usage: make release-ui V=v0.1.0)
	python3 scripts/release.py ui $(V)
