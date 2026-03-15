.PHONY: help
help: ## Show all commands
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: migrate
migrate: ## Apply migrations
	@echo "Running migrations..."
	docker exec -i asynqpg-postgres psql -U postgres -d asynqpg < migrations/001_initial.sql
	@echo "Migrations completed"

.PHONY: lint
lint: ## Run golangci-lint
	@echo "Running linter..."
	golangci-lint run ./...
	@echo "Linter completed"

.PHONY: test
test: ## Run unit tests
	@echo "Running unit tests..."
	go test -v -race -count=1 ./...
	@echo "Unit tests completed"

.PHONY: test-integration
test-integration: ## Run integration tests
	@echo "Running integration tests..."
	go test -v -race -count=1 -tags=integration ./...
	@echo "Integration tests completed"

.PHONY: test-all
test-all: test test-integration ## Run all tests

.PHONY: bench
bench: ## Run benchmarks (integration, requires Docker)
	@echo "Running benchmarks..."
	go test -v -tags=integration -bench=. -benchmem -count=3 -timeout=30m ./...
	@echo "Benchmarks completed"

.PHONY: up
up: ## Run postgresql in docker
	@echo "Run PostgreSQL via Docker..."
	docker compose up -d

.PHONY: demo-up
demo-up: ## Start PostgreSQL + observability stack (Jaeger, Prometheus, Grafana, OTel Collector)
	@echo "Starting PostgreSQL + observability stack..."
	docker compose -f docker-compose.yaml -f deploy/docker-compose.observability.yaml up -d
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Services started:"
	@echo "  Jaeger UI:    http://localhost:16686"
	@echo "  Prometheus:   http://localhost:9090"
	@echo "  Grafana:      http://localhost:3000"

.PHONY: demo-down
demo-down: ## Stop all demo services
	docker compose -f docker-compose.yaml -f deploy/docker-compose.observability.yaml down

.PHONY: demo-run
demo-run: ## Run demo (producer + consumers + UI + OTel)
	go run ./examples/demo/...

.PHONY: demo
demo: demo-up migrate demo-run ## Full demo: start infra, migrate, run example

.PHONY: build-frontend
build-frontend: ## Build frontend SPA (requires Node.js + npm)
	@echo "Building frontend..."
	cd ui/frontend && npm install && npm run build
	@echo "Frontend built: ui/frontend/dist/"

.PHONY: dev-frontend
dev-frontend: ## Start Vite dev server (HMR)
	cd ui/frontend && npm run dev
