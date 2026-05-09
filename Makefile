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
	@echo "Mocks regenerated"

.PHONY: lint
lint: ## Run golangci-lint (all modules)
	@echo "Running linter..."
	golangci-lint run ./...
	cd ui && golangci-lint run ./...
	@echo "Linter completed"

.PHONY: test
test: ## Run unit tests (all modules)
	@echo "Running unit tests..."
	go test -v -race -count=1 ./...
	cd ui && go test -v -race -count=1 ./...
	@echo "Unit tests completed"

.PHONY: test-integration
test-integration: ## Run integration tests (all modules)
	@echo "Running integration tests..."
	go test -v -race -count=1 -tags=integration ./...
	cd ui && go test -v -race -count=1 -tags=integration ./...
	@echo "Integration tests completed"

.PHONY: test-all
test-all: test test-integration ## Run all tests

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
