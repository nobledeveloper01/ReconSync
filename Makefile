.PHONY: help build test test-integration race lint vet fmt tidy db-setup db-drop migrate-up migrate-down ci

TEST_DB      ?= reconsync_test
TEST_DB_URL  ?= postgres://localhost:5432/$(TEST_DB)?sslmode=disable
MIGRATIONS   := migrations

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-18s\033[0m %s\n",$$1,$$2}'

build: ## Compile everything
	go build ./...

test: ## Unit tests with the race detector (no database needed)
	go test -race ./...

test-integration: db-setup ## Full suite including Postgres integration tests
	RECONSYNC_TEST_DATABASE_URL="$(TEST_DB_URL)" go test -race -cover ./...

race: ## Race detector only
	go test -race ./...

vet: ## go vet
	go vet ./...

fmt: ## Format
	gofmt -w .

tidy: ## Tidy modules
	go mod tidy

lint: ## golangci-lint if installed, else vet
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run; else echo "golangci-lint not installed; running go vet"; go vet ./...; fi

db-setup: ## Create the local test database
	@createdb $(TEST_DB) 2>/dev/null || true

db-drop: ## Drop the local test database
	@dropdb --if-exists $(TEST_DB)

migrate-up: ## Apply migrations to TEST_DB
	psql -q -v ON_ERROR_STOP=1 -d $(TEST_DB) -f $(MIGRATIONS)/0001_init.up.sql

migrate-down: ## Roll migrations back on TEST_DB
	psql -q -v ON_ERROR_STOP=1 -d $(TEST_DB) -f $(MIGRATIONS)/0001_init.down.sql

ci: fmt vet test-integration ## What CI runs
