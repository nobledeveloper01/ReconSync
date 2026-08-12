.PHONY: help build test test-integration race lint vet fmt tidy db-setup db-drop migrate-up migrate-down ci

TEST_DB      ?= reconsync_test
TEST_DB_URL  ?= postgres://localhost:5432/$(TEST_DB)?sslmode=disable
MIGRATIONS   := migrations

# Keep in step with .github/workflows/ci.yml so local and CI agree.
GOLANGCI_LINT_VERSION ?= v2.12.2

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-18s\033[0m %s\n",$$1,$$2}'

build: ## Compile everything
	go build ./...

demo: ## End to end in one command: debit in, verified reversal webhook out
	@bash scripts/demo.sh

test: ## Unit tests with the race detector (no database needed)
	go test -race ./tests/...

test-integration: db-setup ## Full suite including Postgres integration tests
	RECONSYNC_TEST_DATABASE_URL="$(TEST_DB_URL)" go test -race -coverpkg=./internal/... ./tests/...

test-isolation: db-setup ## Tenant isolation gate on its own (§8.1)
	RECONSYNC_TEST_DATABASE_URL="$(TEST_DB_URL)" go test -race ./tests/... -run 'Store/TenantIsolation' -v

race: ## Race detector only
	go test -race ./tests/...

vet: ## go vet
	go vet ./...

crosscheck: ## Build for the platform CI uses (dev is often arm64, CI is amd64)
	@echo "building linux/amd64"
	GOOS=linux GOARCH=amd64 go build ./...
	GOOS=linux GOARCH=amd64 go vet ./...

fmt: ## Format
	gofmt -w .

tidy: ## Tidy modules
	go mod tidy

lint: ## Lint with the same golangci-lint version CI pins
	@command -v golangci-lint >/dev/null 2>&1 || \
		{ echo "installing golangci-lint $(GOLANGCI_LINT_VERSION)"; \
		  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	golangci-lint run ./...

vuln: ## Scan for known vulnerabilities (§8.5)
	@command -v govulncheck >/dev/null 2>&1 || go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck ./...

db-setup: ## Create the local test database
	@createdb $(TEST_DB) 2>/dev/null || true

db-drop: ## Drop the local test database
	@dropdb --if-exists $(TEST_DB)

migrate-up: ## Apply migrations to TEST_DB
	psql -q -v ON_ERROR_STOP=1 -d $(TEST_DB) -f $(MIGRATIONS)/0001_init.up.sql

migrate-down: ## Roll migrations back on TEST_DB
	psql -q -v ON_ERROR_STOP=1 -d $(TEST_DB) -f $(MIGRATIONS)/0001_init.down.sql

ci: fmt vet crosscheck lint test-integration ## What CI runs
