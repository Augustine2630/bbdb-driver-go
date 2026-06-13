COVERAGE_THRESHOLD := 65
COVERAGE_OUT       := coverage.out

.PHONY: proto/gen build test test/cover test/cover/check vet tidy help

proto/gen: ## Regenerate Go code from proto files
	buf generate

build: ## Build all packages
	go build ./...

test: ## Run all tests
	go test ./... -count=1 -timeout 60s

test/cover: ## Run tests with coverage report
	go test ./... -count=1 -coverprofile=$(COVERAGE_OUT) -timeout 60s
	go tool cover -func=$(COVERAGE_OUT) | tail -1

test/cover/check: ## Fail if coverage < threshold
	go test ./... -count=1 -coverprofile=$(COVERAGE_OUT) -timeout 60s
	@total=$$(go tool cover -func=$(COVERAGE_OUT) | tail -1 | awk '{gsub(/%/,""); print int($$NF)}'); \
	if [ "$$total" -lt "$(COVERAGE_THRESHOLD)" ]; then \
		echo "FAIL: coverage $$total% < $(COVERAGE_THRESHOLD)%"; exit 1; \
	else \
		echo "OK: coverage $$total% >= $(COVERAGE_THRESHOLD)%"; \
	fi

vet: ## Run go vet
	go vet ./...

tidy: ## Run go mod tidy (then verify module name stays bbdb-driver-go)
	go mod tidy
	@head -1 go.mod | grep -q "module bbdb-driver-go" || (echo "ERROR: go mod tidy changed module name!"; exit 1)

help:
	@grep -E '^[a-zA-Z/_.%-]+:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*##"}; {printf "  %-22s %s\n", $$1, $$2}'
