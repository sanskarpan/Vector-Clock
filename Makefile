.PHONY: build test test-race test-coverage run lint tidy clean frontend frontend-build dev fuzz bench help

BINARY := vectorclock-server
GO_FLAGS := -v
COVERAGE_MIN ?= 60

build:
	go build $(GO_FLAGS) -o bin/$(BINARY) ./cmd/server

# Unit + integration tests, no race detector (fast).
test:
	go test ./... -count=1 -timeout=120s

# Full test suite with race detector. Slower; recommended pre-merge.
test-race:
	go test ./... -race -count=1 -timeout=180s

# Coverage report. Fails the build if total coverage drops below COVERAGE_MIN%.
test-coverage:
	@go test ./... -coverprofile=coverage.out -covermode=atomic -count=1 -timeout=180s
	@COVERAGE=$$(go tool cover -func=coverage.out | tail -1 | awk '{print $$3}' | sed 's/%//'); \
	echo "Coverage: $${COVERAGE}% (minimum: $(COVERAGE_MIN)%)"; \
	awk -v c="$$COVERAGE" -v m="$(COVERAGE_MIN)" 'BEGIN { if (c+0 < m+0) { print "FAIL: coverage " c "% < minimum " m "%"; exit 1 } else { print "PASS: coverage " c "% >= " m "%" } }'
	@go tool cover -html=coverage.out -o coverage.html
	@echo "HTML report: coverage.html"

# Vet + gofmt + golangci-lint (if installed). Fails on any warning.
lint:
	@if [ "$$(gofmt -l . | wc -l)" -gt 0 ]; then \
		echo "gofmt found differences:"; gofmt -l .; exit 1; \
	fi
	@go vet ./...
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run --timeout=5m ./...; \
	else \
		echo "golangci-lint not found, skipping (install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)"; \
	fi

tidy:
	go mod tidy

run:
	go run ./cmd/server

# Run all fuzz targets for a brief time. Each target gets 5 seconds.
fuzz:
	@echo "Fuzzing vector.Compare..."
	@go test -run '^' -fuzz=FuzzCompare -fuzztime=5s ./internal/clock/vector/...
	@echo "Fuzzing vector.MergePassive..."
	@go test -run '^' -fuzz=FuzzMerge -fuzztime=5s ./internal/clock/vector/...
	@echo "Fuzzing vector.TickReceive..."
	@go test -run '^' -fuzz=FuzzTickReceive -fuzztime=5s ./internal/clock/vector/...

# Run all benchmarks once.
bench:
	go test -bench=. -benchtime=1s -run=^$$ ./...

clean:
	rm -rf bin/ coverage.out coverage.html

frontend:
	cd frontend && bun run dev

frontend-build:
	cd frontend && bun run build

# Run both backend and frontend concurrently. Requires tmux or a terminal
# multiplexer; for most setups just run two shells.
dev:
	@echo "Run 'make run' and 'make frontend' in separate terminals."

# Static vulnerability scan. Requires govulncheck.
vuln:
	@go install golang.org/x/vuln/cmd/govulncheck@latest
	@govulncheck ./...

all: build test

help:
	@echo "Available targets:"
	@echo "  build           - Compile the server binary into bin/"
	@echo "  test            - Run tests (no race detector)"
	@echo "  test-race       - Run tests with -race"
	@echo "  test-coverage   - Generate coverage.out + coverage.html, enforce $(COVERAGE_MIN)% minimum"
	@echo "  lint            - gofmt + go vet + golangci-lint"
	@echo "  fuzz            - Run all fuzz targets for 5s each"
	@echo "  bench           - Run benchmarks"
	@echo "  vuln            - Run govulncheck"
	@echo "  tidy            - go mod tidy"
	@echo "  run             - Run the server"
	@echo "  clean           - Remove build + coverage artifacts"