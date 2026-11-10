SHELL := bash
GO ?= go
GOLANGCI_LINT_VERSION ?= v2.13.2
GOVULNCHECK_VERSION ?= v1.1.4
ACTIONLINT_VERSION ?= v1.7.7
BIN := bin/gowhale
VERSION ?= 0.1.0
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X github.com/marxms/go-whale/internal/platform/version.Version=$(VERSION) \
	-X github.com/marxms/go-whale/internal/platform/version.Commit=$(COMMIT) \
	-X github.com/marxms/go-whale/internal/platform/version.Date=$(BUILD_DATE)

.PHONY: help tools-check fmt fmt-check tidy-check mod-verify lint dead-code boundaries workflow-lint test test-count race vet vuln build integration integration-compile verify clean

help:
	@echo "Targets:"
	@echo "  tools-check verifies the Go toolchain and the pinned tool versions"
	@echo "  fmt         format the module with gofumpt and goimports"
	@echo "  fmt-check   fail when a file is not gofumpt/goimports-clean"
	@echo "  tidy-check  fail when go.mod or go.sum is not tidy (go mod tidy -diff)"
	@echo "  mod-verify  verify downloaded modules against go.sum"
	@echo "  lint        run golangci-lint $(GOLANGCI_LINT_VERSION)"
	@echo "  dead-code   run the pinned deadcode tool over ./cmd/..."
	@echo "  boundaries  enforce the core import allow-list"
	@echo "  workflow-lint run actionlint $(ACTIONLINT_VERSION) over the workflows"
	@echo "  test        run the unit suite"
	@echo "  test-count  print the number of discovered test cases"
	@echo "  race        run the unit suite with the race detector"
	@echo "  vet         run go vet"
	@echo "  vuln        run govulncheck $(GOVULNCHECK_VERSION)"
	@echo "  build       build $(BIN)"
	@echo "  integration run tests behind the integration build tag"
	@echo "  integration-compile compile integration-tagged tests without running them"
	@echo "  verify      run every gate above (tools-check, fmt-check, lint, boundaries, workflow-lint, tidy-check, mod-verify, vuln, vet, test, race, test-count, integration-compile, build)"
	@echo "  clean       remove $(BIN)"

tools-check:
	@set -euo pipefail; \
	command -v "$(GO)" >/dev/null 2>&1 || { echo "tools-check: $(GO) not found; install the toolchain declared in go.mod" >&2; exit 1; }; \
	want="$$(sed -n 's/^toolchain go//p' go.mod)"; \
	if [ -z "$$want" ]; then echo "tools-check: go.mod has no toolchain directive" >&2; exit 1; fi; \
	got="$$($(GO) env GOVERSION)"; \
	if [ "$$got" != "go$$want" ]; then echo "tools-check: Go $$got does not match go.mod toolchain go$$want (set GOTOOLCHAIN=auto)" >&2; exit 1; fi; \
	command -v python3 >/dev/null 2>&1 || { echo "tools-check: python3 is required by make boundaries" >&2; exit 1; }; \
	test -f .golangci.yml || { echo "tools-check: .golangci.yml is missing" >&2; exit 1; }; \
	for version in "$(GOLANGCI_LINT_VERSION)" "$(GOVULNCHECK_VERSION)" "$(ACTIONLINT_VERSION)"; do \
		case "$$version" in ''|latest) echo "tools-check: tool versions must be pinned, got '$$version'" >&2; exit 1 ;; esac; \
	done
	@$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) version >/dev/null || { echo "tools-check: golangci-lint $(GOLANGCI_LINT_VERSION) failed to run" >&2; exit 1; }
	@$(GO) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION) -version >/dev/null || { echo "tools-check: actionlint $(ACTIONLINT_VERSION) failed to run" >&2; exit 1; }
	@echo "tools-check: Go $$($(GO) env GOVERSION) · golangci-lint $(GOLANGCI_LINT_VERSION) · govulncheck $(GOVULNCHECK_VERSION) · actionlint $(ACTIONLINT_VERSION)"

# One formatter for the whole module: gofumpt (stricter gofmt) plus goimports
# with the module path as local prefix, both driven by .golangci.yml.
fmt:
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) fmt ./...

# --diff never writes: exit 0 means clean, exit 1 means at least one file would
# change. Any other exit is the tool failing to run, which must not be mistaken
# for a formatting diff.
fmt-check:
	@set +e; \
	out="$$($(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) fmt --diff ./... 2>&1)"; \
	status=$$?; \
	if [ "$$status" -eq 0 ]; then echo "fmt-check: all files are gofumpt/goimports-clean"; exit 0; fi; \
	printf '%s\n' "$$out"; \
	if [ "$$status" -eq 1 ]; then \
		echo "fmt-check: formatting drift; run 'make fmt' (gofumpt + goimports, local prefix github.com/marxms/go-whale)" >&2; \
	else \
		echo "fmt-check: golangci-lint $(GOLANGCI_LINT_VERSION) fmt failed to run (exit $$status)" >&2; \
	fi; \
	exit 1

tidy-check:
	$(GO) mod tidy -diff

mod-verify:
	$(GO) mod verify

lint:
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

# deadcode is pinned by the `tool` directive in go.mod, so this target never
# downloads @latest at run time. It reports functions unreachable from ./cmd/...
dead-code:
	$(GO) tool deadcode ./cmd/...

boundaries:
	python3 scripts/check_import_boundaries.py

workflow-lint:
	$(GO) run github.com/rhysd/actionlint/cmd/actionlint@$(ACTIONLINT_VERSION) .github/workflows/*.yml

test:
	$(GO) test -count=1 ./...

test-count:
	@set -o pipefail; \
	out="$$($(GO) test -list '^Test' ./... 2>&1)" || { echo "test-count: go test -list failed" >&2; echo "$$out" >&2; exit 1; }; \
	count="$$(printf '%s\n' "$$out" | grep -c '^Test' || true)"; \
	if [ "$$count" -eq 0 ]; then echo "test-count: no test cases discovered" >&2; exit 1; fi; \
	echo "test-count: $$count test cases"

race:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN) ./cmd/gowhale

integration:
	$(GO) test -count=1 -tags=integration ./...

integration-compile:
	$(GO) test -count=1 -tags=integration -run '^$$' ./...

verify: tools-check fmt-check lint boundaries workflow-lint tidy-check mod-verify vuln vet test race test-count integration-compile build
	@echo "verify: all gates passed"

clean:
	rm -rf bin
