SHELL := /bin/bash
.DEFAULT_GOAL := help

BACKEND  := backend
FRONTEND := frontend
HAS_WEB  := $(wildcard $(FRONTEND)/package.json)

GOLANGCI_VERSION ?= v2.4.0
GOLANGCI := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)
RACE     ?= -race
PG_DSN   ?= postgres://pandora:pandora@localhost:5432/pandora?sslmode=disable
GATEWAY_URL  ?= http://localhost:30080
RATE     ?= 500
DURATION ?= 60s
KIND_CLUSTER ?= pandora
TAG      ?= sha-$(shell git rev-parse --short HEAD 2>/dev/null)
HELM_ARGS ?=

# $(call require,path): fail clearly when a file owned by a later M1 plan is missing.
define require
@test -e $(1) || { echo "missing $(1): it is added by a later M1 plan"; exit 1; }
endef

.PHONY: help proto lint test test-integration build infra-up infra-down migrate \
	invariants smoke load kind-up kind-down deploy fmt

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-18s %s\n", $$1, $$2}'

proto: ## buf lint + generate Go/connect code into backend/gen/go
	cd $(BACKEND) && buf lint && buf generate

lint: ## golangci-lint, buf lint, frontend lint/typecheck
	cd $(BACKEND) && $(GOLANGCI) run ./...
	cd $(BACKEND) && buf lint
ifneq ($(HAS_WEB),)
	pnpm -C $(FRONTEND) run --if-present lint
	pnpm -C $(FRONTEND) run --if-present typecheck
endif

fmt: ## gofumpt + goimports (and frontend format)
	cd $(BACKEND) && $(GOLANGCI) fmt
ifneq ($(HAS_WEB),)
	pnpm -C $(FRONTEND) run --if-present format
endif

test: ## Unit tests (no Docker)
	cd $(BACKEND) && go test $(RACE) ./...
ifneq ($(HAS_WEB),)
	pnpm -C $(FRONTEND) run --if-present test
endif

test-integration: ## Integration tests (testcontainers, needs Docker)
	cd $(BACKEND) && go test $(RACE) -tags integration ./...

build: ## Compile everything
	cd $(BACKEND) && go build ./...
ifneq ($(HAS_WEB),)
	pnpm -C $(FRONTEND) run --if-present build
endif

infra-up: ## Start local datastores (docker compose)
	docker compose up -d --wait

infra-down: ## Stop local datastores
	docker compose down

migrate: ## Apply goose migrations to PG_DSN
	$(call require,$(BACKEND)/migrations)
	goose -dir $(BACKEND)/migrations postgres "$(PG_DSN)" up

invariants: ## Ledger SQL invariants against PG_DSN (cmd/ledger --invariants)
	$(call require,$(BACKEND)/cmd/ledger/main.go)
	cd $(BACKEND) && PG_DSN="$(PG_DSN)" go run ./cmd/ledger --invariants

smoke: ## k6 smoke against GATEWAY_URL
	$(call require,$(BACKEND)/load/k6/smoke.js)
	k6 run -e GATEWAY_URL=$(GATEWAY_URL) $(BACKEND)/load/k6/smoke.js

load: ## k6 orders_sustained (RATE, DURATION, P99_MS)
	$(call require,$(BACKEND)/load/k6/orders_sustained.js)
	k6 run -e GATEWAY_URL=$(GATEWAY_URL) -e RATE=$(RATE) -e DURATION=$(DURATION) \
		$(BACKEND)/load/k6/orders_sustained.js

kind-up: ## Create the kind cluster
	$(call require,$(BACKEND)/deploy/kind/cluster.yaml)
	kind create cluster --name $(KIND_CLUSTER) --config $(BACKEND)/deploy/kind/cluster.yaml

kind-down: ## Delete the kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

deploy: ## helm upgrade --install the umbrella chart (TAG, HELM_ARGS)
	$(call require,$(BACKEND)/deploy/helm/pandora/Chart.yaml)
	helm upgrade --install pandora $(BACKEND)/deploy/helm/pandora --dependency-update \
		--namespace pandora --create-namespace --set image.tag=$(TAG) --wait --timeout 10m $(HELM_ARGS)
