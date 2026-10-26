-include .env
export

BIN := bin
SERVICES := api scheduler worker recovery
MIGRATE := migrate -path migrations -database "$(DATABASE_URL)"

.PHONY: up down build test lint migrate-up migrate-down run-api run-worker run-scheduler run-recovery loadtest

up:
	docker compose up -d --wait

down:
	docker compose down

build:
	@for s in $(SERVICES); do go build -o $(BIN)/$$s ./cmd/$$s || exit 1; done

test:
	go test -race ./...

lint:
	golangci-lint run ./...

migrate-up:
	$(MIGRATE) up

migrate-down:
	$(MIGRATE) down 1

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

run-scheduler:
	go run ./cmd/scheduler

run-recovery:
	go run ./cmd/recovery

loadtest:
	go run ./scripts/loadtest
