-include .env
export

BIN := bin
SERVICES := api scheduler worker recovery
# The migrator is a small Go program in migrate/, so `make migrate-up` needs no
# golang-migrate binary on the host (and the compose stack uses the same code).
MIGRATE := go run ./migrate -dir migrations

.PHONY: up down build test lint migrate-up migrate-down run-api run-worker run-scheduler run-recovery loadtest seed

up:
	docker compose up -d --build --wait

# Up, then show every service once it is healthy. This is the command to use in
# front of an audience.
up-scale:
	docker compose up -d --build --wait --scale worker=3
	docker compose ps

down:
	docker compose down

build:
	@for s in $(SERVICES); do go build -o $(BIN)/$$s ./cmd/$$s || exit 1; done

test:
	go test -race ./...

lint:
	golangci-lint run ./...

migrate-up:
	$(MIGRATE)

# There is no down migration runner: dropping and recreating the volume is the
# honest way to get a clean database, and it is what `make reset` does.
migrate-down:
	@echo "use 'make reset' to drop the database volume"

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

seed:
	psql "$(DATABASE_URL)" -f scripts/seed.sql

# Wipe everything: containers, volumes, and the Redis queue. Use before a demo.
reset:
	docker compose down -v
	@echo "all data gone; 'make up' starts clean"
