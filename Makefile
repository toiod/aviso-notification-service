-include .env
export HTTP_ADDR DATABASE_URL REDIS_ADDR

.PHONY: up down run-api test migrate-up migrate-down migrate-status

up:
	docker compose up -d

down:
	docker compose down 

run-api:
	go run ./cmd/api

test:
	go test ./...

migrate-up:
	goose -dir migrations up

migrate-down:
	goose -dir migrations down

migrate-status:
	goose -dir migrations status