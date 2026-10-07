.PHONY: build test test-integration lint demo up down

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

# Needs PostgreSQL (with pg_dump) and an S3 server with Object Lock, e.g. `moto_server -p 4566`.
test-integration:
	CBS_TEST_PG_URL=$${CBS_TEST_PG_URL:-postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable} \
	CBS_TEST_S3_ENDPOINT=$${CBS_TEST_S3_ENDPOINT:-http://localhost:4566} \
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
	go test -race -count=1 ./...

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...

up:
	docker compose up -d --build --wait

demo:
	./scripts/demo.sh

down:
	docker compose down -v
