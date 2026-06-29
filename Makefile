.PHONY: build setup up down query test

build:
	CGO_ENABLED=1 go build -o bin/pqrt ./cmd/main.go

setup:
	bash setup.sh

up:
	docker compose up -d

down:
	docker compose down -v

query:
	docker compose run --rm consumer query $(ARGS)

test:
	go test ./src/...
