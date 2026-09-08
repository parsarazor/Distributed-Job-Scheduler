.PHONY: build test vet proto migrate lint run-scheduler run-worker demo

build:
	go build ./...

test:
	go test ./... -count=1 -cover

vet:
	go vet ./...

proto:
	export PATH="$$PATH:$$(go env GOPATH)/bin" && \
	mkdir -p pkg/api/v1 && \
	protoc -I. -I/opt/homebrew/include \
	  --go_out=pkg/api/v1 --go_opt=paths=source_relative \
	  --go-grpc_out=pkg/api/v1 --go-grpc_opt=paths=source_relative \
	  proto/v1/job.proto proto/v1/scheduler.proto proto/v1/worker.proto && \
	mkdir -p /tmp/proto-stage && mv pkg/api/v1/*.go /tmp/proto-stage/ 2>/dev/null || true; \
	if [ -d pkg/api/v1/proto ]; then mv pkg/api/v1/proto/v1/*.go pkg/api/v1/ && rm -rf pkg/api/v1/proto; fi

migrate:
	migrate -path ./migrations -database "$${DATABASE_URL}" up

lint:
	golangci-lint run ./... || staticcheck ./...

run-scheduler:
	go run ./cmd/scheduler

run-worker:
	go run ./cmd/worker

demo:
	go run ./cmd/demo
