.PHONY: build test test-integration vet proto proto-tools migrate lint run-scheduler run-worker demo

build:
	go build ./...

test:
	env -u DATABASE_URL -u INTEGRATION go test -race ./... -count=1 -cover

test-integration:
	env -u DATABASE_URL -u TEST_POSTGRES_DSN -u TEST_RABBITMQ_URL INTEGRATION=1 go test -race ./internal/postgres ./internal/integration -count=1 -timeout=15m

vet:
	go vet ./...

proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

proto:
	@test "$$(protoc --version)" = "libprotoc 35.1" || { printf '%s\n' 'Install protoc 35.1 to regenerate protobufs.'; exit 1; }
	export PATH="$$PATH:$$(go env GOPATH)/bin" && \
	protoc -I. --go_out=. --go_opt=module=github.com/parsarazor/distributed-job-scheduler \
	  --go-grpc_out=. --go-grpc_opt=module=github.com/parsarazor/distributed-job-scheduler \
	  proto/v1/job.proto proto/v1/scheduler.proto proto/v1/worker.proto

migrate:
	@u=$${DATABASE_URL}; \
	case "$$u" in \
		*options=*) dsn="$$u" ;; \
		*\?*) dsn="$$u&options=-c%20lock_timeout%3D5s" ;; \
		*) dsn="$$u?options=-c%20lock_timeout%3D5s" ;; \
	esac; \
	migrate -path ./migrations -database "$$dsn" up

lint:
	go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...

run-scheduler:
	go run ./cmd/scheduler

run-worker:
	go run ./cmd/worker

demo:
	go run ./cmd/demo
