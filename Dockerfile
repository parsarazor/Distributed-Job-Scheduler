FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/scheduler ./cmd/scheduler && \
    CGO_ENABLED=0 go build -o /bin/worker ./cmd/worker

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /bin/scheduler /bin/scheduler
COPY --from=build /bin/worker /bin/worker
COPY migrations /migrations
EXPOSE 50051 50052
