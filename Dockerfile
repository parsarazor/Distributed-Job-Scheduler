FROM golang:1.26.11-alpine3.23@sha256:23f198931d4bd83b4f5a613bd75656257ae8bf832dcbc2db80e1050b5219ca2a AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/scheduler ./cmd/scheduler
COPY cmd/worker ./cmd/worker
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -o /bin/scheduler ./cmd/scheduler && \
    CGO_ENABLED=0 go build -mod=readonly -trimpath -o /bin/worker ./cmd/worker

FROM alpine:3.23.8@sha256:469c9ea410b9215eaee7976ba6c0639039b444be8e5d638f7abbada189971eb2
RUN apk add --no-cache ca-certificates
COPY --from=build /bin/scheduler /bin/scheduler
COPY --from=build /bin/worker /bin/worker
USER 65532:65532
EXPOSE 50051 50052
