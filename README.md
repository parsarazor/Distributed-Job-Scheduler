# Distributed Job Scheduler

A multi-tenant background job system in Go. Submit jobs, they get scheduled, picked up by workers, retried with backoff if they fail, and you get a full audit trail of everything that happened.

I built this to learn how real schedulers deal with the annoying parts of distributed work: two workers grabbing the same job, a worker dying mid-job, the DB saying one thing while the queue says another. Nothing here is magic, it's just the standard patterns wired together properly.

## How it works

Pretty simple flow:

1. You submit a job via gRPC (`SubmitJob`). It gets saved to Postgres **and** an outbox record in the same transaction, so we never lose a job if the queue is down.
2. A background loop picks up ready jobs (`pending`, scheduled time passed, dependencies met) and publishes them to RabbitMQ.
3. Workers pull from the queue with bounded concurrency (semaphore, default 10). Each worker grabs a Redis lease for the job first, so only one worker actually runs it.
4. When done, the worker reports back with its fencing token. If the token doesn't match (lease expired, someone else took over), the write gets rejected instead of overwriting good data.
5. Failed jobs go back to `pending` with exponential backoff + jitter. Run out of attempts and they go `dead`. If a parent job dies, its dependents get cancelled automatically so they don't sit around forever.

Job states are strict: `pending -> running -> succeeded/failed`, `failed -> pending/dead`. Anything else gets rejected. That one decision killed a whole class of weird bugs.

## Stack

- Go 1.26, gRPC
- Postgres (source of truth + transactional outbox)
- RabbitMQ (work queues, acks, dead-lettering)
- Redis (leases / distributed locks with fencing tokens)

No infra? It also runs fully in-memory. If `DATABASE_URL` / `RABBITMQ_URL` / `REDIS_ADDR` aren't set, it just uses in-memory stores. Good for trying things out without Docker.

## Run it

Zero-dependency demo (no Docker needed):

```bash
go run ./cmd/demo
```

This runs the whole lifecycle in-process: submit parent + child, check the dependency gate, dispatch, complete, unlock the child, run a worker pass, reconcile the outbox. Takes a second and prints each step.

Scheduler + worker separately (still in-memory):

```bash
go run ./cmd/scheduler
go run ./cmd/worker
```

Full setup with Docker:

```bash
docker compose up --build
```

That gives you Postgres (:5432), Redis (:6379), RabbitMQ (:5672, UI on :15672), scheduler on :50051, worker on :50052.

## Config

Everything is env vars, see `internal/app/app.go`:

| Var | What it does | Default |
|---|---|---|
| `DATABASE_URL` | Postgres DSN. Unset = in-memory | — |
| `RABBITMQ_URL` | e.g. `amqp://scheduler:scheduler@localhost:5672/` | in-memory |
| `REDIS_ADDR` | e.g. `localhost:6379` | in-memory |
| `PORT` | gRPC port | 50051 / 50052 |
| `WORKER_ID` | Shows up in logs + fencing | worker-1 |
| `MAX_CONCURRENCY` | Worker semaphore size | 10 |

## Layout

```
cmd/scheduler/   - gRPC server + dispatch / retry / reconcile loops
cmd/worker/      - gRPC server + bounded execution loop
cmd/demo/        - in-process end-to-end demo
internal/domain/ - job, tenant, state machine, backoff (no deps, easy to test)
internal/service/- scheduler + worker logic
internal/postgres/ + internal/memory/ - real and in-memory repos (same interfaces)
internal/broker/ - RabbitMQ + in-memory broker
internal/lease/  - Redis + in-memory leases
proto/v1/        - SchedulerService / WorkerService definitions
pkg/api/v1/      - generated gRPC code
migrations/      - Postgres schema
docs/adr/        - why I chose what I chose
```

The `Repository` + `Transactor` interfaces are the seam: all the business logic is tested against the in-memory fakes, Postgres is just an implementation detail.

## Useful commands

```bash
make build   # go build ./...
make test    # go test ./... -count=1 -cover
make vet     # go vet ./...
make proto   # regenerate gRPC code from proto/
make demo    # go run ./cmd/demo
```

Migrations need `DATABASE_URL` set:

```bash
make migrate
```

## Design notes

Longer reasoning lives in `docs/adr/ADR.md`, but the short version:

- **State table, not if-chains.** One map of allowed transitions, everything goes through it.
- **Transactional outbox.** DB write + outbox row in one transaction, async publisher sends to RabbitMQ. No lost jobs, no phantom jobs.
- **Lease + fencing token.** Redis `SET NX EX` for speed, Postgres `WHERE fencing_token = ?` for safety. Stale workers can't clobber good results.
- **Jittered backoff.** Full jitter so a downstream outage doesn't get hammered by synchronized retries.
- **RabbitMQ over Kafka.** I want per-message ack/nack and competing consumers, not a partitioned log.

## Testing

```bash
go test ./... -count=1 -cover
```

Unit tests cover the state machine, backoff, and scheduler/worker against in-memory fakes. There are also property-ish tests for transitions and fault tests (worker crash mid-job, stale fencing token, etc.).
