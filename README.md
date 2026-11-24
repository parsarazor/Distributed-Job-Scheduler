# Distributed Job Scheduler

A multi-tenant background job system in Go. Submit jobs, they get scheduled, picked up by workers, retried with backoff if they fail, and you get a full audit trail of everything that happened.

I built this to learn how real schedulers deal with the annoying parts of distributed work: two workers grabbing the same job, a worker dying mid-job, the DB saying one thing while the queue says another. Nothing here is magic, it's just the standard patterns wired together properly.

## How it works

Pretty simple flow:

1. You submit a job via authenticated gRPC (`SubmitJob`). The job and audit event commit together. An optional `idempotency_key` returns the same job for identical retries from the same tenant; conflicting reuse returns `AlreadyExists`.
2. A background loop locks ready jobs (`pending`, schedule and retry delay passed, dependencies met) with `FOR UPDATE SKIP LOCKED`, creates their outbox rows, and marks the attempts dispatched in one transaction. Only the outbox publisher talks to RabbitMQ. Each confirmation commits in its own bounded transaction; failed rows retry after exponential delays of 1–256 seconds. Messages contain only the job ID, attempt, fencing token and trace context.
3. Each job name has its own durable queue, `jobs.execute.<name>` (names over 242 bytes, `quarantine` or starting with `h.` use `jobs.execute.h.<sha256 prefix>`). A worker consumes the queues of the job names it has handlers for, with prefetch equal to `MAX_CONCURRENCY` across all of them, so it never receives a job it cannot run. It also drains the legacy shared `jobs.execute` queue for announcements staged before this routing existed. Workers have no database access: each one asks the scheduler (`ClaimJobExecution`) to atomically claim the pending attempt in Postgres; that update mints its fencing token and lease. Lease renewal (`RenewJobLease`) and completion (`ReportJobCompletion`) also go through the scheduler. The message is acknowledged after the result commits. Duplicate and stale announcements cannot claim another attempt. Database polling remains available for the in-process demo.
4. When done, the worker reports back with its fencing token. If the token doesn't match (lease expired, someone else took over), the write gets rejected instead of overwriting good data.
5. Failed jobs go back to `pending` with exponential backoff + jitter. Run out of attempts and they go `dead` in Postgres. If a parent job dies, its dependents get cancelled automatically so they don't sit around forever.
6. Confirmed attempts still pending after five minutes are announced again with a new fencing generation. Execution messages expire from the broker after five minutes; late generations cannot claim newer work. This repairs lost or recreated queues.
7. Workers renew leases every third of the TTL. Losing a lease cancels the handler context and withholds completion; the scheduler reaper fences expired attempts and retries or dead-letters them in one transaction.

Normal job states are `pending -> running -> succeeded/failed`, `failed -> pending/dead`, with pending cancellation. `RequeueDeadJob` explicitly grants a dead job with retained input three more attempts, preserving its attempt history and increasing its fencing token. Previously cancelled dependents stay cancelled.

Delivery and execution are **at least once**. A crash after an external effect but before completion can repeat that effect. Handlers receive `job.IdempotencyKey()` (`job UUID:attempt number`) for downstream deduplication of an attempt. For an effect that must happen only once across all retries, use the job UUID as the downstream key. Database fencing protects scheduler writes; external services must implement their own idempotency or fencing. Handlers must honor context cancellation and their deadline.

## Stack

- Go 1.26.11 or newer, gRPC
- Postgres (source of truth + transactional outbox)
- RabbitMQ (durable work queue, persistent messages, confirms, ack/nack and reconnect)
- Postgres-backed leases and fencing tokens

For local use without infrastructure, explicitly set `STORAGE=memory`. This selects process-private in-memory repositories and broker and emits a warning; it is only suitable for local development and tests. With `STORAGE` unset or set to `postgres`, both `DATABASE_URL` and `RABBITMQ_URL` are required.

## Run it

Zero-dependency demo (no Docker needed):

```bash
go run ./cmd/demo
```

This runs the whole lifecycle in-process: submit parent + child, check the dependency gate, dispatch, complete, unlock the child, run a worker pass, reconcile the outbox. Takes a second and prints each step.

Scheduler alone, in memory (workers need a scheduler with shared state, so run them against the Compose stack):

```bash
APP_ENV=development STORAGE=memory DEMO_SEED=true go run ./cmd/scheduler
```

Full setup with Docker:

```bash
docker compose up --build
```

Postgres and AMQP stay on the private Compose network. The RabbitMQ UI (:15672), scheduler (:50051), worker (:50052), and operations endpoints (:9090/:9091) publish only on host loopback. Compose applies versioned migrations with golang-migrate before starting the application services. This Compose configuration explicitly sets `APP_ENV=development` and uses local credentials. Production requires separate credentials, TLS and a worker token.

Provision a tenant before submitting jobs. Generate a random API key, store only its SHA-256 hex digest in `tenants.api_key_hash`, and supply the raw key as gRPC metadata `authorization: Bearer <key>`. Tenant RPCs derive identity from this credential; the request's `tenant_id` is ignored when authenticated. Execution, heartbeat, claim, lease-renewal and completion RPCs require `WORKER_TOKEN`. Worker control (status, drain, cancel-execution) and manual dispatch/reconciliation require the distinct optional `ADMIN_TOKEN`; omitting it disables those methods. The in-memory scheduler seeds `demo-key-123` only with `APP_ENV=development DEMO_SEED=true`, and memory mode binds only to a literal loopback address. In development the worker registers an illustrative `send-email` handler; production starts with no example handlers, so register your own handlers for real work. Unknown handlers fail the attempt.

### Worker-plane trust model

`WORKER_TOKEN` is a single shared credential, deliberately: it authorizes *transport* operations (heartbeat, completion reports, `Execute` lookups) and grants **no** cross-tenant data access by itself. The authorization path for job payloads is:

1. Dispatch goes to the queue of the job's name; workers claim by lease. A job that arrives on the legacy shared queue without a handler fails fast (dead-lettered on first failure, not retried).
2. `Execute`/`Heartbeat` verify the calling worker still holds the job's lease via the fencing token; a stale or lost lease returns the job payload to no one.
3. Tenant payloads therefore flow only to workers that won the dispatch race for that job. `Execute` and `Heartbeat` are lease-scoped, and `ReportJobCompletion` is rejected unless the supplied worker ID and fencing token match the live lease. `GetWorkerStatus`, `DrainWorker` and `CancelExecution` are worker *control*-plane RPCs and require `ADMIN_TOKEN`, so the shared worker credential cannot read active-job metadata, cancel another execution or drain the fleet.

The scheduler is the only service with database credentials. Workers are stateless: they consume RabbitMQ announcements and reach job state only through the scheduler's worker RPCs, and refuse to start if `DATABASE_URL` is set. A compromised worker can therefore claim announced jobs and see their payloads, but cannot read or modify other rows. The shared `WORKER_TOKEN` is still a fleet-wide credential; per-worker identities (mTLS or per-worker tokens) are future hardening.

Tenant admission is serialized in Postgres: at most 1,000 outstanding jobs and 600 newly created jobs per minute per tenant. Dispatch rotates tenants using persisted turns, preserves priority within each tenant, and caps each tenant at `tenants.max_in_flight` (default 10) queued/running dispatched jobs. These limits apply across scheduler replicas. Canceled announcements are retired before publication. Published canceled announcements retain their backlog credits until receipt or broker expiry. Idempotent submission retries do not consume additional quota. Dependency validation reads metadata and limits chains to 128 jobs.

## Tenant administration

Tenants are managed through admin RPCs (`ADMIN_TOKEN`): `CreateTenant`, `ListTenants`, `UpdateTenant` and `RotateTenantKey`. The `schedctl` CLI (also in the image as `/bin/schedctl`) wraps them:

```sh
export ADMIN_TOKEN=$(openssl rand -hex 32)              # Compose passes it to the scheduler; unset disables admin RPCs
export SCHEDULER_ADDR=127.0.0.1:50051                    # SCHEDULER_INSECURE=1 for local plaintext
schedctl tenant create -name acme -max-in-flight 40     # prints the API key once
schedctl tenant list
schedctl tenant update -id <tenant-id> -max-in-flight 100
schedctl tenant update -id <tenant-id> -disable          # -enable to restore
schedctl tenant rotate-key -id <tenant-id>               # old key stops working immediately
```

Only the SHA-256 of each API key is stored, so a lost key is replaced by rotating it. Disabling blocks the tenant's key on every replica at once. Jobs already accepted keep running, so cancel them first if they must stop.

## Writing handlers (`pkg/worker`)

Workers are built with the public SDK `github.com/parsarazor/distributed-job-scheduler/pkg/worker`. `cmd/worker` is this SDK plus the admin-only worker port and the operations server.

```go
cfg, err := worker.ConfigFromEnv() // or build worker.Config directly
w, err := worker.New(cfg)
w.HandleFunc("send-invoice", func(ctx context.Context, job *worker.Job) error {
    var in struct{ OrderID string }
    if err := job.Decode(&in); err != nil {
        return worker.Permanent(err) // dead now, no retries
    }
    return sendInvoice(ctx, job.ID, in.OrderID) // job.ID is the dedup key
})
err = w.Run(ctx) // nil after a clean drain when ctx is cancelled
```

- **Results:** `nil` succeeds. `worker.Permanent(err)` dead-letters at once. Any other error, or a panic (its stack trace goes to `Config.Logger`), is retried with backoff until `MaxAttempts`.
- **Cancellation:** the handler's `ctx` ends on timeout, lease loss, an operator `CancelExecution`, or when `ShutdownTimeout` runs out. `context.Cause(ctx)` is `context.DeadlineExceeded`, `worker.ErrLeaseLost`, `worker.ErrCanceled` or `worker.ErrShutdown`.
- **Run:** returns `nil` after a clean drain, or an error on startup failure, unexpected intake loss, or when `ShutdownTimeout` expires with handlers still running. `Ready()` backs readiness probes.
- **Transport:** `Config.TLS` secures gRPC to the scheduler (system roots when nil). RabbitMQ TLS comes from the `amqps://` URL. `Insecure: true` (set by `ConfigFromEnv` for `APP_ENV=development`) allows `amqp://` and development credentials, and plaintext gRPC only when `TLS` is nil, so a configured `TLS` (e.g. from `SCHEDULER_CA_FILE`) is never downgraded.
- **Boundary:** the SDK has no database driver or storage code; a test enforces this with `go list -deps`.
- **Example:** [`examples/sendemail`](examples/sendemail) is the `send-email` handler `cmd/worker` registers in development.

### Jobs no worker handles

RabbitMQ refuses an announcement when no worker has declared the queue for its job name. The scheduler then puts the job back to pending and retries it a minute later, without counting a broker failure or using the publish retry budget. If no worker appears within `NO_WORKER_TIMEOUT` (24 h by default, at most 168 h; counted from the first refused announcement since the job last reached a worker queue or was requeued, so time blocked on a parent or retry backoff doesn't count), the job goes dead with `no worker for job name`; requeue it once a worker exists. Watch `scheduler_no_worker_deferrals_total` and `scheduler_no_worker_dead_jobs_total`. A job-name queue with no consumer for an hour is deleted, so a retired handler's jobs take this path too.

**Upgrading:** deploy workers before schedulers. Workers keep draining the shared `jobs.execute` queue; if schedulers go first, their announcements wait as above until workers declare their queues.

## Worker credentials

Each worker can have its own token instead of the shared `WORKER_TOKEN`. A per-worker token only works for RPCs whose `worker_id` equals the `WORKER_ID` it was issued for, so a leaked token cannot renew or complete another worker's leases. Revoking one takes effect on every replica immediately.

```sh
schedctl worker create -worker-id edge-1        # prints the token once; set it as that worker's WORKER_TOKEN
schedctl worker list
schedctl worker revoke -id <credential-id>
```

`WORKER_ID` must match `^[A-Za-z0-9._-]{1,255}$` (no spaces or colons). `schedctl`, the scheduler and the worker all reject other IDs at startup or on the request.

A worker's own gRPC port (`Execute`, `Heartbeat`, `GetWorkerStatus`, `CancelExecution`, `DrainWorker`) accepts only `ADMIN_TOKEN`. Worker tokens, shared or per-worker, are refused there, so a revoked worker token works nowhere. Compose leaves that port's admin RPCs off. If you enable them, set the worker's `ADMIN_TOKEN` to a value different from the schedulers' `ADMIN_TOKEN`, so a compromised worker cannot administer tenants or credentials.

To rotate a token with no downtime, create a second credential for the same worker ID, deploy it, then revoke the old one. Once every worker has its own token, set `SHARED_WORKER_TOKEN=disabled` on the schedulers so the fleet-wide token stops working for scheduler RPCs. The schedulers must still have `WORKER_TOKEN` set, because startup requires it.

## Scheduler replicas

Run two or more scheduler replicas against the same Postgres for availability. Every replica runs all background loops. Dispatch, outbox publication, retry recovery, lease reaping and retention claim rows with `FOR UPDATE SKIP LOCKED`, so replicas split the work and never process the same row twice. Tenant limits are enforced in Postgres and hold across replicas.

- **Clients:** put the replicas behind an L4/L7 gRPC load balancer, or a DNS name that resolves to all of them.
- **Workers:** point `SCHEDULER_ADDR` at a DNS name that resolves to every replica. Workers balance calls with gRPC `round_robin`, and replicas recycle connections every 5 minutes so new replicas pick up traffic. If a replica dies, its calls fail with `Unavailable` and the worker retries them on another replica.
- **Per-replica state:** dispatch wake-ups and the bad-key throttle are local to each replica. The `DISPATCH_INTERVAL` tick covers work that another replica made ready.
- **Restarts:** each starting replica extends running leases that have lapsed by `LEASE_RESTART_GRACE`. With other replicas up, only leases that lapsed since their reaper's last pass are affected, so a dead worker's job is re-run at most that much later.

Local two-replica stack: `docker compose -f docker-compose.yml -f docker-compose.ha.yml up -d --build`.

## Config

Everything is env vars, see `internal/app/app.go`:

| Var | What it does | Default |
|---|---|---|
| `STORAGE` | `memory` for local-only in-memory components, or `postgres` | `postgres` |
| `DATABASE_URL` | Scheduler only; required in Postgres mode. Workers reject it | — |
| `RABBITMQ_URL` | Required in Postgres mode, e.g. `amqp://scheduler:scheduler@localhost:5672/` | — |
| `SCHEDULER_ADDR` | Worker only; scheduler gRPC address, e.g. `scheduler:50051` | — |
| `SCHEDULER_CA_FILE`, `SCHEDULER_TLS_SERVER_NAME` | Worker only; CA bundle and server name for scheduler TLS (system roots by default). Plaintext only with `APP_ENV=development` and no CA file | — |
| `PORT` | gRPC port | 50051 / 50052 |
| `WORKER_ID` | Shows up in logs + fencing; letters, digits, `.`, `_`, `-`, at most 255 characters | worker-1 |
| `MAX_CONCURRENCY` | Worker semaphore size | 10 |
| `LEASE_TTL` | Database lease duration | 30s |
| `REAPER_INTERVAL` | Expired-lease scan interval | 10s |
| `DISPATCH_INTERVAL` | Fallback dispatch interval; submissions and completions also trigger dispatch immediately | 2s |
| `LEASE_RESTART_GRACE` | On scheduler startup, extend lapsed running leases by this much so workers can renew them or report results they could not deliver during the outage | 30s |
| `COMPLETION_RETRY_WINDOW` | How long a worker keeps retrying a finished job's result while the scheduler is unreachable | 1m |
| `SHUTDOWN_TIMEOUT` | Worker only; how long shutdown waits for running handlers before cancelling them | 20s |
| `NO_WORKER_TIMEOUT` | Scheduler only; how long a job whose name has no worker queue keeps waiting (retried every minute, without using publish retries) before it goes dead with `no worker for job name` | 24h |
| `JOB_TIMEOUT` | Worker execution time cap; `timeout_ms` can request a shorter deadline | 5m |
| `APP_ENV` | `development` explicitly permits local credentials and plaintext gRPC | production behavior |
| `WORKER_TOKEN` | Bearer credential for `Execute`/`Heartbeat`/`ReportJobCompletion`; at least 32 visible ASCII characters in production | — |
| `ADMIN_TOKEN` | Distinct credential for worker control (status/drain/cancel-execution), manual dispatch and reconciliation; omitted disables these methods | disabled |
| `DEMO_SEED` | Opt-in known demo tenant; requires development memory storage | false |
| `GRPC_HOST` | gRPC bind address; memory mode requires a literal loopback IP | 127.0.0.1 |
| `TLS_CERT_FILE`, `TLS_KEY_FILE` | Server certificate/key; required outside explicit development | — |
| `METRICS_ADDR` | HTTP metrics and health listener; wider binds are explicit | 127.0.0.1:9090 / 127.0.0.1:9091 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional OTLP collector endpoint; standard OTEL TLS/auth variables apply | export disabled |

`/metrics` exposes persisted job counts, outbox depth/lag, transition counters (including retries/dead jobs), execution duration and lease losses. `/readyz` checks the clean schema and connectivity, plus publisher connectivity on the scheduler or a live subscription on the worker; `/healthz` is liveness. Named and whole-server gRPC health match readiness. Health `Watch` requires a worker/admin token, caps active streams, service-name length and receive/stream lifetime, and turns unavailable immediately on worker drain. JSON logs include job, tenant and worker identifiers; trace context is persisted through the outbox so execution spans connect to the submission RPC. Spans export through OTLP when configured. Metrics omit tenant/job labels to keep cardinality bounded.

SIGTERM cancels startup promptly, stops intake and background loops, and gives workers a bounded drain before canceling handlers and exiting. `DrainWorker` cancels the subscription and waits within the request timeout; timeout returns `DeadlineExceeded` with draining still active. `CancelExecution` requires the current fencing token and refuses cancellation after finalization starts. Canceled or timed-out handlers cannot renew their leases.

`ListJobs` returns metadata without payloads, uses a stable timestamp/ID order and exposes `next_offset`. `GetJobEvents` supports `limit`/`offset` and `next_offset`; list/event responses stay within a 1 MiB budget. Handler failures and panic values become fixed audit categories, with detailed user data suppressed. Invalid messages are copied persistently to `jobs.execute.quarantine` with publisher confirmation before the original is acknowledged; transient failures use delayed redelivery. Inspect and prune the quarantine through restricted broker administration.

Hourly retention removes up to 1,000 published outbox rows older than seven days and terminal-job audit rows older than 90 days per pass. It also scrubs payloads and trace context from up to 1,000 jobs terminal for 90 days. `payload_expired` identifies scrubbed jobs, which cannot be requeued; submit new input instead. Job IDs, dependency metadata, submission keys and fingerprints remain for deduplication. Pending delivery rows remain until settled or expired. Production PostgreSQL connections require verified TCP TLS including all fallbacks, and RabbitMQ requires `amqps` with non-development credentials.

Before upgrading, back up the database and RabbitMQ volume and stop old schedulers/workers. PostgreSQL 15+ and clean schema version 18 are required; startup/readiness reject missing or dirty migrations. Migration 7 also rejects unsupported stored priorities (allowed: 0, 2, 5, 10), quarantines cross-tenant dependency chains, and compacts unpublished announcements. Deploy both binaries together because repository and announcement contracts changed.

An old database initialized directly from SQL may lack `schema_migrations`. Compare its tables, columns, constraints and indexes against the complete `000001` schema and, when present, both `lease_owner`/`lease_expires_at` plus the lease index from `000002`. Only a verified complete v1 or v2 schema can be baselined with `migrate -path ./migrations -database "$DATABASE_URL" force 1` (or `force 2`), followed by `up`. `force` records a version without running SQL; never apply it to an arbitrary dirty or partial database. Older Docker initialization could also run `*.down.sql`; a partly initialized volume needs inspection/restore before migration. CI exercises verified v1 and v2 baselines with the deployment migrator.

Prefer an application rollback with the additive schema retained. Down migrations 3–7 cannot restore retired announcements, expired lease decisions, removed dependency edges, fingerprints or scrubbed payloads. Restore a matching backup for a full data rollback. This schema is not compatible with a binary requiring another exact version.

RabbitMQ data directories depend on the node name. For an existing volume, set `RABBITMQ_HOSTNAME` to the exact original hostname so `RABBITMQ_NODENAME=rabbit@<hostname>` remains unchanged. Keep the original image via `RABBITMQ_IMAGE` during recovery; follow the broker’s supported upgrade path before moving to the pinned default. New installations use the stable hostname `rabbitmq`. Do not point a new identity at an existing volume and assume its queues were recovered. See [RabbitMQ upgrades](https://www.rabbitmq.com/docs/upgrade) and [migrate baseline behavior](https://github.com/golang-migrate/migrate/blob/v4.18.3/GETTING_STARTED.md).

## Layout

```
cmd/scheduler/   - gRPC server + dispatch / retry / reconcile / reaper loops
cmd/worker/      - pkg/worker + admin worker port + operations server
pkg/worker/      - public handler SDK
examples/        - example handlers
cmd/demo/        - in-process end-to-end demo
internal/domain/ - job, tenant, state machine, backoff (no deps, easy to test)
internal/service/- scheduler + worker logic
internal/postgres/ + internal/memory/ - real and in-memory repos (same interfaces)
internal/broker/ - RabbitMQ + in-memory broker
proto/v1/        - SchedulerService / WorkerService definitions
pkg/api/v1/      - generated gRPC code
migrations/      - Postgres schema
docs/adr/        - why I chose what I chose
```

The `Repository` + `Transactor` interfaces support both fast memory tests and live Postgres contracts. Live integration tests also run the full Postgres → outbox → RabbitMQ → worker pipeline.

## Useful commands

```bash
make build   # go build ./...
make test    # race tests without Docker
make test-integration # live testcontainers tests with -race; Docker required
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
- **Transactional outbox.** DB write + outbox row in one transaction, async publisher sends to RabbitMQ. Per-message confirmation progress and stale-dispatch repair preserve recovery after broker outages.
- **Lease + fencing token.** Postgres atomically claims jobs, mints fencing tokens, and persists lease ownership/expiry. Stale workers cannot commit completion or renewal writes.
- **Jittered backoff.** Full jitter so a downstream outage doesn't get hammered by synchronized retries.
- **RabbitMQ over Kafka.** I want per-message ack/nack and competing consumers, not a partitioned log.

## Testing

```bash
go test ./... -count=1 -cover
INTEGRATION=1 go test -race ./internal/postgres ./internal/integration -count=1 -timeout=15m
```

Unit tests cover transitions, fault rollback, leases, timeouts, authenticated tenant isolation, idempotent submissions, quotas, fair dispatch and broker failure handling. Testcontainers starts disposable migrated Postgres and RabbitMQ instances for concurrent claim/dispatch/reaper tests, database-clock expiry checks, confirmed publishing, redelivery, drain, and restart recovery. Live tests skip unless explicitly enabled. Docker is the default; alternatively set `TEST_POSTGRES_DSN` and `TEST_RABBITMQ_URL` to dedicated disposable servers. PostgreSQL tests create/drop isolated databases; broker tests delete canonical queues, so never select a shared broker/vhost. `TEST_RABBITMQ_RESTART` can name an absolute executable that restarts only that disposable broker. Without it, native restart tests skip. An inherited `DATABASE_URL` never opts tests into live writes. CI runs formatting, build, vet, Staticcheck, unit/integration race tests, govulncheck, protobuf regeneration checks, and a Compose image/startup/upgrade smoke test. Images, actions and generator/tool versions are pinned.
