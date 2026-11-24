# Architecture Decision Records (ADRs)
## Project: Multi-Tenant Distributed Job Scheduler in Go

---

### ADR 001: Explicit State Machine via State Pattern for Job Lifecycle (Section 4.1)
* **Decision:** Model the job lifecycle using an explicit state transition table (`map[JobStatus][]JobStatus`) with a centralized transition validator that returns a hard error (`ErrInvalidTransition`) on any illegal state jump.
* **Alternative Rejected:** Ad-hoc conditional logic (`if job.Status == ...`) scattered across database queries, HTTP handlers, and queue consumers.
* **Why:** In distributed systems with concurrent actors (scheduler, multiple workers, client cancellations), scattered conditional checks inevitably introduce race conditions, state corruption (e.g. cancelling an already succeeded job, or running a dead job), and silent bugs. An explicit state table establishes a verifiable contract that is trivially unit-tested in isolation without mocks or databases.

---

### ADR 002: Postgres Job Leases with Fencing Tokens (Section 4.2)
* **Decision:** Enforce single-worker execution with an atomic Postgres claim that changes a ready job from pending to running, increments its fencing token, and records owner and expiry. Workers renew the row lease; a scheduler reaper expires abandoned jobs under `FOR UPDATE SKIP LOCKED`.
* **Alternative Rejected:** Postgres advisory locks held for the duration of handler execution, and separate Redis lease/fencing state.
* **Why:** Claims, lease ownership, and fencing must share one source of truth. Exact-token conditional writes reject stale workers even after lease expiry, while short row locks are held only during claim, renewal, completion, or reaping, not while user code runs.

---

### ADR 003: Strategy Pattern for Retry and Jittered Exponential Backoff (Section 4.3)
* **Decision:** Encapsulate retry delays behind a `BackoffStrategy` interface (`NextDelay(attempt int) time.Duration`) with a concrete `ExponentialBackoffWithJitter` implementation using Full Jitter (`rand(0, min(maxBackoff, base * 2^attempt))`).
* **Alternative Rejected:** Hardcoded delay arithmetic or uniform periodic retries (e.g. fixed 5-second intervals).
* **Why:** Fixed-interval retries cause a "thundering herd" where hundreds of failed jobs retry simultaneously against an already struggling downstream dependency or database. The Strategy pattern decouples backoff calculation from job execution logic, allowing deterministic unit testing with zero-delay or stepped mock strategies, while full jitter smooths out load spikes during outages.

---

### ADR 004: Repository and Transactor Patterns for Database Decoupling (Section 4.4)
* **Decision:** Abstract all PostgreSQL operations behind repository interfaces (`JobRepository`, `TenantRepository`, `JobEventRepository`, `OutboxRepository`) orchestrated across atomic transactions using a `Transactor` interface (`WithinTransaction(ctx, fn)`).
* **Alternative Rejected:** Direct SQL queries/ORM calls inside HTTP handlers and gRPC service methods.
* **Why:** Direct database calls make core business logic untestable without a live database, slowing down developer test loops. The Repository + Transactor pattern enables complete unit test coverage of the state machine, scheduling rules, and outbox operations using fast in-memory mocks, while keeping SQL tuning and connection pool management isolated to the infrastructure layer.

---

### ADR 005: Bounded Worker Concurrency with Weighted Semaphores (Section 4.5)
* **Decision:** Regulate worker execution using a bounded goroutine pool governed by `golang.org/x/sync/semaphore.Weighted` pulling from local buffered channels fed by the RabbitMQ consumer, coupled with `context.Context` cancellation on OS termination signals.
* **Alternative Rejected:** Spawning an unbounded goroutine per incoming message (`go handle(job)`) or rigid OS process forks.
* **Why:** Unbounded goroutines cause memory exhaustion, CPU thrashing, and connection pool starvation when queues experience traffic bursts. `semaphore.Weighted` provides non-blocking capacity checks, graceful degradation, and seamless integration with Go's `context` for graceful worker draining without dropping unacknowledged broker messages.

---

### ADR 006: Message Broker Selection: RabbitMQ Work Queues (Section 3 & Section 7 #2)
* **Decision:** Use RabbitMQ as the message broker utilizing native work-queue semantics (competing consumers, per-message AMQP `basic.ack`/`basic.nack`, confirmed poison-message quarantine, and prefetch limits).
* **Alternative Rejected:** Apache Kafka.
* **Why:** Kafka is a distributed append-only log designed for stream processing, high-throughput analytics, and partition-ordered replays; it requires manual partition rebalancing, static consumer groups, and complex consumer-side head-of-line blocking workarounds for retries. RabbitMQ natively supports independent task distribution, individual message acknowledgments, persistent quarantine for poisoned jobs, and queue-based backpressure, perfectly matching asynchronous task-queue semantics.

---

### ADR 007: Transactional Outbox Pattern for Zero Message Loss (Section 7 #2 & #3)
* **Decision:** Submission commits job metadata and an audit event. Ready dispatch commits an outbox record and a per-attempt dispatch marker under row locks. Only the outbox publisher sends to RabbitMQ and marks rows published after confirmation.
* **Alternative Rejected:** Direct inline publishing to RabbitMQ within the HTTP/gRPC request handler after or before the database write.
* **Why:** Dual-write problems are unavoidable with direct publishing: if DB commit succeeds but broker connection drops, the job is permanently lost; if broker publish succeeds but DB commit fails, the worker executes a non-existent phantom job. The transactional outbox guarantees that any job acknowledged to the client is durably committed to disk, guaranteeing at-least-once dispatch.

---

### ADR 008: Periodic Outbox Reconciliation Engine (Section 7 #3)
* **Decision:** Poll unpublished outbox rows with `FOR UPDATE SKIP LOCKED` inside an explicit transaction that remains open through broker confirmation and the published marker. Each pass is bounded to 15 seconds, each row transaction to four seconds, and confirmation to two seconds. Commit progress per row. Failed rows retry indefinitely with 1–256 second backoff; ten failures emit a diagnostic log and every failure increments a metric.
* **Alternative Rejected:** Distributed 2-Phase Commit (XA) or relying entirely on real-time event listeners (e.g. Postgres LISTEN/NOTIFY).
* **Why:** Two-Phase Commit is notoriously fragile, slow, and unsupported across heterogeneous systems like Postgres and RabbitMQ. Real-time notifications can drop events during broker blips or scheduler restarts. A periodic polling worker with `FOR UPDATE SKIP LOCKED` provides robust, non-blocking, self-healing recovery for any message abandoned during unexpected process crashes.

---

### ADR 009: Fencing Token Edge-Case Protection for GC Pauses (Section 7 #1 & #4)
* **Decision:** Increment the `jobs.fencing_token` on each Postgres claim and require exact token equality for every execution-fenced write. Expiring a lease increments the token again before retrying or dead-lettering the job.
* **Alternative Rejected:** Trusting lease expiration and assuming the worker will never run past its TTL.
* **Why:** Garbage-collection pauses, network delays, and VM throttling can exceed the lease TTL. If a worker loses its lease, its late completion and renewal writes must fail even before a later worker claims the job. Exact database token checks enforce that invariant.

---

### ADR 010: Cascading Dependency Failure and Deadlock Prevention Policy (Section 7 #5)
* **Decision:** When parent Job A succeeds, its pending dependents become eligible for the next ready dispatch. If A reaches `dead` or `cancelled`, pending descendants are cancelled atomically with audit events. Child submission locks the parent and rejects terminal failures and cross-tenant dependencies.
* **Alternative Rejected:** Leaving dependent jobs in `pending` indefinitely, or polling parent state in the worker loop.
* **Why:** Without a cascade-abort policy, downstream dependent jobs become zombies that tie up scheduler queries and confuse API consumers. Automatic cancellation upon parent failure provides fast feedback to clients and eliminates distributed deadlock without requiring polling overhead.

---

### ADR 011: Idempotent Scheduler Dispatch across Crashes (Section 7 #6)
* **Decision:** Make the dispatch engine idempotent by combining the outbox's `published` boolean flag with a database transaction: a restarted scheduler simply re-reads unpublished outbox records and marks them published only upon receiving an AMQP publisher confirmation.
* **Alternative Rejected:** Storing ephemeral in-memory queues or relying on broker-side deduplication.
* **Why:** Crashes between confirmation and commit can duplicate announcements. AMQP `message_id` is the outbox event UUID and does not itself deduplicate deliveries. Workers compare the announced attempt and fencing token before atomically claiming. Handlers receive `job.IdempotencyKey()` for downstream attempt deduplication and must use a job-scoped key when effects must be unique across retries.

### ADR 012: Tenant admission and fair dispatch
Submission locks the tenant row before checking idempotency, outstanding quota and rolling submission rate. A unique partial index enforces `(tenant_id, submission_key)`. Dispatch locks tenant rows using `SKIP LOCKED`, orders by persisted last-dispatch time and selects one job per tenant per round. Ten outstanding dispatched jobs per tenant bound broker backlog. The scheduler is the sole owner of the database; workers claim, renew and complete jobs through its worker RPCs and hold no database credentials. Tenant bearer credentials determine API scope; worker execution/control and scheduler administration use distinct deployment credentials. TLS is required outside explicit local development.


### ADR 013: Bounded API and recovery contracts
List jobs as metadata and paginate audit events within a 1 MiB response budget. Publish only identity/trace announcements to the exact execution queue. A five-minute broker TTL and generation-fenced redispatch recover confirmed but unclaimed attempts. Cancellation retires unsent announcements and retains credits for queued messages until receipt or expiry. Permanent poison messages are confirmed in a durable quarantine queue before acknowledgement, and transient failures back off before requeue.

Handlers receive only trace context and canonical stored input. Worker renewal, cancellation and completion share context-aware admission; cancellation cannot revive timed-out work or claim a successful final outcome was aborted. Bounded shutdown leaves unfinished leases to the reaper. Retention scrubs terminal payloads/trace context after 90 days while preserving submission fingerprints and rejecting requeue of expired input.
