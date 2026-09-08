# Architecture Decision Records (ADRs)
## Project: Multi-Tenant Distributed Job Scheduler in Go

---

### ADR 001: Explicit State Machine via State Pattern for Job Lifecycle (Section 4.1)
* **Decision:** Model the job lifecycle using an explicit state transition table (`map[JobStatus][]JobStatus`) with a centralized transition validator that returns a hard error (`ErrInvalidTransition`) on any illegal state jump.
* **Alternative Rejected:** Ad-hoc conditional logic (`if job.Status == ...`) scattered across database queries, HTTP handlers, and queue consumers.
* **Why:** In distributed systems with concurrent actors (scheduler, multiple workers, client cancellations), scattered conditional checks inevitably introduce race conditions, state corruption (e.g. cancelling an already succeeded job, or running a dead job), and silent bugs. An explicit state table establishes a verifiable contract that is trivially unit-tested in isolation without mocks or databases.

---

### ADR 002: Lease-Based Distributed Locking in Redis with Fencing Tokens (Section 4.2)
* **Decision:** Enforce single-worker execution using Redis distributed leases (`SET lock:job:<id> <worker_id> NX EX 30`) renewed periodically via background goroutine heartbeats, combined with a monotonically increasing `fencing_token` checked at Postgres write time.
* **Alternative Rejected:** Postgres advisory locks (`pg_advisory_xact_lock`) or un-fenced Redis locks with simple TTL.
* **Why:** Postgres advisory locks hold open expensive database connection pool slots for the entire duration of long-running user jobs, bottlenecking connection pools. Simple Redis TTL locks fail under GC pauses or network partitions where worker A stalls, its lock expires, worker B picks up the job, and worker A awakens to write stale results. The lease + fencing token pattern provides scalable lock throughput in Redis while guaranteeing zero double-writes in Postgres via conditional `UPDATE jobs ... WHERE fencing_token = :token`.

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
* **Decision:** Use RabbitMQ as the message broker utilizing native work-queue semantics (competing consumers, per-message AMQP `basic.ack`/`basic.nack`, dead-letter exchanges, and prefetch limits).
* **Alternative Rejected:** Apache Kafka.
* **Why:** Kafka is a distributed append-only log designed for stream processing, high-throughput analytics, and partition-ordered replays; it requires manual partition rebalancing, static consumer groups, and complex consumer-side head-of-line blocking workarounds for retries. RabbitMQ natively supports independent task distribution, individual message acknowledgments, dead-letter exchanges for poisoned jobs, and queue-based backpressure, perfectly matching asynchronous task-queue semantics.

---

### ADR 007: Transactional Outbox Pattern for Zero Message Loss (Section 7 #2 & #3)
* **Decision:** Implement the Transactional Outbox pattern where job metadata, audit events, and an `outbox` record are written to PostgreSQL inside a single database transaction; an asynchronous dispatcher publishes outbox records to RabbitMQ and marks them published.
* **Alternative Rejected:** Direct inline publishing to RabbitMQ within the HTTP/gRPC request handler after or before the database write.
* **Why:** Dual-write problems are unavoidable with direct publishing: if DB commit succeeds but broker connection drops, the job is permanently lost; if broker publish succeeds but DB commit fails, the worker executes a non-existent phantom job. The transactional outbox guarantees that any job acknowledged to the client is durably committed to disk, guaranteeing at-least-once dispatch.

---

### ADR 008: Periodic Outbox Reconciliation Engine (Section 7 #3)
* **Decision:** Run a background reconciliation ticker in the Scheduler that polls `SELECT ... FROM job_outbox WHERE published = FALSE AND created_at < NOW() - INTERVAL '5s' FOR UPDATE SKIP LOCKED` and republishes stranded messages to RabbitMQ.
* **Alternative Rejected:** Distributed 2-Phase Commit (XA) or relying entirely on real-time event listeners (e.g. Postgres LISTEN/NOTIFY).
* **Why:** Two-Phase Commit is notoriously fragile, slow, and unsupported across heterogeneous systems like Postgres and RabbitMQ. Real-time notifications can drop events during broker blips or scheduler restarts. A periodic polling worker with `FOR UPDATE SKIP LOCKED` provides robust, non-blocking, self-healing recovery for any message abandoned during unexpected process crashes.

---

### ADR 009: Fencing Token Edge-Case Protection for GC Pauses (Section 7 #1 & #4)
* **Decision:** Incorporate a monotonically increasing `fencing_token` into the `jobs` schema and distributed lease contract; state updates to terminal statuses require `WHERE fencing_token = :held_token`.
* **Alternative Rejected:** Trusting Redis lock expiration and assuming the worker will never run past the lock TTL.
* **Why:** In distributed systems, stop-the-world garbage collection pauses, unexpected network delays, or VM CPU throttling can exceed the lock TTL. If Worker A loses its lease and Worker B assumes ownership, Worker A must not be allowed to overwrite Worker B's progress. Conditioning database updates on the fencing token guarantees that late-arriving writes from zombie workers fail harmlessly.

---

### ADR 010: Cascading Dependency Failure and Deadlock Prevention Policy (Section 7 #5)
* **Decision:** When parent Job A completes: if A succeeds, dependent Job B is transitioned to `pending` and pushed to the outbox; if parent Job A reaches a terminal failure (`dead` or `cancelled`), dependent Job B is automatically transitioned to `cancelled` with an audit event explaining the parent failure.
* **Alternative Rejected:** Leaving dependent jobs in `pending` indefinitely, or polling parent state in the worker loop.
* **Why:** Without a cascade-abort policy, downstream dependent jobs become zombies that tie up scheduler queries and confuse API consumers. Automatic cancellation upon parent failure provides fast feedback to clients and eliminates distributed deadlock without requiring polling overhead.

---

### ADR 011: Idempotent Scheduler Dispatch across Crashes (Section 7 #6)
* **Decision:** Make the dispatch engine idempotent by combining the outbox's `published` boolean flag with a database transaction: a restarted scheduler simply re-reads unpublished outbox records and marks them published only upon receiving an AMQP publisher confirmation.
* **Alternative Rejected:** Storing ephemeral in-memory queues or relying on broker-side deduplication.
* **Why:** Scheduler crashes can occur immediately before or after message dispatch. Relying on database-backed delivery tracking combined with idempotent message identifiers (`job_id` as RabbitMQ `message_id`) ensures the broker and worker deduplicate re-deliveries, preserving at-least-once delivery with zero job loss.
