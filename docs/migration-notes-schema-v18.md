# Migration notes — schema v18 pre-commit hardening

Date: 2026-11-23
Branch: `fix/design-review` (changes left uncommitted)

## Current phase: pre-commit fixes (complete locally)

Four fixes with no DDL and no data rewrite:

- `.github/scripts/deployment-smoke.sh` now asserts the true head version, `18:false`,
  matching `SchemaVersion = 18` in `internal/postgres/postgres.go`.
- `internal/service/negative_cache.go` drops the unused package-level cache; the live
  cache is the interceptor-scoped instance in `internal/service/auth.go`.
- `internal/service/grpc_test.go` carries `//lint:ignore SA1019` on the two
  `GetFencingToken` assertions so `make lint` is clean.
- `internal/service/auth.go` makes `ownsJobMetadata` fail closed with
  `codes.Unauthenticated` before any scheduler read, matching `ownedJob`; covered by
  `TestOwnsJobMetadataFailsClosedWithoutIdentity`.

## Behaviour that must be preserved

- Every upgrade path ends at `schema_migrations = 18:false`.
- Legacy rows keep the lease and outbox projection the deployment smoke asserts
  (`lease_expires_at IS NOT NULL`, `job_outbox.published`).
- Wire compatibility: `fencing_token` remains proto field 5, deprecated, never set.
- Unauthenticated job-scoped RPCs return `codes.Unauthenticated`; tenant isolation
  still returns `ErrJobNotFound` for foreign tenants.

## Phased plan

1. **Pre-commit (this change).** Build, vet, lint, unit and Postgres contract tests pass.
   No database or wire change.
2. **Commit and CI on `fix/design-review`.** `.github/workflows/ci.yml` runs `make lint`,
   `make test-integration` (testcontainers Postgres 16) and
   `.github/scripts/deployment-smoke.sh`. This phase ships only after all three pass.
3. **Deployment smoke.** The script builds both legacy fixtures, forces them to
   baseline 1 and 2, runs `migrate up` to head, then asserts `18:false` and the
   projected legacy row. No manual production step is introduced.

## Cutover mechanics

No dual write or backfill is needed: this phase changes no schema, no backfill and no
message format. Existing databases reach v18 only through `migrate up` using the pinned
image `migrate/migrate:v4.19.1`. A version must never be `force`d above what has actually
been applied.

## Rollback for the current phase

- **Code:** revert the pre-commit commit on `fix/design-review`. No database action is
  required because this phase applies no DDL and writes no production data.
- **Smoke databases:** `deployment-smoke.sh` creates `legacy_v1` and `legacy_v2` inside
  its own Compose project and its `cleanup` trap runs `docker compose down --volumes`;
  nothing outside the run is touched.
- **If a run leaves `schema_migrations` dirty:** fix the fixture and rerun; do not
  `force 18` until the DDL is confirmed applied.
- **Manual schema downgrade:** down files exist for every version `000001`–`000018`
  (checked in this phase). Downgrading is destructive and was not executed; take a
  snapshot and step down one version at a time before considering it.

## Verification record for this phase

```
make lint
go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
EXIT:0

gofmt -l .
(no output)

go build ./... && go vet ./...
BUILD_VET_OK

env -u DATABASE_URL -u TEST_POSTGRES_DSN -u INTEGRATION go test -race -count=1 ./...
EXIT:0

TEST_POSTGRES_DSN=postgres://prsamohamadi@localhost:5432/job_scheduler_test?sslmode=disable \
  INTEGRATION=1 go test -race -count=1 ./internal/postgres/...
ok  github.com/prsamohamadi/distributed-job-scheduler/internal/postgres  7.914s
```

`TestOwnsJobMetadataFailsClosedWithoutIdentity` fails against the previous
`ownsJobMetadata` body (`unauthenticated ownsJobMetadata must fail closed: <nil>`) and
passes with the fix.

## Next deprecated item

`fencing_token` is deprecated in `proto/v1/worker.proto` and no longer populated. Remove
it at the next wire-breaking revision and mark field 5 `reserved`.
