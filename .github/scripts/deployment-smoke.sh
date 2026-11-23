#!/usr/bin/env bash
set -euo pipefail

export COMPOSE_PROJECT_NAME="scheduler-smoke-${GITHUB_RUN_ID:-$$}"
export RABBITMQ_HOSTNAME=ci-rabbit-legacy

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        docker compose ps -a
        docker compose logs --no-color
    fi
    docker compose down --volumes
    exit "$status"
}
trap cleanup EXIT

docker compose config --quiet
docker compose up -d --build --wait --wait-timeout 180 scheduler worker
curl -fsS http://127.0.0.1:9090/readyz
curl -fsS http://127.0.0.1:9091/readyz
for service in scheduler worker; do
    test "$(docker compose exec -T "$service" id -u)" = 65532
done

# Persist a message with a nondefault node identity and recreate the container.
# Match the old node name exactly as an operator preserving a volume would.
rabbit_api() {
    curl -fsS --retry 15 --retry-all-errors --retry-delay 1 --max-time 5 \
        --user scheduler:scheduler -H 'Content-Type: application/json' "$@"
}
api=http://127.0.0.1:15672/api
rabbit_api -X PUT -d '{"durable":true}' "$api/queues/%2F/recreate-smoke"
rabbit_api -d '{"properties":{"delivery_mode":2},"routing_key":"recreate-smoke","payload":"retained","payload_encoding":"string"}' \
    "$api/exchanges/%2F/amq.default/publish" | jq -e '.routed == true'
docker compose up -d --no-deps --force-recreate --wait --wait-timeout 120 rabbitmq
rabbit_api -d '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto","truncate":50000}' \
    "$api/queues/%2F/recreate-smoke/get" | jq -e 'length == 1 and .[0].payload == "retained"'

psql_db() {
    local db=$1
    shift
    docker compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U scheduler -d "$db" "$@"
}

for baseline in 1 2; do
    db="legacy_v$baseline"
    psql_db scheduler -c "CREATE DATABASE $db"
    psql_db "$db" < migrations/000001_init_schema.up.sql
    if [ "$baseline" = 2 ]; then
        psql_db "$db" < migrations/000002_leases.up.sql
    fi
    psql_db "$db" <<'SQL'
INSERT INTO tenants (id,name,api_key_hash)
VALUES ('00000000-0000-0000-0000-000000000001','legacy fixture',repeat('a',64));
INSERT INTO jobs (id,tenant_id,status)
VALUES ('00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000001','running');
INSERT INTO job_outbox (job_id,routing_key,payload)
VALUES ('00000000-0000-0000-0000-000000000002','job.ready','{}');
SQL
    migrate_args=(-path=/migrations "-database=postgres://scheduler:scheduler@postgres:5432/$db?sslmode=disable")
    docker compose run --rm --no-deps migrate "${migrate_args[@]}" force "$baseline"
    docker compose run --rm --no-deps migrate "${migrate_args[@]}" up
    test "$(psql_db "$db" -Atc 'SELECT version || chr(58) || dirty::text FROM schema_migrations')" = '19:false'
    test "$(psql_db "$db" -Atc "SELECT count(*) FROM jobs JOIN tenants ON jobs.tenant_id=tenants.id JOIN job_outbox ON job_outbox.job_id=jobs.id WHERE tenants.name='legacy fixture' AND jobs.lease_expires_at IS NOT NULL AND job_outbox.published")" = 1
done
