#!/usr/bin/env bash
# End-to-end demo and smoke test against a running deployment (docker compose or Kubernetes).
# It creates a policy, backs up the demo "shop" database twice, restores it into
# "shop_restored" and checks the restored data matches.
#
#   API=http://localhost:8080 PSQL="docker compose exec -T postgres psql -U postgres" ./scripts/demo.sh
set -euo pipefail

API=${API:-http://localhost:8080}
PSQL=${PSQL:-docker compose exec -T postgres psql -U postgres}
# Host name of PostgreSQL as the workers see it.
PG_HOST=${PG_HOST:-postgres:5432}
J='Content-Type: application/json'

json() { python3 -c "import sys, json; d = json.load(sys.stdin); print($1)"; }

wait_job() {
  for _ in $(seq 1 120); do
    state=$(curl -sf "$API/v1/jobs/$1" | json 'd["state"]')
    case $state in succeeded) return 0 ;; failed) curl -s "$API/v1/jobs/$1"; echo; return 1 ;; esac
    sleep 1
  done
  echo "job $1 timed out"; return 1
}

echo "== waiting for the API"
for _ in $(seq 1 60); do curl -sf "$API/healthz" >/dev/null && break; sleep 2; done

echo "== creating a backup policy (the password comes from the workers' environment)"
policy=$(curl -sf -X POST "$API/v1/policies" -H "$J" -d "{
  \"name\": \"shop-db-$(date +%s)\", \"source_type\": \"postgres\",
  \"source\": \"postgres://postgres:\${SHOP_DB_PASSWORD}@$PG_HOST/shop?sslmode=disable\",
  \"schedule\": \"0 2 * * *\", \"retention_days\": 30}" | json 'd["id"]')
echo "policy $policy"

for run in 1 2; do
  echo "== backup $run"
  job=$(curl -sf -X POST "$API/v1/policies/$policy/backups" | json 'd["id"]')
  wait_job "$job"
  curl -sf "$API/v1/jobs/$job" | json 'd["result"]["snapshot_id"], d["result"]["stats"]'
done

# pg_dump writes the dump time into the archive header, so even an unchanged database gives one
# changed chunk (the first). Everything else must be deduplicated.
second=$(curl -sf "$API/v1/jobs/$job" | json 'd["result"]["stats"]["new_chunks"]')
if [ "$second" -gt 1 ]; then echo "expected at most 1 new chunk for unchanged data, got $second"; exit 1; fi
echo "second backup of unchanged data uploaded $second new chunk(s); the rest were deduplicated"

snapshot=$(curl -sf "$API/v1/jobs/$job" | json 'd["result"]["snapshot_id"]')
echo "== restoring $snapshot into shop_restored"
$PSQL -q -c "DROP DATABASE IF EXISTS shop_restored WITH (FORCE)" -c "CREATE DATABASE shop_restored"
job=$(curl -sf -X POST "$API/v1/restores" -H "$J" -d "{\"snapshot_id\": \"$snapshot\",
  \"target_dsn\": \"postgres://postgres:\${SHOP_DB_PASSWORD}@$PG_HOST/shop_restored?sslmode=disable\"}" | json 'd["id"]')
wait_job "$job"

q="SELECT count(*), md5(string_agg(id || status || amount_cents || note, ',' ORDER BY id)) FROM orders"
a=$($PSQL -At -d shop -c "$q")
b=$($PSQL -At -d shop_restored -c "$q")
echo "original: $a"
echo "restored: $b"
[ "$a" = "$b" ] || { echo "MISMATCH"; exit 1; }
echo "== restored data matches the original"
curl -sf "$API/metrics" | grep '^cbs_jobs'
