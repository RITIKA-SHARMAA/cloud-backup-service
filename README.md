# cloud-backup-service

[![CI](https://github.com/RITIKA-SHARMAA/cloud-backup-service/actions/workflows/ci.yml/badge.svg)](https://github.com/RITIKA-SHARMAA/cloud-backup-service/actions/workflows/ci.yml)

A small cloud-native backup and restore service in Go. It backs up PostgreSQL databases and
directories into an S3 bucket protected by **Object Lock**, so backups cannot be deleted or
altered during their retention period. That is the core defence against ransomware.

- **Incremental and deduplicated.** Data is split into content-defined chunks, and a chunk the
  vault already has is never uploaded again.
- **Encrypted.** Every chunk is compressed with zstd and encrypted with AES-256-GCM before it
  leaves the machine.
- **Verified restores.** Every chunk is authenticated and every file is checksummed. A corrupted
  or tampered backup fails loudly instead of restoring bad data.
- **Distributed.** An API, a scheduler and any number of workers coordinate through a PostgreSQL
  job queue, using `SELECT ... FOR UPDATE SKIP LOCKED`, leases with heartbeats, retries with
  backoff, and idempotency keys.
- **Runs on Kubernetes** with a Helm chart. CI deploys it to a kind cluster and runs a full
  backup and restore on every push.

**Stack:** Go 1.24 · PostgreSQL · AWS S3 (aws-sdk-go-v2) with Object Lock · Docker ·
Kubernetes · Helm · GitHub Actions

## Results

Measured on a 2-vCPU VM with a local S3 server. See [docs/RESULTS.md](docs/RESULTS.md) for the
full setup.

| Backup of a 1M-row database | Dump | Uploaded | Chunks new / reused |
|---|---|---|---|
| First backup | 89.7 MiB | 31.5 MiB | 71 / 0 |
| After appending 10,000 rows | 90.6 MiB | 319 KiB | 3 / 69 |
| No changes since the last backup | 208.4 MiB | 29.5 KiB | 1 / 147 |
| After updating 100 rows spread across the table | 89.7 MiB | 31.0 MiB | 66 / 5 |

- The last row shows the limit of logical dumps; RESULTS.md explains why.
- A restore of 1,010,000 rows took 4.2 s and matched the source row for row.
- A worker killed with `kill -9` mid-backup had its job finished by another worker once the
  lease expired.

## Architecture

```mermaid
flowchart LR
  user["curl / cbs CLI"] --> api["api<br/>REST, /metrics"]
  api -->|"policies, jobs"| pg[("PostgreSQL catalog<br/>policies · jobs · snapshots")]
  sched["scheduler<br/>cron → jobs"] -->|"FOR UPDATE SKIP LOCKED"| pg
  w1["worker 1"] & w2["worker 2"] & w3["worker N"] -->|"claim with lease,<br/>heartbeat, retry"| pg
  w1 & w2 & w3 -->|"pg_dump / files"| src[("sources:<br/>PostgreSQL, directories")]
  w1 & w2 & w3 -->|"encrypted chunks,<br/>manifests last"| s3[("S3 vault<br/>versioning + Object Lock")]
```

### How a backup works

1. **Chunking** (`internal/chunk`): the source stream (a file, or `pg_dump` output) is cut where
   a rolling hash of the last 64 bytes matches a bit pattern (simplified FastCDC). Chunks are
   256 KiB to 4 MiB, about 1 MiB on average. Boundaries depend on content, not offsets, so an
   insert only changes the chunks around it.
2. **Naming** (`internal/crypto`): a chunk's ID is the HMAC-SHA256 of its content. Equal content
   gives an equal ID, which is how deduplication works. The HMAC is keyed so that someone who can
   list the bucket cannot test whether a known file is in it.
3. **Dedup check:** `HEAD chunks/ab/<id>`. If the chunk exists it is reused, otherwise it is
   uploaded.
4. **Seal:** zstd-compress, then encrypt with AES-256-GCM, with the chunk ID as additional
   authenticated data. Compression comes first because encrypted data doesn't compress.
5. **Upload with Object Lock:** each object gets a retain-until date. S3 returns a version ID,
   and the manifest records it.
6. **Manifest last:** the encrypted manifest (`snapshots/<id>.json`) lists the files, their
   SHA-256 and the exact chunk versions. A snapshot exists only once its manifest exists, so a
   crash mid-backup never leaves a half-written snapshot. It only leaves orphan chunks, which
   garbage collection removes after their lock expires.

### Why Object Lock, and why versions

With Object Lock (and the versioning it requires), even someone holding admin credentials cannot
delete or overwrite a locked object version before its retain-until date. In COMPLIANCE mode
that includes the AWS root user. An attacker can still *upload* a new version under the same
key, so manifests pin every chunk's **version ID** and restores read exactly that version. There
is a test for this: `TestS3OverwriteKeepsLockedVersion`.

### The job system

| Problem | Solution |
|---|---|
| N workers must not take the same job | `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)` |
| A worker crashes mid-job | Leases: the worker heartbeats every lease/3. If it stops, another worker takes the job when the lease expires. |
| The old worker wakes up later | Every update checks `lease_owner`, so the old worker gets `ErrLeaseLost` and reports nothing |
| A client retries "start backup" | `Idempotency-Key` header, enforced by a unique column, so the same job comes back |
| A retried backup after a crash | The snapshot ID is derived from the job, so the retry finds an already-committed manifest. Uploaded chunks are reused. |
| A broken dependency (S3 down) | Exponential backoff 2s, 4s, 8s ... up to 5 min, then `failed` after `max_attempts` |
| Kubernetes stops a pod (SIGTERM) | The running job gets 20 s to finish. Otherwise it is released back to the queue without using an attempt. |
| Several scheduler replicas | Due policies are locked with `SKIP LOCKED`, and job keys are `policy-<id>-at-<due time>` |
| GC deleting a chunk a running backup reuses | Backups hold a shared advisory lock; GC needs it exclusively and retries later |

## Quick start (Docker Compose)

```bash
docker compose up -d --build      # PostgreSQL, S3 (moto), api, scheduler, 2 workers
./scripts/demo.sh                 # backup twice, restore, compare every row
```

The demo database `shop` has 200,000 orders. `scripts/demo.sh` creates a policy, runs two
backups, restores the second into `shop_restored` and checks the data matches.

## API

```bash
# Create a policy. ${SHOP_DB_PASSWORD} is expanded by the worker from its environment
# (a Kubernetes Secret), so passwords are never stored in the catalog.
curl -X POST localhost:8080/v1/policies -H 'Content-Type: application/json' -d '{
  "name": "shop-db", "source_type": "postgres",
  "source": "postgres://postgres:${SHOP_DB_PASSWORD}@postgres:5432/shop?sslmode=disable",
  "schedule": "0 2 * * *", "retention_days": 30 }'

# Back up now. Repeat the same Idempotency-Key and you get the same job back.
curl -X POST localhost:8080/v1/policies/1/backups -H 'Idempotency-Key: 2026-10-07-manual'

curl localhost:8080/v1/jobs/1          # state, attempts, lease_owner, result, last_error
curl localhost:8080/v1/snapshots       # all snapshots with sizes and dedup stats
curl localhost:8080/v1/snapshots/ID    # files and checksums of one snapshot

curl -X POST localhost:8080/v1/restores -H 'Content-Type: application/json' \
  -d '{"snapshot_id": "ID", "target_dsn": "postgres://postgres:${SHOP_DB_PASSWORD}@postgres:5432/shop_restored?sslmode=disable"}'

curl -X POST localhost:8080/v1/gc -H 'Content-Type: application/json' -d '{"keep_days": 30}'
curl localhost:8080/metrics            # Prometheus: jobs by state, HTTP requests
```

Schedules are standard 5-field cron, evaluated in UTC. Workers expose `/metrics` on port 8082
with bytes uploaded, chunks new/reused, and jobs by result.

## CLI

`cbs` works directly against a vault, with no database needed:

```bash
go build -o bin/ ./cmd/...
export CBS_ENCRYPTION_KEY=$(bin/cbs keygen)        # keep this key safe: no key, no restore
export CBS_VAULT_URL=file://$PWD/vault             # or s3://bucket (+ CBS_S3_ENDPOINT)

bin/cbs backup --dir ~/Documents
bin/cbs backup --pg 'postgres://user:pass@localhost/shop'
bin/cbs snapshots
bin/cbs show SNAPSHOT_ID
bin/cbs verify SNAPSHOT_ID
bin/cbs restore SNAPSHOT_ID --to-dir /tmp/restored
bin/cbs gc --keep-days 30
```

## Kubernetes

```bash
docker build -t cloud-backup-service:dev .
kind create cluster --name cbs
kind load docker-image cloud-backup-service:dev --name cbs
kubectl apply -f deploy/kind/dependencies.yaml       # demo PostgreSQL + S3, no persistence
helm install demo deploy/helm/cloud-backup-service --wait
kubectl port-forward svc/demo-cbs-api 8080:8080 &
API=http://localhost:8080 PG_HOST=postgres:5432 \
  PSQL="kubectl exec -i deploy/postgres -- psql -U postgres" ./scripts/demo.sh
```

The chart deploys the API (Deployment + Service), the scheduler and 3 worker replicas, with
these settings:

- Configuration from a ConfigMap; the encryption key and credentials from a Secret
  (`secrets.existingSecret` for a real one).
- Liveness and readiness probes on `/healthz`.
- Resource requests and limits.
- Non-root containers.
- `terminationGracePeriodSeconds` longer than the worker's shutdown grace.

Each worker pod's name is its worker ID, so `lease_owner` on a job tells you which pod has it.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `CBS_DATABASE_URL` | | catalog database (api, scheduler, worker) |
| `CBS_VAULT_URL` | `file://./vault` | `s3://bucket` or `file:///path` |
| `CBS_S3_ENDPOINT` | AWS | S3-compatible endpoint, e.g. LocalStack or moto |
| `CBS_ENCRYPTION_KEY` | | 64 hex characters (`cbs keygen`) |
| `CBS_RETENTION` | `720h` | Object Lock retention of new objects |
| `CBS_LOCK_MODE` | `GOVERNANCE` | or `COMPLIANCE` (not even the root user can delete) |
| `CBS_LEASE` | `30s` | job lease; workers heartbeat every third of it |
| `CBS_WORKER_ID` | hostname | shown as `lease_owner` |
| `CBS_HTTP_ADDR` | `:8080` / `:8081` / `:8082` | api / scheduler / worker |

## Tests

```bash
go test -race ./...    # 28 unit tests run anywhere

# The 24 integration tests need PostgreSQL (with pg_dump) and an S3 server with Object Lock:
pip install "moto[server]" && moto_server -p 4566 &
CBS_TEST_PG_URL='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
CBS_TEST_S3_ENDPOINT=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
go test -race ./...
```

| Package | What is tested |
|---|---|
| `chunk` | reassembly, size limits, determinism, a 20-byte insert keeps almost all chunks (fixed-size blocks lose all) |
| `crypto` | round trip, tampering, wrong key, chunk swapped under another ID, keyed IDs |
| `vault` | the same contract on the local and S3 backends; an overwrite keeps the locked version; locked deletes refused |
| `engine` | directory and PostgreSQL backup and restore (every row compared), incremental uploads, corruption detected, path traversal refused, idempotent commit, encrypted manifests, GC respects locks |
| `queue` | idempotent enqueue, lease ownership, backoff and final failure, takeover after expiry, heartbeats, release |
| `worker` | 3 workers × 20 jobs run exactly once, crash takeover, lost lease cancels the job, graceful shutdown finishes or releases, backup/restore/gc jobs end to end, GC waits for backups |
| `scheduler` | 5 concurrent schedulers enqueue a due policy once |
| `api` | policy and job flow, Idempotency-Key, validation errors (400/404), health and metrics |

CI runs gofmt, go vet and the full suite with the race detector against PostgreSQL and moto. It
also runs `scripts/demo.sh` against both the Docker Compose stack and a kind cluster installed
with Helm.

## Design decisions

- **PostgreSQL as the job queue** instead of Kafka or SQS. The catalog already needs
  PostgreSQL, and a table gives transactions, unique idempotency keys and inspectable state with
  plain SQL. It is enough for thousands of jobs a minute; a very high-volume system would use a
  dedicated queue.
- **Logical dumps (`pg_dump --compress=0`).** pg_dump's own compression is turned off, because
  compressed output changes everywhere when a few rows change and would defeat dedup. Chunks are
  compressed after chunking instead.
- **A small dependency list:** aws-sdk-go-v2, lib/pq, klauspost/compress, robfig/cron. Metrics
  are written in the Prometheus text format by hand.

## Limits

- **Update-heavy databases deduplicate poorly** with logical dumps (see RESULTS.md). Production
  systems use physical, block-level incrementals for that.
- Chunks are uploaded one at a time per backup. Parallel uploads would help on real networks.
- One region, no cross-account copy. Real ransomware protection also copies the vault to a
  separate AWS account.
- Restores of PostgreSQL go into an existing empty database.
- GC deletes the latest version of unreferenced chunks. Older versions created by overwrites
  stay until removed by an S3 lifecycle rule.
- Results come from a single VM with a local S3 server, not from AWS.

See [docs/LEARNING.md](docs/LEARNING.md) for an explanation of every concept with interview
questions, and [docs/PITCH.md](docs/PITCH.md) for a 60-second summary.

## License

MIT
