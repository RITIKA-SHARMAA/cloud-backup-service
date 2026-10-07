# Learning guide

Each section explains one idea in plain words, points to the code, and ends with interview
questions and short answers. Read a section, open the file, and try to answer the questions
without looking.

---

## 1. Backups, snapshots and why "incremental" matters

A **snapshot** is the state of some data at one moment. A full copy every night is simple but
expensive: 100 GB a night is 3 TB a month. An **incremental** backup stores only what changed,
while still letting you restore any snapshot completely.

This project does that with **deduplication**: data is cut into chunks, each chunk is named by
its content, and a chunk that is already stored is never uploaded again. Every snapshot is a
full list of chunks (so it restores on its own), but most of those chunks are shared with
earlier snapshots.

Code: `internal/engine/backup.go` (`storeChunk`, `backupStream`).

**Q: What is the difference between incremental and differential backups?**
Incremental stores the changes since the *last backup of any kind*; differential stores the
changes since the *last full backup*. Restoring an incremental chain classically needs every
link. Chunk-level dedup avoids chains altogether: each manifest lists every chunk it needs.

**Q: What is RPO and RTO?**
RPO (recovery point objective) is how much data you can afford to lose, which is set by how often
you back up. RTO (recovery time objective) is how long a restore may take. Here, a nightly policy
gives an RPO of 24 hours. The 1M-row restore took 4.2 s on a local setup.

---

## 2. Content-defined chunking

**Fixed-size chunking** cuts every 1 MiB. Insert one byte at the start and every boundary
shifts, every chunk changes, and dedup finds nothing.

**Content-defined chunking** places a boundary wherever a *rolling hash* of the last few bytes
matches a pattern (here: certain bits are all zero). After an insert, boundaries shift only until
the hash re-synchronises at the next natural boundary. Everything after that is identical.

The rolling hash is a "gear" hash, as in FastCDC: `h = (h << 1) + gear[byte]`. Each step shifts
the old bytes further out, so after 64 bytes they have no effect: the hash depends only on a
sliding window. FastCDC also uses a harder-to-match mask before the target size and an easier
one after it, which keeps chunk sizes close to the target. Min and max sizes are hard limits.

Code: `internal/chunk/chunker.go`. The test `TestInsertNearStartKeepsMostChunks` shows a 20-byte
insert keeping almost every chunk, while fixed-size blocks lose all of them.

**Q: Why have a minimum and a maximum chunk size?**
Without a minimum, unlucky data gives tiny chunks: millions of objects, with metadata and request
costs. Without a maximum, data where the pattern never matches (long runs of zeros) gives one
huge chunk that no longer dedups.

**Q: What chunk size would you pick and why?**
It's a trade-off. Smaller chunks dedup better (a change re-uploads less) but mean more objects,
more S3 requests and bigger manifests. Around 1 MiB is a common middle ground for object storage.

**Q: Why must the gear table never change?**
Boundaries depend on it. A new table would give new boundaries for the same data, so nothing
already in the vault would match again.

---

## 3. Encryption: AES-256-GCM, nonces, and keyed chunk IDs

**AES-256-GCM** is *authenticated* encryption. It encrypts, and it adds a 16-byte tag, so any
change to the ciphertext (or the wrong key) makes decryption fail. That is how restores detect
corruption and tampering.

- **Nonce:** GCM needs a unique 12-byte nonce per encryption with the same key. Reusing one
  breaks both secrecy and authentication. Each chunk gets a random nonce, stored next to the
  ciphertext.
- **Additional authenticated data (AAD):** the chunk ID is passed as AAD. It is not encrypted,
  but it is covered by the tag, so a valid encrypted chunk cannot be moved to another name.
- **Compress before encrypting:** encrypted bytes look random and don't compress.
- **Keyed chunk IDs (HMAC-SHA256):** with plain SHA-256 names, anyone who can list the bucket
  could check whether a known file is backed up, by hashing it and looking for the name. With an
  HMAC under a secret key they cannot.

Code: `internal/crypto/codec.go`, `TestTamperedCiphertextIsRejected`,
`TestWrongKeyAndWrongAADAreRejected`.

**Q: What happens if the encryption key is lost?**
Every backup made with it is unreadable. That is the point of encryption, so the key must live
in a proper secret store (AWS KMS, Secrets Manager, a Kubernetes Secret backed by one) and be
backed up separately.

**Q: How would you rotate the key?**
Use envelope encryption: encrypt each chunk with a data key, and encrypt the data keys with a
master key in KMS. Rotation then only re-encrypts the small data keys. This project uses one key
directly, which is simpler but does not rotate cheaply.

---

## 4. S3, versioning and Object Lock (ransomware protection)

Ransomware attacks backups first, because deleting backups is what makes victims pay. The
defence is **immutability**: storage that refuses deletes and overwrites for a period.

- **Versioning:** S3 keeps every version of a key. "Overwriting" creates a new version and keeps
  the old one.
- **Object Lock:** each version can carry a *retain-until* date. Before that date the version
  cannot be deleted or altered.
  - **GOVERNANCE** mode: users with a special permission can override it.
  - **COMPLIANCE** mode: nobody can, including the account root user.
- Object Lock can only be enabled when a bucket is created (`EnsureBucket`).
- Manifests record each chunk's **version ID**, so even if an attacker uploads garbage under a
  chunk's key, restores read the original locked version.

Code: `internal/vault/s3.go`, `TestS3OverwriteKeepsLockedVersion`.

**Q: GOVERNANCE or COMPLIANCE for production?**
COMPLIANCE for real protection, since stolen admin credentials cannot undo it. GOVERNANCE is
useful in testing, where you need to clean up.

**Q: Can an attacker still hurt you with Object Lock on?**
They can stop new backups, upload junk to increase costs, or wait until locks expire. They also
might steal data rather than destroy it, which is why the data is encrypted. A further step is to
copy the vault to a separate AWS account the attacker doesn't control.

**Q: Why does `DeleteVersion` refuse an empty version ID?**
On a versioned bucket, a delete without a version ID only adds a "delete marker". The data is
still there, so pretending it was deleted would be misleading.

---

## 5. Manifests and crash safety

A backup uploads all chunks first and writes the **manifest last**. The manifest is the only
thing that makes a snapshot visible. So:

- A crash before the manifest leaves orphan chunks, but no broken snapshot.
- A crash after the manifest but before the job is marked done is handled by the retry. It uses
  the same snapshot ID (derived from the job), finds the manifest, and treats it as done. This is
  **idempotency**.

The same rule applies to files during restore: each is written as `name.partial` and renamed only
after its checksum matches. A rename within one filesystem is atomic.

Code: `engine.commit`, `engine.RestoreDir`, `TestCommitIsIdempotent`, `TestCorruptedChunkIsDetected`.

**Q: What does "idempotent" mean, and where does this project need it?**
Doing something twice has the same effect as doing it once. It is needed everywhere retries
happen: enqueueing a job (idempotency keys), committing a snapshot (deterministic IDs), and
recording a snapshot in the catalog (`ON CONFLICT DO NOTHING`).

---

## 6. A job queue in PostgreSQL: SKIP LOCKED, leases and heartbeats

Workers need to share a list of jobs without two of them doing the same one.

- **`FOR UPDATE SKIP LOCKED`:** a worker selects a runnable job and locks the row. Other workers
  *skip* locked rows instead of waiting, so they each get a different job straight away.
- **Leases:** a claimed job has `lease_owner` and `lease_expires_at`. The worker renews the lease
  (a **heartbeat**) every third of its length. If the worker dies, the lease expires and the job
  becomes claimable again. This is how crashes are handled without any coordinator.
- **Fencing by owner:** every update includes `WHERE lease_owner = me`. A worker that paused (say,
  a long GC pause) and lost its lease gets `ErrLeaseLost` and must stop. It cannot overwrite the
  new owner's result.
- **Retries with exponential backoff:** a failed job waits 2 s, 4 s, 8s ... (max 5 minutes)
  before running again, up to `max_attempts`, then becomes `failed`.
- **Graceful shutdown:** on SIGTERM the worker stops claiming. The running job gets a grace period
  to finish; otherwise it is cancelled and **released**, going back to the queue without using up
  an attempt.

Code: `internal/queue/queue.go`, `internal/worker/worker.go`.
Tests: `TestConcurrentWorkersRunEachJobExactlyOnce`, `TestCrashedWorkersJobIsFinishedByAnother`,
`TestLostLeaseCancelsTheJobWithoutReporting`, `TestShutdownFinishesOrReleasesTheRunningJob`.

**Q: Is this "exactly once"?**
Not strictly. A job can run twice if a worker loses its lease while still working, and the new
owner runs it again. What it guarantees is *at-least-once execution with idempotent effects*:
running a backup twice produces the same snapshot ID and reuses the same chunks, so the result
is the same. "Exactly-once effect" is the realistic goal in distributed systems.

**Q: How long should a lease be?**
Long enough that normal hiccups (a slow network call, a GC pause) don't lose it, and short enough
that a crashed worker's job is picked up quickly. Here it's 30 s with a heartbeat every 10 s, and
the measured takeover after `kill -9` took about 30 s.

**Q: Why not Kafka or SQS?**
The catalog database already exists, and a table gives transactions, unique keys, easy inspection
(`SELECT * FROM jobs`) and leases with plain SQL. A dedicated queue becomes worth it at much higher
volume, or when many independent services consume the same events.

---

## 7. The scheduler and cron

A policy has a cron schedule such as `0 2 * * *` (02:00 UTC every day) and a `next_run_at`. Every
5 s, the scheduler:

1. Locks due policies with `FOR UPDATE SKIP LOCKED`.
2. Enqueues a backup job with the key `policy-<id>-at-<due time>`.
3. Moves `next_run_at` to the next future time.

Running several schedulers is safe: the row locks stop two from handling the same policy, and
the idempotency key stops a duplicate even if a tick repeats. If the scheduler was down for a
while, it runs **one** catch-up backup rather than one per missed slot.

Code: `internal/scheduler/scheduler.go`, `TestDuePolicyIsEnqueuedOnceEvenWithConcurrentSchedulers`.

**Q: Why evaluate cron in UTC?**
Pods may run in any time zone, and daylight-saving changes make local times repeat or vanish.
UTC is unambiguous.

---

## 8. Garbage collection, and why it must not race with backups

GC is **mark and sweep**:

1. Delete manifests older than the cutoff, always keeping the newest snapshot of each source.
2. Mark every chunk referenced by the remaining manifests.
3. Delete the unmarked chunks.

Object Lock has the final say: a locked object cannot be deleted, so GC can never remove data
early, even with a wrong cutoff.

**The race:** a backup checks "does chunk X exist? yes, reuse it", then GC deletes X because no
committed manifest references it yet, and then the backup commits a manifest pointing at a
deleted chunk. To prevent it, backups take a *shared* PostgreSQL advisory lock and GC takes it
*exclusively* (and retries later if it can't).

Code: `internal/engine/gc.go`, `worker.handlers.lock`, `TestGCWaitsForRunningBackups`.

**Q: What is an advisory lock?**
A lock on a number, not on a row or table, that PostgreSQL holds for you. Applications decide
what the number means. Here it means "the vault's chunk set", and it belongs to the database
connection that took it.

---

## 9. PostgreSQL backups: pg_dump

`pg_dump --format=custom --compress=0` writes a consistent logical dump. It runs in a single
transaction, so the dump is a snapshot even while the database is being written to.
`pg_restore` loads it into an empty database.

- **`--compress=0`** because compressed output changes everywhere when a few rows change, which
  kills dedup. zstd is applied per chunk instead.
- The dump is **streamed**: `pg_dump` stdout goes straight into the chunker, so a 200 MB dump
  never touches local disk and memory stays at a few MB.
- **pg_dump's exit code is checked.** A dump that failed half way must not become a snapshot
  (`TestPostgresBackupOfMissingDatabaseFails`).
- **The measured limit:** updating 100 scattered rows changed 66 of 71 chunks. PostgreSQL's MVCC
  writes a new row version (often at the end of the table) instead of updating in place, and
  pg_dump outputs rows in physical order. Appends dedup very well (3 of 72 chunks). For
  update-heavy databases, physical backups (base backup plus WAL archiving) are the real answer.

**Q: Logical vs physical backup?**
A logical backup (pg_dump) is SQL-level data. It is portable across versions and can restore one
table, but it is slower for big databases and gives no point-in-time recovery. A physical backup
copies data files plus the write-ahead log (WAL). It is fast and allows point-in-time recovery,
but it is tied to the major version.

---

## 10. Kubernetes and Helm

- **Deployment:** keeps N identical pods running and replaces them on failure or upgrade.
  There are three here: api, scheduler, worker (3 replicas).
- **Service:** a stable name and IP in front of the API pods.
- **ConfigMap and Secret:** settings and credentials, injected as environment variables
  (`envFrom`). The encryption key belongs in a Secret, ideally synced from a cloud secret store.
- **Probes:**
  - *Readiness* decides whether a pod receives traffic.
  - *Liveness* decides whether it gets restarted.
  - Both call `/healthz`, which checks the database connection.
- **Graceful termination:** Kubernetes sends SIGTERM, waits `terminationGracePeriodSeconds` (40 s
  here), then sends SIGKILL. The worker's own grace (20 s) fits inside that window.
- **Helm:** templates plus `values.yaml`, so the same chart deploys to kind with demo values or
  to EKS with real ones.
- **The pod name is the worker ID** (downward API), so `lease_owner` tells you exactly which pod
  holds a job.

**Q: How would you scale this?**
Add worker replicas, since they share the queue with no coordination. Scale the API behind its
Service. The catalog PostgreSQL then becomes the bottleneck; a managed instance with a connection
pooler handles a lot before that matters. In a real cluster, an autoscaler (KEDA) could scale
workers on the `cbs_jobs{state="queued"}` metric.

**Q: How do workers get AWS access on EKS without static keys?**
IAM Roles for Service Accounts (IRSA) or EKS Pod Identity. The pod's service account maps to an
IAM role, and the SDK picks up temporary credentials automatically.

---

## 11. Observability

- **Structured logs:** every line is JSON (`log/slog`) with `job_id`, `snapshot_id`, `worker`
  and `attempt`, so you can filter one job's whole story across pods.
- **Metrics:**
  - `/metrics` in Prometheus format.
  - Jobs by state (a queue that keeps growing means too few workers).
  - Jobs finished by result.
  - Bytes uploaded vs logical bytes (the dedup ratio).
  - Chunks new vs reused.
- **Health:** `/healthz` for probes.

**Q: What would you alert on?**
- No successful backup for a policy within 2× its interval.
- Jobs failing repeatedly.
- The queued-jobs count rising for a long time.
- Verify jobs failing, which means corrupted data.
