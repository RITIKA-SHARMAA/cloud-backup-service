# 60-second pitch

> "I built a small cloud-native backup service in Go, modelled on the problem cloud data
> protection products solve.
>
> It backs up PostgreSQL databases and folders into S3 with Object Lock, so even someone holding
> admin credentials cannot delete or change a backup during its retention period. That's the
> core defence against ransomware. Manifests also record the exact S3 version of every chunk, so
> overwriting a key doesn't change what gets restored.
>
> Data is split with content-defined chunking, named by a keyed hash, compressed and encrypted
> with AES-GCM. A chunk the vault already has is never uploaded again. On a million-row database,
> the first backup uploaded 31 MB, and after appending 10,000 rows the next one uploaded 319 KB.
> I also measured where it doesn't work: 100 scattered updates changed almost every chunk,
> because PostgreSQL moves updated rows. That's why real products use block-level incrementals.
>
> The work is distributed: an API, a cron scheduler and several workers share a job queue in
> PostgreSQL using SELECT FOR UPDATE SKIP LOCKED, leases with heartbeats, retries with backoff and
> idempotency keys. I tested it by killing a worker with kill -9 mid-backup, and another worker
> finished the job once the lease expired.
>
> It runs on Kubernetes with a Helm chart, and CI deploys it to a kind cluster and does a real
> backup and restore on every push. There are 52 tests, run with the race detector."

## If they ask "what would you do next?"

- Physical incrementals for update-heavy databases: WAL archiving or changed-block tracking.
- Upload chunks in parallel, and copy the vault to a second AWS account.
- Envelope encryption with KMS, so keys can rotate without re-encrypting data.
- Autoscale workers on queue depth with KEDA.
