# Measured results

Every number here comes from one real run, on 2026-10-07. Nothing is estimated.

## Setup

| | |
|---|---|
| Machine | Linux VM, Intel Xeon @ 2.10 GHz, 2 vCPUs, 7 GiB RAM, Ubuntu 24.04 |
| Software | Go 1.24.7, PostgreSQL 16.15, pg_dump 16 |
| S3 | moto 5.2.3 (S3 API with versioning and Object Lock) on the same machine, so **no network latency** is included |
| Deployment | `api`, `scheduler` and **two `worker` processes** against one catalog database, chunk target 1 MiB |
| Source | demo database `shop`: 100,000 customers and 1,000,000 orders, 157 MB on disk |

## Backups of a PostgreSQL database

| # | What changed before the backup | Dump size | New chunks | Reused chunks | Uploaded | Time |
|---|---|---|---|---|---|---|
| 1 | first backup | 89.7 MiB | 71 | 0 | 31.5 MiB | 2.58 s |
| 2 | 100 rows updated, spread evenly across the table | 89.7 MiB | 66 | 5 | 31.0 MiB | 2.37 s |
| 3 | 10,000 rows appended | 90.6 MiB | 3 | 69 | **319 KiB** | 1.07 s |
| 4 | nothing since the previous backup (table now at 1,410,000 orders) | 208.4 MiB | 1 | 147 | **29.5 KiB** | 2.0 s |

Uploaded bytes are after zstd compression and AES-256-GCM encryption.

### What the numbers show

- **Compression:** the first backup stored 89.7 MiB of dump as 31.5 MiB.
- **Appends deduplicate very well:** adding 10,000 rows re-uploaded 3 chunks out of 72, which is
  0.35% of the data.
- **An unchanged database still produces one new chunk:** pg_dump writes the time of the dump
  into the archive header, so the first chunk always differs. Everything else was reused.
- **Scattered updates deduplicate badly, and this is the most interesting result.** Updating
  100 rows spread across the table changed 66 of 71 chunks. Two reasons:
  1. One chunk holds about 14,000 rows of the dump, and 100 evenly spread updates touch most chunks.
  2. PostgreSQL never updates a row in place (MVCC): it writes a new row version, often at the
     end of the table. pg_dump outputs rows in physical order, so updated rows also *move* in the dump.

  Logical dumps plus content-defined chunking suit append-heavy data. For update-heavy databases,
  real backup products use physical, block-level incrementals (WAL archiving or changed-block
  tracking) instead. This project does not implement that (see Limits in the README).

## Restore

| Snapshot | Restored into | Rows | Time | Check |
|---|---|---|---|---|
| backup 3 | empty database `shop_restored` | 1,010,000 orders, 100,000 customers | 4.21 s | md5 over every row of both tables identical to the source |

The restore job ran through the API (`POST /v1/restores`) and was executed by a worker.

## Verify

`cbs verify` of the 208.4 MiB snapshot (download, authenticate and re-hash every chunk, check the
file checksum): **1.44 s**.

## Worker crash during a backup

1. A backup job was started, and its worker (`worker-2`) was killed with `kill -9` about one
   second in, mid-upload.
2. `worker-1` took the job over when the 30 s lease expired. The job's `attempts` became 2.
3. The retry finished in 2.5 s and committed one snapshot. Chunks the dead worker had already
   uploaded were simply reused, because chunks are content-addressed.
4. The job finished 33 s after it was started, which is the lease timeout plus the work.

Afterwards, garbage collection reported one object that no snapshot references but is still
under Object Lock retention: an orphan of the interrupted attempt, kept until its lock expires.

## Directory backup (CLI)

`cbs backup --dir` on this repository's `internal/` folder (133.5 KiB of source code) uploaded
55.0 KiB in 30 chunks. `cbs restore --to-dir` reproduced it exactly (`diff -r` showed no difference).

## Tests

52 tests (`go test -race ./...`): 28 unit tests, plus 24 integration tests that run against
real PostgreSQL and S3 when `CBS_TEST_PG_URL` and `CBS_TEST_S3_ENDPOINT` are set. Statement
coverage of `internal/...` is 76.5%.
