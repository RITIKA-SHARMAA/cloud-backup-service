package worker_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/chunk"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/crypto"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/engine"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/metrics"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/queue"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/testutil"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/vault"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/worker"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func enqueueN(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, _, err := queue.Enqueue(context.Background(), db, queue.NewJob{Kind: queue.KindGC, Payload: i, IdempotencyKey: fmt.Sprint("job-", i)}); err != nil {
			t.Fatal(err)
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func jobState(t *testing.T, db *sql.DB, id int64) *queue.Job {
	t.Helper()
	j, err := queue.Get(context.Background(), db, id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// Three workers drain twenty jobs concurrently; every job must run exactly once.
func TestConcurrentWorkersRunEachJobExactlyOnce(t *testing.T) {
	db := testutil.Catalog(t)
	enqueueN(t, db, 20)

	var mu sync.Mutex
	runs := map[int64]int{}
	handler := func(ctx context.Context, j *queue.Job, _ *slog.Logger) (any, error) {
		mu.Lock()
		runs[j.ID]++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond) // long enough for workers to overlap
		return "ok", nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		w := &worker.Worker{DB: db, ID: fmt.Sprint("w", i), Handler: handler, Poll: 10 * time.Millisecond, Log: quiet}
		wg.Add(1)
		go func() { defer wg.Done(); w.Run(ctx) }()
	}
	waitFor(t, "all jobs to succeed", func() bool {
		c, _ := queue.CountByState(context.Background(), db)
		return c[queue.Succeeded] == 20
	})
	cancel()
	wg.Wait()

	if len(runs) != 20 {
		t.Fatalf("%d distinct jobs ran, want 20", len(runs))
	}
	for id, n := range runs {
		if n != 1 {
			t.Fatalf("job %d ran %d times", id, n)
		}
	}
}

// A worker that crashes mid-job (simulated by claiming and never heartbeating) loses the job
// to a healthy worker once the lease expires.
func TestCrashedWorkersJobIsFinishedByAnother(t *testing.T) {
	db := testutil.Catalog(t)
	enqueueN(t, db, 1)
	crashed, err := queue.Claim(context.Background(), db, "crashed", 300*time.Millisecond)
	if err != nil || crashed == nil {
		t.Fatal("setup claim failed")
	}

	var ran atomic.Int32
	w := &worker.Worker{DB: db, ID: "healthy", Lease: time.Minute, Poll: 20 * time.Millisecond, Log: quiet,
		Handler: func(context.Context, *queue.Job, *slog.Logger) (any, error) { ran.Add(1); return "done", nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	waitFor(t, "takeover", func() bool { return jobState(t, db, crashed.ID).State == queue.Succeeded })
	j := jobState(t, db, crashed.ID)
	if ran.Load() != 1 || j.Attempts != 2 {
		t.Fatalf("ran %d times, attempts %d", ran.Load(), j.Attempts)
	}
}

// If a worker's lease is taken away (it was too slow to heartbeat, e.g. a long GC pause), its
// job context is cancelled and it reports nothing; the new owner's outcome stands.
func TestLostLeaseCancelsTheJobWithoutReporting(t *testing.T) {
	db := testutil.Catalog(t)
	enqueueN(t, db, 1)
	started := make(chan int64, 1)
	cancelled := make(chan struct{})
	m := metrics.New()
	w := &worker.Worker{DB: db, ID: "slow", Lease: 300 * time.Millisecond, Log: quiet, Metrics: m,
		Handler: func(ctx context.Context, j *queue.Job, _ *slog.Logger) (any, error) {
			started <- j.ID
			<-ctx.Done()
			close(cancelled)
			return nil, ctx.Err()
		}}
	go w.RunOnce(context.Background())
	id := <-started
	// Another worker steals the job, as if the lease had expired.
	if _, err := db.Exec(`UPDATE jobs SET lease_owner = 'thief' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("job was not cancelled after losing its lease")
	}
	waitFor(t, "lease_lost metric", func() bool { return m.Get("cbs_jobs_total", "kind", "gc", "result", "lease_lost") == 1 })
	if j := jobState(t, db, id); j.State != queue.Running || *j.LeaseOwner != "thief" {
		t.Fatalf("the old owner must not change the job: %+v", j)
	}
}

func TestFailingJobIsRequeuedWithError(t *testing.T) {
	db := testutil.Catalog(t)
	enqueueN(t, db, 1)
	w := &worker.Worker{DB: db, ID: "w", Log: quiet,
		Handler: func(context.Context, *queue.Job, *slog.Logger) (any, error) { return nil, errors.New("S3 unavailable") }}
	if ok, err := w.RunOnce(context.Background()); !ok || err != nil {
		t.Fatal(ok, err)
	}
	j, _ := queue.List(context.Background(), db, 1)
	if j[0].State != queue.Queued || *j[0].LastError != "S3 unavailable" {
		t.Fatalf("%+v", j[0])
	}
}

// On SIGTERM a job that finishes within the grace period completes normally; one that does not
// is cancelled and released so another worker can pick it up immediately.
func TestShutdownFinishesOrReleasesTheRunningJob(t *testing.T) {
	for _, c := range []struct {
		name      string
		jobTakes  time.Duration
		wantState string
		wantTries int
	}{
		{"finishes within grace", 100 * time.Millisecond, queue.Succeeded, 1},
		{"too slow, released", time.Hour, queue.Queued, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			db := testutil.Catalog(t)
			enqueueN(t, db, 1)
			started := make(chan int64, 1)
			w := &worker.Worker{DB: db, ID: "w", ShutdownGrace: 300 * time.Millisecond, Log: quiet,
				Handler: func(ctx context.Context, j *queue.Job, _ *slog.Logger) (any, error) {
					started <- j.ID
					select {
					case <-time.After(c.jobTakes):
						return "done", nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}}
			stop, sigterm := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { w.Run(stop); close(done) }()
			id := <-started
			sigterm()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not stop")
			}
			if j := jobState(t, db, id); j.State != c.wantState || j.Attempts != c.wantTries {
				t.Fatalf("state %s attempts %d, want %s %d", j.State, j.Attempts, c.wantState, c.wantTries)
			}
		})
	}
}

// The production handlers end to end: a policy is backed up twice by the worker (the second
// time incrementally), restored by a restore job, and garbage-collected by a gc job.
func TestBackupRestoreAndGCJobs(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	codec, _ := crypto.NewCodec(bytes.Repeat([]byte{3}, 32))
	eng := engine.New(&vault.Local{Root: t.TempDir()}, codec)
	eng.Chunking = chunk.Config{Min: 2 << 10, Avg: 8 << 10, Max: 32 << 10}
	m := metrics.New()
	w := &worker.Worker{DB: db, ID: "w", Handler: worker.NewHandler(db, eng, m), Log: quiet, Metrics: m}

	src := t.TempDir()
	data := bytes.Repeat([]byte("customer record\n"), 20000)
	os.WriteFile(filepath.Join(src, "data.txt"), data, 0o644)
	pol := &catalog.Policy{Name: "files", SourceType: "dir", Source: src, Schedule: "0 2 * * *"}
	if err := catalog.CreatePolicy(ctx, db, pol); err != nil {
		t.Fatal(err)
	}

	runJob := func(kind, key string, payload any) *queue.Job {
		t.Helper()
		j, _, err := queue.Enqueue(ctx, db, queue.NewJob{Kind: kind, PolicyID: &pol.ID, Payload: payload, IdempotencyKey: key})
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := w.RunOnce(ctx); !ok || err != nil {
			t.Fatalf("RunOnce: %v %v", ok, err)
		}
		j = jobState(t, db, j.ID)
		if j.State != queue.Succeeded {
			t.Fatalf("%s job ended %s: %v", kind, j.State, *j.LastError)
		}
		return j
	}

	first := runJob(queue.KindBackup, "b1", worker.BackupPayload{PolicyID: pol.ID})
	runJob(queue.KindBackup, "b2", worker.BackupPayload{PolicyID: pol.ID})
	snaps, _ := catalog.ListSnapshots(ctx, db)
	if len(snaps) != 2 || snaps[1].NewChunks != 0 || snaps[1].ReusedChunks == 0 {
		t.Fatalf("second backup should be fully deduplicated: %+v", snaps)
	}
	if m.Get("cbs_chunks_total", "type", "reused") == 0 {
		t.Fatal("metrics not recorded")
	}

	var res struct {
		SnapshotID string `json:"snapshot_id"`
	}
	json.Unmarshal(first.Result, &res)
	if res.SnapshotID != worker.SnapshotIDForJob(first) {
		t.Fatalf("snapshot id %q", res.SnapshotID)
	}
	dest := t.TempDir()
	runJob(queue.KindRestore, "r1", worker.RestorePayload{SnapshotID: res.SnapshotID, TargetDir: dest})
	if got, _ := os.ReadFile(filepath.Join(dest, "data.txt")); !bytes.Equal(got, data) {
		t.Fatal("restored data differs")
	}

	gc := runJob(queue.KindGC, "gc1", worker.GCPayload{KeepDays: 1})
	if string(gc.Result) == "" {
		t.Fatal("gc should report its result")
	}
}

// GC must wait while a backup holds the shared lock.
func TestGCWaitsForRunningBackups(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	codec, _ := crypto.NewCodec(bytes.Repeat([]byte{3}, 32))
	eng := engine.New(&vault.Local{Root: t.TempDir()}, codec)
	w := &worker.Worker{DB: db, ID: "w", Handler: worker.NewHandler(db, eng, metrics.New()), Log: quiet}

	conn, _ := db.Conn(ctx)
	defer conn.Close()
	conn.ExecContext(ctx, `SELECT pg_advisory_lock_shared(72202)`) // a backup in progress

	j, _, _ := queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: worker.GCPayload{KeepDays: 1}, IdempotencyKey: "gc"})
	w.RunOnce(ctx)
	got := jobState(t, db, j.ID)
	if got.State != queue.Queued || got.LastError == nil {
		t.Fatalf("gc should be postponed while a backup runs: %s", got.State)
	}
}
