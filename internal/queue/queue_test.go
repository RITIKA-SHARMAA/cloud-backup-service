package queue_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/queue"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/testutil"
)

func TestEnqueueIsIdempotent(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	a, created, err := queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 1, IdempotencyKey: "k1"})
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	b, created, err := queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 2, IdempotencyKey: "k1"})
	if err != nil || created || b.ID != a.ID {
		t.Fatalf("second enqueue should return job %d unchanged, got %d created=%v err=%v", a.ID, b.ID, created, err)
	}
	if _, _, err := queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC}); err == nil {
		t.Fatal("missing idempotency key should be rejected")
	}
}

func TestClaimCompleteAndEmptyQueue(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	if j, err := queue.Claim(ctx, db, "w1", time.Minute); j != nil || err != nil {
		t.Fatalf("empty queue: %v %v", j, err)
	}
	queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 1, IdempotencyKey: "k"})
	j, err := queue.Claim(ctx, db, "w1", time.Minute)
	if err != nil || j == nil || j.State != queue.Running || j.Attempts != 1 || *j.LeaseOwner != "w1" {
		t.Fatalf("claim: %+v %v", j, err)
	}
	if again, _ := queue.Claim(ctx, db, "w2", time.Minute); again != nil {
		t.Fatal("a leased job was handed out twice")
	}
	if err := queue.Complete(ctx, db, j.ID, "w2", "x"); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("a worker that does not own the job must not complete it: %v", err)
	}
	if err := queue.Complete(ctx, db, j.ID, "w1", map[string]int{"n": 1}); err != nil {
		t.Fatal(err)
	}
	got, _ := queue.Get(ctx, db, j.ID)
	if got.State != queue.Succeeded || string(got.Result) != `{"n": 1}` {
		t.Fatalf("after complete: %s %s", got.State, got.Result)
	}
}

func TestFailRetriesWithBackoffThenGivesUp(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	j, _, _ := queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 1, IdempotencyKey: "k", MaxAttempts: 2})

	c, _ := queue.Claim(ctx, db, "w", time.Minute)
	if err := queue.Fail(ctx, db, c.ID, "w", errors.New("disk full")); err != nil {
		t.Fatal(err)
	}
	got, _ := queue.Get(ctx, db, j.ID)
	if got.State != queue.Queued || *got.LastError != "disk full" || !got.RunAfter.After(time.Now()) {
		t.Fatalf("after first failure: state %s run_after %v", got.State, got.RunAfter)
	}
	if again, _ := queue.Claim(ctx, db, "w", time.Minute); again != nil {
		t.Fatal("job was retried before its backoff elapsed")
	}

	db.Exec(`UPDATE jobs SET run_after = now() WHERE id = $1`, j.ID) // skip the wait
	c, _ = queue.Claim(ctx, db, "w", time.Minute)
	if c == nil || c.Attempts != 2 {
		t.Fatalf("second attempt: %+v", c)
	}
	queue.Fail(ctx, db, c.ID, "w", errors.New("disk still full"))
	got, _ = queue.Get(ctx, db, j.ID)
	if got.State != queue.Failed {
		t.Fatalf("after max attempts: %s", got.State)
	}
}

// A worker that crashes stops heartbeating. Once its lease expires another worker takes the
// job, and the crashed worker can no longer report on it.
func TestExpiredLeaseIsTakenOver(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 1, IdempotencyKey: "k"})

	a, _ := queue.Claim(ctx, db, "crashed-worker", 300*time.Millisecond)
	if b, _ := queue.Claim(ctx, db, "worker-b", time.Minute); b != nil {
		t.Fatal("job taken over before the lease expired")
	}
	time.Sleep(400 * time.Millisecond)
	b, err := queue.Claim(ctx, db, "worker-b", time.Minute)
	if err != nil || b == nil || b.ID != a.ID || b.Attempts != 2 {
		t.Fatalf("takeover: %+v %v", b, err)
	}
	if err := queue.Heartbeat(ctx, db, a.ID, "crashed-worker", time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("old owner's heartbeat should fail: %v", err)
	}
	if err := queue.Complete(ctx, db, a.ID, "worker-b", nil); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatKeepsTheLease(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 1, IdempotencyKey: "k"})
	a, _ := queue.Claim(ctx, db, "w1", 300*time.Millisecond)
	for i := 0; i < 4; i++ {
		time.Sleep(150 * time.Millisecond)
		if err := queue.Heartbeat(ctx, db, a.ID, "w1", 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if b, _ := queue.Claim(ctx, db, "w2", time.Minute); b != nil {
		t.Fatal("a job with a live heartbeat was taken over")
	}
}

func TestFinalAttemptWithExpiredLeaseIsMarkedFailed(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	j, _, _ := queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 1, IdempotencyKey: "k", MaxAttempts: 1})
	queue.Claim(ctx, db, "dies", 100*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	if b, _ := queue.Claim(ctx, db, "w2", time.Minute); b != nil {
		t.Fatal("a job with no attempts left was handed out again")
	}
	got, _ := queue.Get(ctx, db, j.ID)
	if got.State != queue.Failed {
		t.Fatalf("state %s, want failed", got.State)
	}
}

func TestReleaseDoesNotCountTheAttempt(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	j, _, _ := queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 1, IdempotencyKey: "k"})
	c, _ := queue.Claim(ctx, db, "w", time.Minute)
	if err := queue.Release(ctx, db, c.ID, "w"); err != nil {
		t.Fatal(err)
	}
	got, _ := queue.Get(ctx, db, j.ID)
	if got.State != queue.Queued || got.Attempts != 0 || got.LeaseOwner != nil {
		t.Fatalf("after release: %+v", got)
	}
}

func TestCountByStateAndList(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		queue.Enqueue(ctx, db, queue.NewJob{Kind: queue.KindGC, Payload: 1, IdempotencyKey: fmt.Sprint(i)})
	}
	queue.Claim(ctx, db, "w", time.Minute)
	counts, err := queue.CountByState(ctx, db)
	if err != nil || counts[queue.Queued] != 2 || counts[queue.Running] != 1 || counts[queue.Failed] != 0 {
		t.Fatalf("counts %v %v", counts, err)
	}
	if jobs, _ := queue.List(ctx, db, 2); len(jobs) != 2 || jobs[0].ID < jobs[1].ID {
		t.Fatal("List should return the newest jobs first")
	}
}
