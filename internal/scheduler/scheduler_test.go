package scheduler_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/queue"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/scheduler"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/testutil"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestDuePolicyIsEnqueuedOnceEvenWithConcurrentSchedulers(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	p := &catalog.Policy{Name: "nightly", SourceType: "dir", Source: "/data", Schedule: "0 2 * * *"}
	if err := catalog.CreatePolicy(ctx, db, p); err != nil {
		t.Fatal(err)
	}
	// Pretend the policy has been due for a while.
	db.Exec(`UPDATE policies SET next_run_at = now() - interval '1 minute'`)

	// Five scheduler replicas tick at the same moment.
	now := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := scheduler.Tick(ctx, db, now, quiet)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	jobs, _ := queue.List(ctx, db, 10)
	if total != 1 || len(jobs) != 1 || jobs[0].Kind != queue.KindBackup || *jobs[0].PolicyID != p.ID {
		t.Fatalf("want exactly one backup job, got total=%d jobs=%d", total, len(jobs))
	}

	got, _ := catalog.GetPolicy(ctx, db, p.ID)
	if !got.NextRunAt.After(now) || got.NextRunAt.UTC().Hour() != 2 || got.NextRunAt.UTC().Minute() != 0 {
		t.Fatalf("next run not advanced to the next 02:00: %v", got.NextRunAt)
	}
	// Ticking again before the next run creates nothing.
	if n, _ := scheduler.Tick(ctx, db, now, quiet); n != 0 {
		t.Fatalf("second tick enqueued %d jobs", n)
	}
}

func TestPolicyValidation(t *testing.T) {
	db := testutil.Catalog(t)
	ctx := context.Background()
	bad := []catalog.Policy{
		{Name: "", SourceType: "dir", Source: "/x", Schedule: "* * * * *"},
		{Name: "a", SourceType: "ftp", Source: "/x", Schedule: "* * * * *"},
		{Name: "a", SourceType: "dir", Source: "", Schedule: "* * * * *"},
		{Name: "a", SourceType: "dir", Source: "/x", Schedule: "every day"},
	}
	for _, p := range bad {
		if err := catalog.CreatePolicy(ctx, db, &p); err == nil {
			t.Fatalf("policy %+v should be rejected", p)
		}
	}
	ok := catalog.Policy{Name: "a", SourceType: "dir", Source: "/x", Schedule: "*/5 * * * *"}
	if err := catalog.CreatePolicy(ctx, db, &ok); err != nil || ok.RetentionDays != 30 {
		t.Fatalf("valid policy rejected: %v", err)
	}
	dup := ok
	if err := catalog.CreatePolicy(ctx, db, &dup); err == nil {
		t.Fatal("duplicate name should be rejected")
	}
}
