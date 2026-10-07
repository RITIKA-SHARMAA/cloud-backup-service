// Package worker runs jobs from the queue: claim, keep the lease alive, execute, report.
package worker

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/metrics"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/queue"
)

// Handler executes one job and returns a JSON-serialisable result. It must stop promptly when
// ctx is cancelled (lease lost, or shutdown grace period over).
type Handler func(ctx context.Context, job *queue.Job, log *slog.Logger) (any, error)

// Worker is one process's job loop. Run several replicas for throughput and availability.
type Worker struct {
	DB      *sql.DB
	ID      string // unique per process, e.g. the pod name
	Handler Handler
	Lease   time.Duration // how long a claim is valid without a heartbeat
	Poll    time.Duration // how long to sleep when the queue is empty
	// ShutdownGrace is how long a running job may continue after shutdown starts. If it has
	// not finished by then it is cancelled and released back to the queue for another worker.
	ShutdownGrace time.Duration
	Log           *slog.Logger
	Metrics       *metrics.Registry
}

func (w *Worker) defaults() {
	if w.Lease == 0 {
		w.Lease = 30 * time.Second
	}
	if w.Poll == 0 {
		w.Poll = time.Second
	}
	if w.ShutdownGrace == 0 {
		w.ShutdownGrace = 20 * time.Second
	}
	if w.Log == nil {
		w.Log = slog.Default()
	}
	if w.Metrics == nil {
		w.Metrics = metrics.New()
	}
}

// Run claims and executes jobs until ctx is cancelled (e.g. on SIGTERM), then returns once the
// current job has finished or been released.
func (w *Worker) Run(ctx context.Context) error {
	w.defaults()
	w.Log.Info("worker started", "worker", w.ID, "lease", w.Lease.String())
	for {
		if ctx.Err() != nil {
			w.Log.Info("worker stopped", "worker", w.ID)
			return nil
		}
		job, err := queue.Claim(ctx, w.DB, w.ID, w.Lease)
		if err != nil {
			if ctx.Err() == nil {
				w.Log.Error("claim failed", "error", err)
				sleep(ctx, w.Poll)
			}
			continue
		}
		if job == nil {
			sleep(ctx, w.Poll)
			continue
		}
		w.process(ctx, job)
	}
}

// RunOnce claims and runs at most one job; it reports whether there was one. Used by tests.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	w.defaults()
	job, err := queue.Claim(ctx, w.DB, w.ID, w.Lease)
	if err != nil || job == nil {
		return false, err
	}
	w.process(ctx, job)
	return true, nil
}

func (w *Worker) process(stop context.Context, job *queue.Job) {
	log := w.Log.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempts, "worker", w.ID)
	log.Info("job started")
	start := time.Now()

	// The job's own context is NOT derived from stop: on shutdown the job gets ShutdownGrace
	// to finish. It is cancelled early if the lease is lost.
	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leaseLost := make(chan struct{})
	go w.heartbeat(jobCtx, job.ID, cancel, leaseLost, log)
	go func() {
		select {
		case <-stop.Done():
			log.Info("shutdown requested, giving the running job a grace period", "grace", w.ShutdownGrace.String())
			select {
			case <-time.After(w.ShutdownGrace):
				cancel()
			case <-jobCtx.Done():
			}
		case <-jobCtx.Done():
		}
	}()

	result, err := w.Handler(jobCtx, job, log)
	shuttingDown := stop.Err() != nil && jobCtx.Err() != nil
	cancel()

	// Reporting uses a fresh short context: the job's context may already be cancelled.
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rcancel()
	select {
	case <-leaseLost:
		// Another worker owns the job now. Do not report anything: its result wins.
		log.Warn("lease lost; abandoning job without reporting")
		w.Metrics.Add("cbs_jobs_total", 1, "kind", job.Kind, "result", "lease_lost")
		return
	default:
	}
	switch {
	case err == nil:
		err = queue.Complete(rctx, w.DB, job.ID, w.ID, result)
		w.count(job, "succeeded", err, log)
		log.Info("job succeeded", "duration_ms", time.Since(start).Milliseconds())
	case shuttingDown:
		err = queue.Release(rctx, w.DB, job.ID, w.ID)
		w.count(job, "released", err, log)
		log.Info("job released for another worker")
	default:
		log.Error("job failed", "error", err.Error(), "duration_ms", time.Since(start).Milliseconds())
		err = queue.Fail(rctx, w.DB, job.ID, w.ID, err)
		w.count(job, "failed", err, log)
	}
}

func (w *Worker) count(job *queue.Job, result string, reportErr error, log *slog.Logger) {
	if reportErr != nil {
		log.Error("could not record job result", "result", result, "error", reportErr)
		if errors.Is(reportErr, queue.ErrLeaseLost) {
			result = "lease_lost"
		}
	}
	w.Metrics.Add("cbs_jobs_total", 1, "kind", job.Kind, "result", result)
}

// heartbeat renews the lease at a third of its length, so two missed beats still leave time.
// If renewal reports the lease is gone, the job is cancelled.
func (w *Worker) heartbeat(ctx context.Context, id int64, cancel context.CancelFunc, lost chan<- struct{}, log *slog.Logger) {
	t := time.NewTicker(w.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := queue.Heartbeat(ctx, w.DB, id, w.ID, w.Lease)
			if errors.Is(err, queue.ErrLeaseLost) {
				close(lost)
				cancel()
				return
			}
			if err != nil && ctx.Err() == nil {
				log.Warn("heartbeat failed, will retry", "error", err)
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
