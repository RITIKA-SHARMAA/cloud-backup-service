// Package scheduler turns policies into backup jobs when their cron schedule is due.
package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/queue"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/worker"
)

// Tick enqueues one backup job for every policy whose next_run_at has passed, then moves
// next_run_at forward. It returns how many jobs it created.
//
// Safe with several scheduler replicas: policies are locked with FOR UPDATE SKIP LOCKED, so two
// schedulers never handle the same policy at once, and the job's idempotency key is derived from
// the policy and its due time, so even a repeated tick cannot enqueue the same run twice.
func Tick(ctx context.Context, db *sql.DB, now time.Time, log *slog.Logger) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT id, schedule, next_run_at FROM policies
		WHERE next_run_at <= $1 ORDER BY next_run_at FOR UPDATE SKIP LOCKED`, now)
	if err != nil {
		return 0, err
	}
	type due struct {
		id       int64
		schedule string
		at       time.Time
	}
	var list []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.id, &d.schedule, &d.at); err != nil {
			rows.Close()
			return 0, err
		}
		list = append(list, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	created := 0
	for _, d := range list {
		pid := d.id
		job, isNew, err := queue.Enqueue(ctx, tx, queue.NewJob{
			Kind: queue.KindBackup, PolicyID: &pid, Payload: worker.BackupPayload{PolicyID: d.id},
			IdempotencyKey: fmt.Sprintf("policy-%d-at-%d", d.id, d.at.Unix()),
		})
		if err != nil {
			return 0, err
		}
		if isNew {
			created++
			log.Info("backup scheduled", "policy_id", d.id, "job_id", job.ID, "due", d.at)
		}
		// If the scheduler was down for a while, skip the missed runs and schedule the next
		// future one: one catch-up backup is enough, not one per missed slot.
		next, err := catalog.NextRun(d.schedule, now)
		if err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE policies SET next_run_at = $2 WHERE id = $1`, d.id, next); err != nil {
			return 0, err
		}
	}
	return created, tx.Commit()
}

// Run calls Tick every interval until ctx is cancelled.
func Run(ctx context.Context, db *sql.DB, interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := Tick(ctx, db, time.Now(), log); err != nil && ctx.Err() == nil {
			log.Error("scheduler tick failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
