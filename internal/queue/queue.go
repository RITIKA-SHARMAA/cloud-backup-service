// Package queue is a job queue stored in a PostgreSQL table.
//
// Why PostgreSQL and not Kafka or SQS? The catalog already needs PostgreSQL, and a table gives
// transactions, idempotency keys and leases with plain SQL. The key trick is
// SELECT ... FOR UPDATE SKIP LOCKED: each worker locks the row it picks, and other workers
// skip locked rows instead of waiting, so N workers take N different jobs without
// coordinating and without two of them ever taking the same one.
package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
)

// Job kinds.
const (
	KindBackup  = "backup"
	KindRestore = "restore"
	KindGC      = "gc"
)

// Job states.
const (
	Queued    = "queued"
	Running   = "running"
	Succeeded = "succeeded"
	Failed    = "failed"
)

// ErrLeaseLost means this worker no longer owns the job: its lease expired and another worker
// took it over. The old worker must stop and must not report a result.
var ErrLeaseLost = errors.New("queue: lease lost")

// Job is one row of the jobs table.
type Job struct {
	ID             int64           `json:"id"`
	Kind           string          `json:"kind"`
	PolicyID       *int64          `json:"policy_id,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	State          string          `json:"state"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAfter       time.Time       `json:"run_after"`
	LeaseOwner     *string         `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

const cols = `id, kind, policy_id, payload, idempotency_key, state, attempts, max_attempts, run_after,
	lease_owner, lease_expires_at, last_error, result, created_at, updated_at`

func scan(row interface{ Scan(...any) error }) (*Job, error) {
	var j Job
	var result []byte
	err := row.Scan(&j.ID, &j.Kind, &j.PolicyID, &j.Payload, &j.IdempotencyKey, &j.State, &j.Attempts,
		&j.MaxAttempts, &j.RunAfter, &j.LeaseOwner, &j.LeaseExpiresAt, &j.LastError, &result, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return nil, err
	}
	j.Result = result
	return &j, nil
}

// NewJob describes a job to enqueue.
type NewJob struct {
	Kind           string
	PolicyID       *int64
	Payload        any
	IdempotencyKey string
	MaxAttempts    int // default 5
}

// Enqueue inserts a job unless one with the same idempotency key exists, and returns the job
// either way. created reports whether this call inserted it. This makes "trigger a backup"
// safe to retry: a client that times out and sends the request again gets the same job.
func Enqueue(ctx context.Context, q catalog.Querier, n NewJob) (job *Job, created bool, err error) {
	if n.IdempotencyKey == "" {
		return nil, false, errors.New("queue: idempotency key is required")
	}
	if n.MaxAttempts == 0 {
		n.MaxAttempts = 5
	}
	payload, err := json.Marshal(n.Payload)
	if err != nil {
		return nil, false, err
	}
	j, err := scan(q.QueryRowContext(ctx, `INSERT INTO jobs (kind, policy_id, payload, idempotency_key, max_attempts)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (idempotency_key) DO NOTHING RETURNING `+cols,
		n.Kind, n.PolicyID, payload, n.IdempotencyKey, n.MaxAttempts))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	j, err = scan(q.QueryRowContext(ctx, `SELECT `+cols+` FROM jobs WHERE idempotency_key = $1`, n.IdempotencyKey))
	return j, false, err
}

// Claim takes the next runnable job for owner and leases it for lease. It returns nil, nil when
// there is nothing to do. A job is runnable when it is queued and due, or when it is running
// but its lease has expired (its worker crashed or hung).
func Claim(ctx context.Context, db *sql.DB, owner string, lease time.Duration) (*Job, error) {
	// A job whose worker died on its final attempt has no attempts left: mark it failed
	// instead of handing it out forever.
	if _, err := db.ExecContext(ctx, `UPDATE jobs SET state = 'failed', updated_at = now(),
		last_error = coalesce(last_error || '; ', '') || 'lease expired on the final attempt', lease_owner = NULL, lease_expires_at = NULL
		WHERE state = 'running' AND lease_expires_at < now() AND attempts >= max_attempts`); err != nil {
		return nil, err
	}
	j, err := scan(db.QueryRowContext(ctx, `
		UPDATE jobs SET state = 'running', attempts = attempts + 1, lease_owner = $1,
		       lease_expires_at = now() + make_interval(secs => $2), updated_at = now()
		WHERE id = (
		    SELECT id FROM jobs
		    WHERE (state = 'queued' AND run_after <= now())
		       OR (state = 'running' AND lease_expires_at < now())
		    ORDER BY run_after, id
		    LIMIT 1
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING `+cols, owner, lease.Seconds()))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return j, err
}

// owned runs an UPDATE that only applies while owner still holds the job's lease.
func owned(ctx context.Context, db *sql.DB, query string, args ...any) error {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Heartbeat extends the lease. Workers call it regularly while a job runs.
func Heartbeat(ctx context.Context, db *sql.DB, id int64, owner string, lease time.Duration) error {
	return owned(ctx, db, `UPDATE jobs SET lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, id, owner, lease.Seconds())
}

// Complete marks the job succeeded and stores its result.
func Complete(ctx context.Context, db *sql.DB, id int64, owner string, result any) error {
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return owned(ctx, db, `UPDATE jobs SET state = 'succeeded', result = $3, last_error = NULL,
		lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, id, owner, b)
}

// Fail records an error. The job goes back to the queue after an exponential backoff
// (2s, 4s, 8s ... capped at 5 minutes), or to "failed" when it has used all its attempts.
// Backing off stops a broken dependency (say, S3 down) from being hammered by instant retries.
func Fail(ctx context.Context, db *sql.DB, id int64, owner string, cause error) error {
	return owned(ctx, db, `UPDATE jobs SET
		state = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
		run_after = now() + make_interval(secs => least(power(2, attempts), 300)::double precision),
		last_error = $3, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, id, owner, truncate(cause.Error()))
}

// Release hands an unfinished job back without counting the attempt, used on graceful shutdown.
func Release(ctx context.Context, db *sql.DB, id int64, owner string) error {
	return owned(ctx, db, `UPDATE jobs SET state = 'queued', attempts = greatest(attempts - 1, 0),
		lease_owner = NULL, lease_expires_at = NULL, run_after = now(), updated_at = now()
		WHERE id = $1 AND lease_owner = $2 AND state = 'running'`, id, owner)
}

// Get returns one job.
func Get(ctx context.Context, q catalog.Querier, id int64) (*Job, error) {
	j, err := scan(q.QueryRowContext(ctx, `SELECT `+cols+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, catalog.ErrNotFound
	}
	return j, err
}

// List returns the most recent jobs, newest first.
func List(ctx context.Context, q catalog.Querier, limit int) ([]*Job, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+cols+` FROM jobs ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Job{}
	for rows.Next() {
		j, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// CountByState feeds the jobs gauge on /metrics.
func CountByState(ctx context.Context, q catalog.Querier) (map[string]int, error) {
	rows, err := q.QueryContext(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{Queued: 0, Running: 0, Succeeded: 0, Failed: 0}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}

func truncate(s string) string {
	if len(s) > 2000 {
		return s[:2000] + "..."
	}
	return s
}

// Describe is a short label for logs.
func (j *Job) Describe() string {
	return fmt.Sprintf("%s job %d (attempt %d/%d)", j.Kind, j.ID, j.Attempts, j.MaxAttempts)
}
