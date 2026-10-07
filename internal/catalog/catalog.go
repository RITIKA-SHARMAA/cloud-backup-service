// Package catalog is the metadata database (PostgreSQL): backup policies, the job queue and a
// record of completed snapshots. The backup data itself lives only in the vault.
package catalog

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
)

//go:embed schema.sql
var schema string

// Querier is satisfied by both *sql.DB and *sql.Tx, so the same functions work inside or
// outside a transaction.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Open connects and applies the schema. The schema is idempotent (CREATE ... IF NOT EXISTS) and
// runs under an advisory lock, so several pods starting at once do not race each other.
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	if err := Migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// migrateLock is an arbitrary constant that identifies "schema migration" among advisory locks.
const migrateLock = 72_201

func Migrate(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrateLock); err != nil {
		return err
	}
	defer conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", migrateLock)
	_, err = conn.ExecContext(ctx, schema)
	return err
}

// Policy says what to back up and when.
type Policy struct {
	ID            int64     `json:"id"`
	Name          string    `json:"name"`
	SourceType    string    `json:"source_type"` // "dir" or "postgres"
	Source        string    `json:"source"`      // path, or DSN; ${VARS} are expanded by the worker
	Schedule      string    `json:"schedule"`    // standard 5-field cron in UTC, e.g. "0 2 * * *"
	RetentionDays int       `json:"retention_days"`
	NextRunAt     time.Time `json:"next_run_at"`
	CreatedAt     time.Time `json:"created_at"`
}

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("not found")

// Validate checks a policy before it is stored.
func (p *Policy) Validate() error {
	switch {
	case strings.TrimSpace(p.Name) == "":
		return errors.New("name is required")
	case p.SourceType != "dir" && p.SourceType != "postgres":
		return errors.New(`source_type must be "dir" or "postgres"`)
	case strings.TrimSpace(p.Source) == "":
		return errors.New("source is required")
	case p.RetentionDays < 1:
		return errors.New("retention_days must be at least 1")
	}
	if _, err := cron.ParseStandard(p.Schedule); err != nil {
		return fmt.Errorf("schedule: %w", err)
	}
	return nil
}

// NextRun returns the first time after t the cron schedule fires. Schedules are always
// evaluated in UTC, so "0 2 * * *" means 02:00 UTC whatever time zone a pod happens to run in.
func NextRun(schedule string, t time.Time) (time.Time, error) {
	s, err := cron.ParseStandard(schedule)
	if err != nil {
		return time.Time{}, err
	}
	return s.Next(t.UTC()), nil
}

// CreatePolicy validates and inserts p, filling ID, NextRunAt and CreatedAt.
func CreatePolicy(ctx context.Context, q Querier, p *Policy) error {
	if p.RetentionDays == 0 {
		p.RetentionDays = 30
	}
	if err := p.Validate(); err != nil {
		return err
	}
	next, _ := NextRun(p.Schedule, time.Now())
	err := q.QueryRowContext(ctx, `INSERT INTO policies (name, source_type, source, schedule, retention_days, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, next_run_at, created_at`,
		p.Name, p.SourceType, p.Source, p.Schedule, p.RetentionDays, next).Scan(&p.ID, &p.NextRunAt, &p.CreatedAt)
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		return fmt.Errorf("a policy named %q already exists", p.Name)
	}
	return err
}

const policyCols = `id, name, source_type, source, schedule, retention_days, next_run_at, created_at`

func scanPolicy(row interface{ Scan(...any) error }) (*Policy, error) {
	var p Policy
	err := row.Scan(&p.ID, &p.Name, &p.SourceType, &p.Source, &p.Schedule, &p.RetentionDays, &p.NextRunAt, &p.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &p, err
}

func GetPolicy(ctx context.Context, q Querier, id int64) (*Policy, error) {
	return scanPolicy(q.QueryRowContext(ctx, `SELECT `+policyCols+` FROM policies WHERE id = $1`, id))
}

func ListPolicies(ctx context.Context, q Querier) ([]*Policy, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+policyCols+` FROM policies ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Policy{}
	for rows.Next() {
		p, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SnapshotRecord is the catalog's copy of a completed snapshot's summary, for fast listing.
type SnapshotRecord struct {
	ID            string    `json:"id"`
	PolicyID      *int64    `json:"policy_id,omitempty"`
	JobID         *int64    `json:"job_id,omitempty"`
	SourceType    string    `json:"source_type"`
	Source        string    `json:"source"`
	CreatedAt     time.Time `json:"created_at"`
	LogicalBytes  int64     `json:"logical_bytes"`
	UploadedBytes int64     `json:"uploaded_bytes"`
	NewChunks     int       `json:"new_chunks"`
	ReusedChunks  int       `json:"reused_chunks"`
	DurationMS    int64     `json:"duration_ms"`
}

// RecordSnapshot inserts a snapshot row; recording the same snapshot twice is a no-op.
func RecordSnapshot(ctx context.Context, q Querier, s *SnapshotRecord) error {
	_, err := q.ExecContext(ctx, `INSERT INTO snapshots (id, policy_id, job_id, source_type, source, created_at,
		logical_bytes, uploaded_bytes, new_chunks, reused_chunks, duration_ms)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (id) DO NOTHING`,
		s.ID, s.PolicyID, s.JobID, s.SourceType, s.Source, s.CreatedAt,
		s.LogicalBytes, s.UploadedBytes, s.NewChunks, s.ReusedChunks, s.DurationMS)
	return err
}

func ListSnapshots(ctx context.Context, q Querier) ([]*SnapshotRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT id, policy_id, job_id, source_type, source, created_at,
		logical_bytes, uploaded_bytes, new_chunks, reused_chunks, duration_ms FROM snapshots ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*SnapshotRecord{}
	for rows.Next() {
		var s SnapshotRecord
		if err := rows.Scan(&s.ID, &s.PolicyID, &s.JobID, &s.SourceType, &s.Source, &s.CreatedAt,
			&s.LogicalBytes, &s.UploadedBytes, &s.NewChunks, &s.ReusedChunks, &s.DurationMS); err != nil {
			return nil, err
		}
		out = append(out, &s)
	}
	return out, rows.Err()
}

// DeleteSnapshotRecords removes catalog rows for snapshots that GC deleted from the vault.
func DeleteSnapshotRecords(ctx context.Context, q Querier, keep map[string]bool) error {
	all, err := ListSnapshots(ctx, q)
	if err != nil {
		return err
	}
	for _, s := range all {
		if !keep[s.ID] {
			if _, err := q.ExecContext(ctx, `DELETE FROM snapshots WHERE id = $1`, s.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
