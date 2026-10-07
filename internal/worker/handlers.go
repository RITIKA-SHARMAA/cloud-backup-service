package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/engine"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/manifest"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/metrics"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/queue"
)

// Payloads stored in jobs.payload.
type (
	BackupPayload struct {
		PolicyID int64 `json:"policy_id"`
	}
	RestorePayload struct {
		SnapshotID string `json:"snapshot_id"`
		TargetDir  string `json:"target_dir,omitempty"` // for directory snapshots
		TargetDSN  string `json:"target_dsn,omitempty"` // for PostgreSQL snapshots; ${VARS} expanded
	}
	GCPayload struct {
		KeepDays int `json:"keep_days"`
	}
)

// gcLock is the advisory lock shared by backups and exclusive for GC (see engine.GC): many
// backups may run together, but GC only runs when no backup is in progress.
const gcLock = 72_202

// NewHandler returns the production job handler.
func NewHandler(db *sql.DB, e *engine.Engine, m *metrics.Registry) Handler {
	m.Describe("cbs_jobs_total", "Jobs finished by this process, by kind and result.")
	m.Describe("cbs_uploaded_bytes_total", "Compressed and encrypted bytes uploaded to the vault.")
	m.Describe("cbs_logical_bytes_total", "Bytes of source data backed up.")
	m.Describe("cbs_chunks_total", "Chunks processed, by whether they were new or deduplicated.")
	h := &handlers{db: db, e: e, m: m}
	return func(ctx context.Context, job *queue.Job, log *slog.Logger) (any, error) {
		switch job.Kind {
		case queue.KindBackup:
			return h.backup(ctx, job, log)
		case queue.KindRestore:
			return h.restore(ctx, job, log)
		case queue.KindGC:
			return h.gc(ctx, job, log)
		}
		return nil, fmt.Errorf("unknown job kind %q", job.Kind)
	}
}

type handlers struct {
	db *sql.DB
	e  *engine.Engine
	m  *metrics.Registry
}

// SnapshotIDForJob is deterministic: a retried job writes the same snapshot ID, so if an earlier
// attempt already committed the manifest the retry finds it instead of creating a duplicate.
func SnapshotIDForJob(job *queue.Job) string {
	return job.CreatedAt.UTC().Format("20060102T150405Z") + fmt.Sprintf("-j%d", job.ID)
}

func (h *handlers) backup(ctx context.Context, job *queue.Job, log *slog.Logger) (any, error) {
	var p BackupPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return nil, err
	}
	pol, err := catalog.GetPolicy(ctx, h.db, p.PolicyID)
	if err != nil {
		return nil, fmt.Errorf("policy %d: %w", p.PolicyID, err)
	}

	unlock, err := h.lock(ctx, "pg_advisory_lock_shared", "pg_advisory_unlock_shared")
	if err != nil {
		return nil, err
	}
	defer unlock()

	id := SnapshotIDForJob(job)
	log = log.With("snapshot_id", id, "policy", pol.Name)
	var snap *manifest.Snapshot
	switch pol.SourceType {
	case manifest.SourceDir:
		snap, err = h.e.BackupDir(ctx, id, pol.Source)
	case manifest.SourcePostgres:
		// The DSN may reference environment variables (e.g. ${SHOP_DB_PASSWORD}) so that
		// passwords live in Kubernetes Secrets, not in the catalog table.
		snap, err = h.e.BackupPostgres(ctx, id, os.ExpandEnv(pol.Source))
	default:
		err = fmt.Errorf("unknown source type %q", pol.SourceType)
	}
	if err != nil {
		return nil, err
	}
	st := snap.Stats
	h.m.Add("cbs_uploaded_bytes_total", float64(st.UploadedBytes))
	h.m.Add("cbs_logical_bytes_total", float64(st.LogicalBytes))
	h.m.Add("cbs_chunks_total", float64(st.NewChunks), "type", "new")
	h.m.Add("cbs_chunks_total", float64(st.ReusedChunks), "type", "reused")

	jobID := job.ID
	err = catalog.RecordSnapshot(ctx, h.db, &catalog.SnapshotRecord{
		ID: snap.ID, PolicyID: &pol.ID, JobID: &jobID, SourceType: snap.SourceType, Source: snap.Source,
		CreatedAt: snap.CreatedAt, LogicalBytes: st.LogicalBytes, UploadedBytes: st.UploadedBytes,
		NewChunks: st.NewChunks, ReusedChunks: st.ReusedChunks, DurationMS: st.DurationMS,
	})
	if err != nil {
		return nil, err
	}
	log.Info("snapshot committed", "logical_bytes", st.LogicalBytes, "uploaded_bytes", st.UploadedBytes,
		"new_chunks", st.NewChunks, "reused_chunks", st.ReusedChunks)
	return map[string]any{"snapshot_id": snap.ID, "stats": st}, nil
}

func (h *handlers) restore(ctx context.Context, job *queue.Job, log *slog.Logger) (any, error) {
	var p RestorePayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return nil, err
	}
	snap, err := h.e.Load(ctx, p.SnapshotID)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	switch {
	case snap.SourceType == manifest.SourceDir && p.TargetDir != "":
		err = h.e.RestoreDir(ctx, snap, p.TargetDir)
	case snap.SourceType == manifest.SourcePostgres && p.TargetDSN != "":
		err = h.e.RestorePostgres(ctx, snap, os.ExpandEnv(p.TargetDSN))
	default:
		err = errors.New("restore needs target_dir for directory snapshots or target_dsn for PostgreSQL snapshots")
	}
	if err != nil {
		return nil, err
	}
	ms := time.Since(start).Milliseconds()
	log.Info("restore finished", "snapshot_id", snap.ID, "duration_ms", ms)
	return map[string]any{"snapshot_id": snap.ID, "duration_ms": ms}, nil
}

func (h *handlers) gc(ctx context.Context, job *queue.Job, log *slog.Logger) (any, error) {
	var p GCPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return nil, err
	}
	if p.KeepDays < 1 {
		return nil, errors.New("keep_days must be at least 1")
	}
	// try-lock: if backups are running, fail now and let the queue retry later with backoff.
	unlock, err := h.lock(ctx, "pg_try_advisory_lock", "pg_advisory_unlock")
	if err != nil {
		return nil, err
	}
	defer unlock()

	report, err := h.e.GC(ctx, time.Now().AddDate(0, 0, -p.KeepDays))
	if err != nil {
		return nil, err
	}
	remaining, err := h.e.List(ctx)
	if err != nil {
		return nil, err
	}
	keep := map[string]bool{}
	for _, s := range remaining {
		keep[s.ID] = true
	}
	if err := catalog.DeleteSnapshotRecords(ctx, h.db, keep); err != nil {
		return nil, err
	}
	log.Info("gc finished", "snapshots_deleted", report.SnapshotsDeleted, "chunks_deleted", report.ChunksDeleted, "still_locked", report.StillLocked)
	return report, nil
}

// lock takes a session-level advisory lock on a dedicated connection (advisory locks belong to
// the connection that took them) and returns a function that releases it.
func (h *handlers) lock(ctx context.Context, lockFn, unlockFn string) (func(), error) {
	conn, err := h.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if strings.Contains(lockFn, "try") {
		var ok bool
		if err := conn.QueryRowContext(ctx, "SELECT "+lockFn+"($1)", gcLock).Scan(&ok); err != nil {
			conn.Close()
			return nil, err
		}
		if !ok {
			conn.Close()
			return nil, errors.New("backups are running; garbage collection will retry later")
		}
	} else if _, err := conn.ExecContext(ctx, "SELECT "+lockFn+"($1)", gcLock); err != nil {
		conn.Close()
		return nil, err
	}
	return func() {
		conn.ExecContext(context.Background(), "SELECT "+unlockFn+"($1)", gcLock)
		conn.Close()
	}, nil
}
