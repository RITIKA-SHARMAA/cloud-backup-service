package engine

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/manifest"
)

// dumpPath is the name of the single "file" a PostgreSQL snapshot contains.
const dumpPath = "database.dump"

// BackupPostgres streams pg_dump output straight into the chunker; the dump is never written
// to local disk.
//
// Two flags matter:
//   - --format=custom lets pg_restore restore it (and restore selectively).
//   - --compress=0 turns off pg_dump's own compression. Compressed output changes almost
//     everywhere when a few rows change, which would defeat deduplication. Chunks are compressed
//     with zstd after chunking instead, so storage stays small and unchanged data dedups.
func (e *Engine) BackupPostgres(ctx context.Context, id, dsn string) (*manifest.Snapshot, error) {
	start := time.Now()
	cmd := exec.CommandContext(ctx, "pg_dump", "--format=custom", "--compress=0", "--no-owner", "--dbname="+dsn)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start pg_dump: %w", err)
	}
	s := e.newSession()
	f, streamErr := s.backupStream(ctx, dumpPath, stdout)
	if streamErr != nil {
		cmd.Process.Kill()
	}
	// Wait must come after reading: it closes the pipe. pg_dump's exit status tells us whether
	// the dump was complete; a dump cut short must never become a snapshot.
	waitErr := cmd.Wait()
	if streamErr != nil {
		return nil, streamErr
	}
	if waitErr != nil {
		return nil, fmt.Errorf("pg_dump failed: %v: %s", waitErr, strings.TrimSpace(stderr.String()))
	}
	snap := &manifest.Snapshot{ID: id, SourceType: manifest.SourcePostgres, Source: DatabaseName(dsn),
		CreatedAt: start.UTC(), Files: []manifest.File{f}}
	return e.commit(ctx, snap, s, start)
}

// RestorePostgres pipes the verified dump into pg_restore against the target database, which
// should exist and be empty.
func (e *Engine) RestorePostgres(ctx context.Context, snap *manifest.Snapshot, dsn string) error {
	if snap.SourceType != manifest.SourcePostgres || len(snap.Files) != 1 {
		return fmt.Errorf("snapshot %s is not a PostgreSQL backup", snap.ID)
	}
	cmd := exec.CommandContext(ctx, "pg_restore", "--no-owner", "--exit-on-error", "--dbname="+dsn)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start pg_restore: %w", err)
	}
	writeErr := e.writeFile(ctx, snap.Files[0], stdin)
	stdin.Close()
	if writeErr != nil {
		cmd.Process.Kill()
		cmd.Wait()
		return writeErr
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("pg_restore failed: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// DatabaseName extracts the database name from a connection string, so a password in the DSN
// is never written into a manifest or a log.
func DatabaseName(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && u.Scheme != "" {
		return strings.TrimPrefix(u.Path, "/")
	}
	for _, kv := range strings.Fields(dsn) { // key=value form
		if v, ok := strings.CutPrefix(kv, "dbname="); ok {
			return v
		}
	}
	return "unknown"
}
