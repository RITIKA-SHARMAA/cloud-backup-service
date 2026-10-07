package engine

import (
	"context"
	"errors"
	"path"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/manifest"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/vault"
)

// GCReport says what garbage collection did.
type GCReport struct {
	SnapshotsDeleted int `json:"snapshots_deleted"`
	ChunksDeleted    int `json:"chunks_deleted"`
	StillLocked      int `json:"still_locked"` // past our cutoff, but S3 retention still forbids deleting
}

// GC deletes snapshots created before cutoff (always keeping the newest snapshot of each
// source), then deletes chunks no remaining snapshot refers to.
//
// It is mark-and-sweep: mark every chunk referenced by a surviving manifest, sweep the rest.
// Object Lock has the final say: anything still inside its retention period is refused by the
// store and counted as StillLocked, so GC can never destroy data early, even with a bad cutoff.
//
// GC must not run at the same time as a backup: a backup could decide to reuse a chunk that GC
// is about to delete. The worker runs GC only while holding a PostgreSQL advisory lock that
// backups also take (see job.Worker); the CLI documents the same rule.
func (e *Engine) GC(ctx context.Context, cutoff time.Time) (GCReport, error) {
	var r GCReport
	snaps, err := e.List(ctx)
	if err != nil {
		return r, err
	}
	newest := map[string]string{} // source -> newest snapshot ID (List is oldest first)
	for _, s := range snaps {
		newest[s.SourceType+":"+s.Source] = s.ID
	}

	live := map[string]bool{}
	for _, s := range snaps {
		expired := s.CreatedAt.Before(cutoff) && newest[s.SourceType+":"+s.Source] != s.ID
		if expired {
			deleted, err := e.deleteLatest(ctx, manifest.Key(s.ID))
			if err != nil {
				return r, err
			}
			if deleted {
				r.SnapshotsDeleted++
				continue
			}
			r.StillLocked++
		}
		for _, f := range s.Files {
			for _, c := range f.Chunks {
				live[c.ID] = true
			}
		}
	}

	keys, err := e.Store.List(ctx, "chunks/")
	if err != nil {
		return r, err
	}
	for _, k := range keys {
		if live[path.Base(k)] {
			continue
		}
		deleted, err := e.deleteLatest(ctx, k)
		if err != nil {
			return r, err
		}
		if deleted {
			r.ChunksDeleted++
		} else {
			r.StillLocked++
		}
	}
	return r, nil
}

// deleteLatest deletes the current version of key. It returns false if retention forbids it.
func (e *Engine) deleteLatest(ctx context.Context, key string) (bool, error) {
	ver, found, err := e.Store.Head(ctx, key)
	if err != nil || !found {
		return false, err
	}
	err = e.Store.DeleteVersion(ctx, key, ver)
	if errors.Is(err, vault.ErrLocked) {
		return false, nil
	}
	return err == nil, err
}
