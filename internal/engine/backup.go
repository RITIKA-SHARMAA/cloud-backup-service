// Package engine turns source data into snapshots and back: backup, restore, verify and
// garbage collection. It works on any vault.Store.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/chunk"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/crypto"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/manifest"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/vault"
)

// Engine holds what every operation needs. It is safe to share between goroutines.
type Engine struct {
	Store    vault.Store
	Codec    *crypto.Codec
	Chunking chunk.Config
}

// New returns an Engine with the default ~1 MiB chunking.
func New(store vault.Store, codec *crypto.Codec) *Engine {
	return &Engine{Store: store, Codec: codec, Chunking: chunk.Default}
}

// session tracks one backup run: its statistics, and chunks already handled during this run, so
// a chunk that repeats inside the same backup is checked against the vault only once.
// A session is used by one goroutine.
type session struct {
	e     *Engine
	stats manifest.Stats
	seen  map[string]string // chunk ID -> version
}

func (e *Engine) newSession() *session { return &session{e: e, seen: map[string]string{}} }

// storeChunk uploads one chunk unless the vault already has it (deduplication).
func (s *session) storeChunk(ctx context.Context, plain []byte) (manifest.ChunkRef, error) {
	id := s.e.Codec.ChunkID(plain)
	ref := manifest.ChunkRef{ID: id, Size: len(plain)}

	v, ok := s.seen[id]
	if !ok {
		key := manifest.ChunkKey(id)
		ver, found, err := s.e.Store.Head(ctx, key)
		if err != nil {
			return ref, err
		}
		if found {
			v = ver
			s.stats.ReusedChunks++
		} else {
			// The chunk ID is passed as additional authenticated data, binding the ciphertext to its name.
			sealed, err := s.e.Codec.Seal(plain, []byte(id))
			if err != nil {
				return ref, err
			}
			if v, err = s.e.Store.Put(ctx, key, sealed); err != nil {
				return ref, err
			}
			s.stats.NewChunks++
			s.stats.UploadedBytes += int64(len(sealed))
		}
		s.seen[id] = v
	} else {
		s.stats.ReusedChunks++
	}
	ref.Version = v
	return ref, nil
}

// backupStream chunks one stream and stores its chunks. The stream is never held in memory
// as a whole: only one chunk (at most 4 MiB) at a time, which is what lets a large database
// dump flow straight from pg_dump into the vault.
func (s *session) backupStream(ctx context.Context, path string, r io.Reader) (manifest.File, error) {
	f := manifest.File{Path: path}
	whole := sha256.New()
	c := chunk.New(io.TeeReader(r, whole), s.e.Chunking)
	for {
		if err := ctx.Err(); err != nil {
			return f, err
		}
		data, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return f, err
		}
		ref, err := s.storeChunk(ctx, data)
		if err != nil {
			return f, fmt.Errorf("%s: %w", path, err)
		}
		f.Chunks = append(f.Chunks, ref)
		f.Size += int64(len(data))
	}
	f.SHA256 = hex.EncodeToString(whole.Sum(nil))
	s.stats.LogicalBytes += f.Size
	return f, nil
}

// BackupDir backs up every regular file under root. Symlinks and special files are skipped.
func (e *Engine) BackupDir(ctx context.Context, id, root string) (*manifest.Snapshot, error) {
	start := time.Now()
	var paths []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			paths = append(paths, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)

	s := e.newSession()
	snap := &manifest.Snapshot{ID: id, SourceType: manifest.SourceDir, Source: root, CreatedAt: start.UTC()}
	for _, p := range paths {
		rel, _ := filepath.Rel(root, p)
		fh, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		info, _ := fh.Stat()
		f, err := s.backupStream(ctx, filepath.ToSlash(rel), fh)
		fh.Close()
		if err != nil {
			return nil, err
		}
		f.Mode = uint32(info.Mode().Perm())
		snap.Files = append(snap.Files, f)
	}
	return e.commit(ctx, snap, s, start)
}

// commit writes the manifest, the very last step of a backup.
//
// If the manifest already exists, an earlier attempt of the same job crashed after committing
// but before reporting success. The snapshot is then already complete, so that is not an error:
// this is what makes retrying a backup job idempotent.
func (e *Engine) commit(ctx context.Context, snap *manifest.Snapshot, s *session, start time.Time) (*manifest.Snapshot, error) {
	snap.Stats = s.stats
	snap.Stats.DurationMS = time.Since(start).Milliseconds()
	key := manifest.Key(snap.ID)
	if _, found, err := e.Store.Head(ctx, key); err != nil {
		return nil, err
	} else if found {
		return e.Load(ctx, snap.ID)
	}
	plain, err := manifest.Marshal(snap)
	if err != nil {
		return nil, err
	}
	sealed, err := e.Codec.Seal(plain, []byte(key))
	if err != nil {
		return nil, err
	}
	if _, err := e.Store.Put(ctx, key, sealed); err != nil {
		return nil, err
	}
	return snap, nil
}

// Load reads and decrypts a snapshot manifest (the latest version of its key).
func (e *Engine) Load(ctx context.Context, id string) (*manifest.Snapshot, error) {
	key := manifest.Key(id)
	sealed, err := e.Store.Get(ctx, key, "")
	if errors.Is(err, vault.ErrNotFound) {
		return nil, fmt.Errorf("snapshot %s not found", id)
	}
	if err != nil {
		return nil, err
	}
	plain, err := e.Codec.Open(sealed, []byte(key))
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", id, err)
	}
	return manifest.Unmarshal(plain)
}

// List returns every snapshot in the vault, oldest first.
func (e *Engine) List(ctx context.Context) ([]*manifest.Snapshot, error) {
	keys, err := e.Store.List(ctx, "snapshots/")
	if err != nil {
		return nil, err
	}
	var out []*manifest.Snapshot
	for _, k := range keys {
		id, ok := manifest.IDFromKey(k)
		if !ok {
			continue
		}
		s, err := e.Load(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
