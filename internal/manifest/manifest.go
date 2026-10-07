// Package manifest describes a snapshot: which chunks, in which order, rebuild each file.
//
// A snapshot is "complete" only once its manifest exists in the vault. The backup engine uploads
// every chunk first and writes the manifest last, so a crash part-way through leaves orphan
// chunks (cleaned up later by garbage collection) but never a manifest pointing at missing data.
package manifest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Source types.
const (
	SourceDir      = "dir"
	SourcePostgres = "postgres"
)

// ChunkRef points at one stored chunk.
type ChunkRef struct {
	ID      string `json:"id"`          // HMAC-SHA256 of the plaintext (see crypto.ChunkID)
	Version string `json:"v,omitempty"` // S3 version ID: restore reads exactly this locked version
	Size    int    `json:"n"`           // plaintext size
}

// File is one restored file (or, for PostgreSQL, the single dump stream).
type File struct {
	Path   string     `json:"path"`
	Mode   uint32     `json:"mode,omitempty"`
	Size   int64      `json:"size"`
	SHA256 string     `json:"sha256"` // of the whole original content, checked after restore
	Chunks []ChunkRef `json:"chunks"`
}

// Stats are measured during the backup.
type Stats struct {
	LogicalBytes  int64 `json:"logical_bytes"`  // size of the source data
	UploadedBytes int64 `json:"uploaded_bytes"` // compressed+encrypted bytes actually sent
	NewChunks     int   `json:"new_chunks"`
	ReusedChunks  int   `json:"reused_chunks"` // already in the vault, not uploaded again
	DurationMS    int64 `json:"duration_ms"`
}

// Snapshot is the manifest stored at snapshots/<ID>.json (encrypted).
type Snapshot struct {
	ID         string    `json:"id"`
	SourceType string    `json:"source_type"`
	Source     string    `json:"source"` // directory path, or database name (never a password)
	CreatedAt  time.Time `json:"created_at"`
	Files      []File    `json:"files"`
	Stats      Stats     `json:"stats"`
}

// Key is where a snapshot's manifest lives in the vault.
func Key(id string) string { return "snapshots/" + id + ".json" }

// IDFromKey reverses Key.
func IDFromKey(key string) (string, bool) {
	if !strings.HasPrefix(key, "snapshots/") || !strings.HasSuffix(key, ".json") {
		return "", false
	}
	return strings.TrimSuffix(strings.TrimPrefix(key, "snapshots/"), ".json"), true
}

// ChunkKey spreads chunks over 256 prefixes by their first two hex characters, which keeps
// listings manageable and spreads load across S3 partitions.
func ChunkKey(id string) string { return "chunks/" + id[:2] + "/" + id }

// NewID returns a sortable, unique snapshot ID such as 20261007T120000Z-3fa1c2.
func NewID(t time.Time) string {
	b := make([]byte, 3)
	rand.Read(b)
	return t.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b)
}

// Marshal / Unmarshal with validation, so a damaged manifest is reported, not half-restored.
func Marshal(s *Snapshot) ([]byte, error) { return json.Marshal(s) }

func Unmarshal(b []byte) (*Snapshot, error) {
	var s Snapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if s.ID == "" || (s.SourceType != SourceDir && s.SourceType != SourcePostgres) {
		return nil, errors.New("manifest: missing id or unknown source type")
	}
	for _, f := range s.Files {
		var total int64
		for _, c := range f.Chunks {
			if len(c.ID) != 64 {
				return nil, fmt.Errorf("manifest: bad chunk id in %s", f.Path)
			}
			total += int64(c.Size)
		}
		if total != f.Size {
			return nil, fmt.Errorf("manifest: chunk sizes of %s add up to %d, not %d", f.Path, total, f.Size)
		}
	}
	return &s, nil
}
