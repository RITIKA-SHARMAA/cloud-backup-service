// Package vault stores sealed chunks and snapshot manifests.
//
// Two backends implement the same Store interface: Local (a directory, for tests and laptops)
// and S3 (with Object Lock). The rest of the code never knows which one it is talking to.
package vault

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ErrNotFound is returned by Get for a missing key or version.
var ErrNotFound = errors.New("vault: object not found")

// ErrLocked is returned when a delete is refused because the object is still under retention.
var ErrLocked = errors.New("vault: object is locked by its retention period")

// Store is everything the backup engine needs from object storage.
//
// Versions matter for ransomware protection. On S3 with Object Lock, an attacker who steals
// credentials can still upload a new object under an existing key, but cannot delete or alter
// the old locked version. Manifests therefore record the exact version of every chunk, and a
// restore reads that version, so an overwrite cannot change what gets restored.
type Store interface {
	// Put stores data under key and returns the version ID ("" when the backend has no versions).
	Put(ctx context.Context, key string, data []byte) (version string, err error)
	// Get reads a specific version, or the latest one when version is "".
	Get(ctx context.Context, key, version string) ([]byte, error)
	// Head reports whether key exists and its latest version.
	Head(ctx context.Context, key string) (version string, found bool, err error)
	// List returns all keys under prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	// DeleteVersion permanently deletes one version. It fails with ErrLocked while retention applies.
	DeleteVersion(ctx context.Context, key, version string) error
	// String describes the store for logs, e.g. "s3://bucket".
	String() string
}

// Local stores objects as files under Root. Objects are write-once: a second Put to the same
// key fails, and deletes are refused until the file is older than Retention. It imitates the
// S3 behaviour closely enough for tests, but a local disk offers no real tamper protection.
type Local struct {
	Root      string
	Retention time.Duration
}

func (l *Local) path(key string) string { return filepath.Join(l.Root, filepath.FromSlash(key)) }

func (l *Local) String() string { return "file://" + l.Root }

func (l *Local) Put(_ context.Context, key string, data []byte) (string, error) {
	p := l.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	// Write to a temp file and rename, so a crash never leaves a half-written object behind.
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	// os.Link fails if the target exists, which gives write-once semantics without a race.
	if err := os.Link(tmp.Name(), p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", fmt.Errorf("vault: %s already exists and is immutable", key)
		}
		return "", err
	}
	return "", nil
}

func (l *Local) Get(_ context.Context, key, _ string) ([]byte, error) {
	b, err := os.ReadFile(l.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return b, err
}

func (l *Local) Head(_ context.Context, key string) (string, bool, error) {
	_, err := os.Stat(l.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	return "", err == nil, err
}

func (l *Local) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	err := filepath.WalkDir(l.Root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		rel, _ := filepath.Rel(l.Root, p)
		if k := filepath.ToSlash(rel); strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
		return nil
	})
	sort.Strings(keys)
	return keys, err
}

func (l *Local) DeleteVersion(_ context.Context, key, _ string) error {
	st, err := os.Stat(l.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if time.Since(st.ModTime()) < l.Retention {
		return ErrLocked
	}
	return os.Remove(l.path(key))
}

// Open builds a Store from a URL: file:///path or s3://bucket. For S3, endpoint may point at
// LocalStack (e.g. http://localhost:4566); empty means real AWS.
func Open(ctx context.Context, url, endpoint string, retention time.Duration, mode LockMode) (Store, error) {
	switch {
	case strings.HasPrefix(url, "file://"):
		return &Local{Root: strings.TrimPrefix(url, "file://"), Retention: retention}, nil
	case strings.HasPrefix(url, "s3://"):
		return NewS3(ctx, strings.TrimPrefix(url, "s3://"), endpoint, retention, mode)
	default:
		return nil, fmt.Errorf("vault: unsupported URL %q (use file:///path or s3://bucket)", url)
	}
}
