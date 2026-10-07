package vault

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// contract runs the same checks against any Store implementation.
func contract(t *testing.T, s Store) {
	ctx := context.Background()
	p := fmt.Sprintf("test-%d/", time.Now().UnixNano())

	if _, found, err := s.Head(ctx, p+"a"); err != nil || found {
		t.Fatalf("missing key: found=%v err=%v", found, err)
	}
	if _, err := s.Get(ctx, p+"a", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on missing key: want ErrNotFound, got %v", err)
	}
	v, err := s.Put(ctx, p+"a", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	ver, found, err := s.Head(ctx, p+"a")
	if err != nil || !found || ver != v {
		t.Fatalf("Head after Put: version %q found=%v err=%v, want version %q", ver, found, err, v)
	}
	got, err := s.Get(ctx, p+"a", v)
	if err != nil || string(got) != "hello" {
		t.Fatalf("Get: %q %v", got, err)
	}
	if _, err := s.Put(ctx, p+"b/c", []byte("x")); err != nil {
		t.Fatal(err)
	}
	keys, err := s.List(ctx, p)
	if err != nil || len(keys) != 2 || keys[0] != p+"a" || keys[1] != p+"b/c" {
		t.Fatalf("List: %v %v", keys, err)
	}
	// The object was written moments ago, so its retention has not expired.
	if err := s.DeleteVersion(ctx, p+"a", v); !errors.Is(err, ErrLocked) {
		t.Fatalf("deleting a locked object: want ErrLocked, got %v", err)
	}
	if got, err := s.Get(ctx, p+"a", v); err != nil || string(got) != "hello" {
		t.Fatal("locked object changed after a refused delete")
	}
}

func TestLocalStore(t *testing.T) {
	contract(t, &Local{Root: t.TempDir(), Retention: time.Hour})
}

func TestLocalIsWriteOnce(t *testing.T) {
	s := &Local{Root: t.TempDir()}
	ctx := context.Background()
	if _, err := s.Put(ctx, "k", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "k", []byte("two")); err == nil {
		t.Fatal("second Put to the same key should fail")
	}
	if b, _ := s.Get(ctx, "k", ""); string(b) != "one" {
		t.Fatalf("content changed to %q", b)
	}
}

func TestLocalDeleteAfterRetention(t *testing.T) {
	s := &Local{Root: t.TempDir(), Retention: 0}
	ctx := context.Background()
	s.Put(ctx, "k", []byte("x"))
	if err := s.DeleteVersion(ctx, "k", ""); err != nil {
		t.Fatalf("expired object should be deletable: %v", err)
	}
	if _, found, _ := s.Head(ctx, "k"); found {
		t.Fatal("object still present")
	}
}

func TestOpenRejectsUnknownScheme(t *testing.T) {
	if _, err := Open(context.Background(), "ftp://x", "", 0, ""); err == nil {
		t.Fatal("expected error")
	}
}

// --- S3 integration tests: run when CBS_TEST_S3_ENDPOINT points at LocalStack or another
// S3-compatible server with Object Lock support. CI sets it; locally:
//   CBS_TEST_S3_ENDPOINT=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test go test ./...

// NewTestS3 returns a fresh Object Lock bucket, or skips the test when no endpoint is configured.
func NewTestS3(t *testing.T) *S3 {
	t.Helper()
	ep := os.Getenv("CBS_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("CBS_TEST_S3_ENDPOINT not set")
	}
	ctx := context.Background()
	s, err := NewS3(ctx, fmt.Sprintf("cbs-test-%d", time.Now().UnixNano()), ep, time.Hour, Governance)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestS3Store(t *testing.T) {
	contract(t, NewTestS3(t))
}

// An attacker with write access can upload a new object under an existing key, but the old
// locked version is still there, byte for byte, under its version ID.
func TestS3OverwriteKeepsLockedVersion(t *testing.T) {
	s := NewTestS3(t)
	ctx := context.Background()
	v1, err := s.Put(ctx, "chunks/x", []byte("good data"))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := s.Put(ctx, "chunks/x", []byte("ransomware garbage"))
	if err != nil {
		t.Fatal(err)
	}
	if v1 == "" || v1 == v2 {
		t.Fatalf("expected two distinct versions, got %q and %q", v1, v2)
	}
	if b, _ := s.Get(ctx, "chunks/x", v1); string(b) != "good data" {
		t.Fatalf("original version changed: %q", b)
	}
	if err := s.DeleteVersion(ctx, "chunks/x", v1); !errors.Is(err, ErrLocked) {
		t.Fatalf("deleting the locked original: want ErrLocked, got %v", err)
	}
}

func TestS3RefusesDeleteWithoutVersion(t *testing.T) {
	s := NewTestS3(t)
	if err := s.DeleteVersion(context.Background(), "k", ""); err == nil {
		t.Fatal("expected error")
	}
}
