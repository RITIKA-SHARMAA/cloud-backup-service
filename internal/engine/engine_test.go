package engine

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/chunk"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/crypto"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/manifest"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/vault"
)

var small = chunk.Config{Min: 2 << 10, Avg: 8 << 10, Max: 32 << 10}

func testEngine(t *testing.T, store vault.Store) *Engine {
	t.Helper()
	codec, err := crypto.NewCodec(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	e := New(store, codec)
	e.Chunking = small
	return e
}

func writeTree(t *testing.T, root string, files map[string][]byte) {
	t.Helper()
	for name, data := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, data, 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

func random(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func sampleTree() map[string][]byte {
	return map[string][]byte{
		"a.bin":          random(1, 300<<10),
		"docs/notes.txt": []byte(strings.Repeat("meeting notes\n", 2000)),
		"docs/empty.txt": {},
		"copy-of-a.bin":  random(1, 300<<10), // identical content: must be stored once
	}
}

func assertSameTree(t *testing.T, want map[string][]byte, root string) {
	t.Helper()
	for name, data := range want {
		got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%s: restored content differs (err %v)", name, err)
		}
	}
}

func TestBackupAndRestoreDirectory(t *testing.T) {
	ctx := context.Background()
	e := testEngine(t, &vault.Local{Root: t.TempDir()})
	src, files := t.TempDir(), sampleTree()
	writeTree(t, src, files)

	snap, err := e.BackupDir(ctx, "s1", src)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Files) != 4 || snap.Stats.LogicalBytes != int64(600<<10+28000) {
		t.Fatalf("unexpected snapshot: %d files, %d bytes", len(snap.Files), snap.Stats.LogicalBytes)
	}
	// The duplicate file's chunks are found in this same backup, so they count as reused.
	if snap.Stats.ReusedChunks == 0 {
		t.Fatal("identical files should share chunks")
	}

	dest := t.TempDir()
	loaded, err := e.Load(ctx, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RestoreDir(ctx, loaded, dest); err != nil {
		t.Fatal(err)
	}
	assertSameTree(t, files, dest)
	if st, _ := os.Stat(filepath.Join(dest, "a.bin")); st.Mode().Perm() != 0o640 {
		t.Fatalf("file mode not restored: %v", st.Mode())
	}
}

func TestIncrementalBackupUploadsOnlyChanges(t *testing.T) {
	ctx := context.Background()
	e := testEngine(t, &vault.Local{Root: t.TempDir()})
	src, files := t.TempDir(), sampleTree()
	writeTree(t, src, files)
	first, err := e.BackupDir(ctx, "s1", src)
	if err != nil {
		t.Fatal(err)
	}

	unchanged, err := e.BackupDir(ctx, "s2", src)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Stats.NewChunks != 0 || unchanged.Stats.UploadedBytes != 0 {
		t.Fatalf("unchanged data uploaded %d chunks", unchanged.Stats.NewChunks)
	}

	// Insert a few bytes in the middle of the big file.
	a := files["a.bin"]
	files["a.bin"] = append(append(append([]byte(nil), a[:150<<10]...), []byte("EDIT")...), a[150<<10:]...)
	writeTree(t, src, files)
	edited, err := e.BackupDir(ctx, "s3", src)
	if err != nil {
		t.Fatal(err)
	}
	total := edited.Stats.NewChunks + edited.Stats.ReusedChunks
	if edited.Stats.NewChunks == 0 || edited.Stats.NewChunks > 3 {
		t.Fatalf("a 4-byte insert created %d new chunks out of %d", edited.Stats.NewChunks, total)
	}
	if edited.Stats.UploadedBytes*10 > first.Stats.UploadedBytes {
		t.Fatalf("incremental upload %d bytes is not small next to the first %d", edited.Stats.UploadedBytes, first.Stats.UploadedBytes)
	}

	// Both versions restore exactly.
	old, _ := e.Load(ctx, "s1")
	dest := t.TempDir()
	if err := e.RestoreDir(ctx, old, dest); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dest, "a.bin")); !bytes.Equal(got, a) {
		t.Fatal("old snapshot did not restore the old content")
	}
	dest2 := t.TempDir()
	if err := e.RestoreDir(ctx, edited, dest2); err != nil {
		t.Fatal(err)
	}
	assertSameTree(t, files, dest2)
}

func TestCorruptedChunkIsDetected(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	e := testEngine(t, &vault.Local{Root: root})
	src := t.TempDir()
	writeTree(t, src, map[string][]byte{"f": random(9, 100<<10)})
	snap, err := e.BackupDir(ctx, "s1", src)
	if err != nil {
		t.Fatal(err)
	}
	// Flip one byte of one stored chunk, as disk corruption or an attacker would.
	p := filepath.Join(root, filepath.FromSlash(manifest.ChunkKey(snap.Files[0].Chunks[1].ID)))
	b, _ := os.ReadFile(p)
	b[len(b)/2] ^= 0xff
	os.Chmod(p, 0o644)
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := e.Verify(ctx, snap); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("verify should report the tampered chunk, got %v", err)
	}
	dest := t.TempDir()
	if err := e.RestoreDir(ctx, snap, dest); err == nil {
		t.Fatal("restore of a corrupted snapshot must fail")
	}
	if _, err := os.Stat(filepath.Join(dest, "f")); !os.IsNotExist(err) {
		t.Fatal("a failed restore must not leave the file under its real name")
	}
}

func TestRestoreRefusesPathTraversal(t *testing.T) {
	e := testEngine(t, &vault.Local{Root: t.TempDir()})
	snap := &manifest.Snapshot{ID: "x", SourceType: manifest.SourceDir, Files: []manifest.File{{Path: "../../evil"}}}
	if err := e.RestoreDir(context.Background(), snap, t.TempDir()); err == nil {
		t.Fatal("expected refusal")
	}
}

func TestCommitIsIdempotent(t *testing.T) {
	ctx := context.Background()
	e := testEngine(t, &vault.Local{Root: t.TempDir()})
	src := t.TempDir()
	writeTree(t, src, map[string][]byte{"f": []byte("hello")})
	if _, err := e.BackupDir(ctx, "same-id", src); err != nil {
		t.Fatal(err)
	}
	// A retried job runs the same backup with the same ID again: that must succeed.
	if _, err := e.BackupDir(ctx, "same-id", src); err != nil {
		t.Fatalf("retry of a committed snapshot failed: %v", err)
	}
	snaps, _ := e.List(ctx)
	if len(snaps) != 1 {
		t.Fatalf("want 1 snapshot, got %d", len(snaps))
	}
}

func TestManifestIsEncrypted(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	e := testEngine(t, &vault.Local{Root: root})
	src := t.TempDir()
	writeTree(t, src, map[string][]byte{"salary-report.txt": []byte("x")})
	e.BackupDir(ctx, "s1", src)
	raw, _ := os.ReadFile(filepath.Join(root, "snapshots", "s1.json"))
	if bytes.Contains(raw, []byte("salary-report")) {
		t.Fatal("file names must not be readable in the vault")
	}
}

func TestGCDeletesOnlyUnreferencedExpiredData(t *testing.T) {
	ctx := context.Background()
	store := &vault.Local{Root: t.TempDir(), Retention: 0}
	e := testEngine(t, store)
	src := t.TempDir()
	writeTree(t, src, map[string][]byte{"f": random(1, 100<<10)})
	old, _ := e.BackupDir(ctx, manifest.NewID(time.Now()), src)
	time.Sleep(10 * time.Millisecond)
	writeTree(t, src, map[string][]byte{"f": random(2, 100<<10)}) // completely new content
	time.Sleep(10 * time.Millisecond)
	newest, _ := e.BackupDir(ctx, manifest.NewID(time.Now()), src)

	r, err := e.GC(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if r.SnapshotsDeleted != 1 || r.ChunksDeleted != len(old.Files[0].Chunks) {
		t.Fatalf("unexpected GC result %+v (old snapshot had %d chunks)", r, len(old.Files[0].Chunks))
	}
	if err := e.Verify(ctx, newest); err != nil {
		t.Fatalf("GC damaged the surviving snapshot: %v", err)
	}

	// With a long retention, nothing can be deleted however aggressive the cutoff.
	store.Retention = time.Hour
	writeTree(t, src, map[string][]byte{"f": random(3, 100<<10)})
	time.Sleep(10 * time.Millisecond)
	e.BackupDir(ctx, manifest.NewID(time.Now()), src)
	r, _ = e.GC(ctx, time.Now().Add(time.Hour))
	if r.SnapshotsDeleted != 0 || r.ChunksDeleted != 0 || r.StillLocked == 0 {
		t.Fatalf("locked data must survive GC: %+v", r)
	}
}

func TestDatabaseNameNeverIncludesPassword(t *testing.T) {
	for dsn, want := range map[string]string{
		"postgres://u:secret@host:5432/shop?sslmode=disable": "shop",
		"host=x user=u password=secret dbname=shop":          "shop",
	} {
		if got := DatabaseName(dsn); got != want {
			t.Fatalf("%s -> %s", dsn, got)
		}
	}
}
