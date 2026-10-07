package manifest

import (
	"strings"
	"testing"
	"time"
)

func sample() *Snapshot {
	id := strings.Repeat("a", 64)
	return &Snapshot{
		ID: "s1", SourceType: SourceDir, Source: "/data", CreatedAt: time.Unix(0, 0).UTC(),
		Files: []File{{Path: "a.txt", Size: 10, SHA256: "x", Chunks: []ChunkRef{{ID: id, Version: "v1", Size: 6}, {ID: id, Size: 4}}}},
	}
}

func TestRoundTrip(t *testing.T) {
	b, err := Marshal(sample())
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Files[0].Chunks[0].Version != "v1" || got.Files[0].Size != 10 {
		t.Fatalf("round trip lost data: %+v", got)
	}
}

func TestUnmarshalRejectsInconsistentManifest(t *testing.T) {
	s := sample()
	s.Files[0].Size = 11
	b, _ := Marshal(s)
	if _, err := Unmarshal(b); err == nil {
		t.Fatal("size mismatch should be rejected")
	}
	if _, err := Unmarshal([]byte(`{"id":"x","source_type":"ftp"}`)); err == nil {
		t.Fatal("unknown source type should be rejected")
	}
	if _, err := Unmarshal([]byte(`not json`)); err == nil {
		t.Fatal("invalid JSON should be rejected")
	}
}

func TestKeys(t *testing.T) {
	if id, ok := IDFromKey(Key("abc")); !ok || id != "abc" {
		t.Fatal("Key/IDFromKey mismatch")
	}
	if _, ok := IDFromKey("chunks/ab/abc"); ok {
		t.Fatal("chunk key parsed as snapshot")
	}
	if ChunkKey("abcdef") != "chunks/ab/abcdef" {
		t.Fatal(ChunkKey("abcdef"))
	}
	a, b := NewID(time.Now()), NewID(time.Now())
	if a == b || len(a) != len("20261007T120000Z-3fa1c2") {
		t.Fatalf("bad IDs %q %q", a, b)
	}
}
