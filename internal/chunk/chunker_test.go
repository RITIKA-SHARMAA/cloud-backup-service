package chunk

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand"
	"testing"
)

// small keeps tests fast while exercising the same code paths as Default.
var small = Config{Min: 1 << 10, Avg: 4 << 10, Max: 16 << 10}

func randomData(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func split(t *testing.T, data []byte, cfg Config) [][]byte {
	t.Helper()
	c := New(bytes.NewReader(data), cfg)
	var out [][]byte
	for {
		ch, err := c.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, append([]byte(nil), ch...))
	}
}

func hashes(chunks [][]byte) map[[32]byte]bool {
	m := map[[32]byte]bool{}
	for _, c := range chunks {
		m[sha256.Sum256(c)] = true
	}
	return m
}

func TestChunksReassembleToInput(t *testing.T) {
	data := randomData(1, 1<<20)
	chunks := split(t, data, small)
	if got := bytes.Join(chunks, nil); !bytes.Equal(got, data) {
		t.Fatal("reassembled chunks differ from input")
	}
}

func TestChunkSizesStayWithinLimits(t *testing.T) {
	chunks := split(t, randomData(2, 2<<20), small)
	for i, c := range chunks {
		last := i == len(chunks)-1
		if len(c) > small.Max || (!last && len(c) < small.Min) {
			t.Fatalf("chunk %d has size %d, limits %d..%d", i, len(c), small.Min, small.Max)
		}
	}
	avg := (2 << 20) / len(chunks)
	if avg < small.Avg/2 || avg > small.Avg*2 {
		t.Fatalf("average chunk size %d is far from target %d", avg, small.Avg)
	}
}

func TestBoundariesAreDeterministic(t *testing.T) {
	data := randomData(3, 512<<10)
	a, b := split(t, data, small), split(t, data, small)
	if len(a) != len(b) {
		t.Fatal("same input gave different chunk counts")
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatalf("chunk %d differs between runs", i)
		}
	}
}

// The property that makes incremental backups cheap: inserting bytes near the start changes
// only the first chunk or two, because boundaries are chosen by content, not by offset.
func TestInsertNearStartKeepsMostChunks(t *testing.T) {
	data := randomData(4, 1<<20)
	edited := append(append(append([]byte(nil), data[:100]...), []byte("a few inserted bytes")...), data[100:]...)

	before, after := split(t, data, small), hashes(split(t, edited, small))
	shared := 0
	for h := range hashes(before) {
		if after[h] {
			shared++
		}
	}
	if shared < len(before)-3 {
		t.Fatalf("only %d of %d chunks survived a 20-byte insert", shared, len(before))
	}

	// For contrast, fixed-size blocks lose nearly everything after the same edit.
	fixed := func(b []byte) map[[32]byte]bool {
		m := map[[32]byte]bool{}
		for i := 0; i < len(b); i += small.Avg {
			m[sha256.Sum256(b[i:min(i+small.Avg, len(b))])] = true
		}
		return m
	}
	fb, fa, fixedShared := fixed(data), fixed(edited), 0
	for h := range fb {
		if fa[h] {
			fixedShared++
		}
	}
	if fixedShared > 1 {
		t.Fatalf("expected fixed-size blocks to lose almost all matches, kept %d", fixedShared)
	}
}

func TestEmptyInput(t *testing.T) {
	if _, err := New(bytes.NewReader(nil), small).Next(); err != io.EOF {
		t.Fatalf("want io.EOF, got %v", err)
	}
}

func TestInvalidConfigRejected(t *testing.T) {
	for _, c := range []Config{{0, 4, 8}, {4, 4, 8}, {1, 6, 8}, {1, 8, 8}} {
		if c.Validate() == nil {
			t.Fatalf("config %+v should be invalid", c)
		}
	}
	if Default.Validate() != nil {
		t.Fatal("default config must be valid")
	}
}
