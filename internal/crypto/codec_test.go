package crypto

import (
	"bytes"
	"strings"
	"testing"
)

func testCodec(t *testing.T, fill byte) *Codec {
	t.Helper()
	c, err := NewCodec(bytes.Repeat([]byte{fill}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRoundTrip(t *testing.T) {
	c := testCodec(t, 1)
	plain := []byte(strings.Repeat("backup data ", 1000))
	sealed, err := c.Seal(plain, []byte("id"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) >= len(plain) {
		t.Fatalf("repetitive data should compress: %d sealed vs %d plain", len(sealed), len(plain))
	}
	got, err := c.Open(sealed, []byte("id"))
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestSameInputGivesDifferentCiphertext(t *testing.T) {
	c := testCodec(t, 1)
	a, _ := c.Seal([]byte("same"), nil)
	b, _ := c.Seal([]byte("same"), nil)
	if bytes.Equal(a, b) {
		t.Fatal("random nonces should make ciphertexts differ")
	}
}

func TestTamperedCiphertextIsRejected(t *testing.T) {
	c := testCodec(t, 1)
	sealed, _ := c.Seal([]byte("important rows"), nil)
	for _, i := range []int{0, 5, len(sealed) - 1} {
		bad := append([]byte(nil), sealed...)
		bad[i] ^= 0x01
		if _, err := c.Open(bad, nil); err == nil {
			t.Fatalf("flipping a bit at offset %d was not detected", i)
		}
	}
}

func TestWrongKeyAndWrongAADAreRejected(t *testing.T) {
	sealed, _ := testCodec(t, 1).Seal([]byte("secret"), []byte("chunk-a"))
	if _, err := testCodec(t, 2).Open(sealed, []byte("chunk-a")); err == nil {
		t.Fatal("wrong key accepted")
	}
	if _, err := testCodec(t, 1).Open(sealed, []byte("chunk-b")); err == nil {
		t.Fatal("chunk stored under a different id accepted")
	}
}

func TestParseKey(t *testing.T) {
	if _, err := ParseKey(strings.Repeat("ab", 32)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "abc", strings.Repeat("ab", 31), strings.Repeat("zz", 32)} {
		if _, err := ParseKey(bad); err == nil {
			t.Fatalf("key %q should be rejected", bad)
		}
	}
}

func TestChunkIDIsKeyedAndStable(t *testing.T) {
	a, b := testCodec(t, 1), testCodec(t, 2)
	if a.ChunkID([]byte("x")) != a.ChunkID([]byte("x")) {
		t.Fatal("same content must give the same ID")
	}
	if a.ChunkID([]byte("x")) == a.ChunkID([]byte("y")) {
		t.Fatal("different content gave the same ID")
	}
	if a.ChunkID([]byte("x")) == b.ChunkID([]byte("x")) {
		t.Fatal("IDs must depend on the key")
	}
	if len(a.ChunkID(nil)) != 64 {
		t.Fatal("ID should be 64 hex characters")
	}
}
