// Package crypto turns a plaintext chunk into the bytes stored in the vault and back:
// compress with zstd, then encrypt with AES-256-GCM.
//
// Order matters: encrypted data looks random and does not compress, so compression must come first.
// GCM is authenticated encryption: Open fails if even one stored bit was changed or the wrong key
// is used, so a tampered or corrupted chunk can never be restored silently.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// formatV1 is the first byte of every sealed chunk, so the format can change later without
// breaking old backups.
const formatV1 = 1

// Codec compresses and encrypts chunks. It is safe for concurrent use.
type Codec struct {
	aead  cipher.AEAD
	idKey []byte
	enc   *zstd.Encoder
	dec   *zstd.Decoder
}

// ParseKey decodes a 64-character hex string into a 32-byte AES-256 key.
func ParseKey(s string) ([]byte, error) {
	k, err := hex.DecodeString(s)
	if err != nil || len(k) != 32 {
		return nil, errors.New("encryption key must be 64 hex characters (32 bytes)")
	}
	return k, nil
}

// NewCodec builds a Codec from a 32-byte key.
func NewCodec(key []byte) (*Codec, error) {
	if len(key) != 32 {
		return nil, errors.New("crypto: key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	// A separate key for chunk IDs, derived from the main key, so the two uses never share a key.
	m := hmac.New(sha256.New, key)
	m.Write([]byte("cloud-backup-service chunk-id v1"))
	return &Codec{aead: aead, idKey: m.Sum(nil), enc: enc, dec: dec}, nil
}

// ChunkID names a chunk by its content: HMAC-SHA256 of the plaintext under a secret key.
//
// Equal content gives an equal ID, which is what makes deduplication work. A plain SHA-256
// would also work, but it would let anyone who can list the bucket test whether a known file
// (say, a leaked document) is in the backup by hashing it. With a keyed HMAC they cannot.
func (c *Codec) ChunkID(plain []byte) string {
	m := hmac.New(sha256.New, c.idKey)
	m.Write(plain)
	return hex.EncodeToString(m.Sum(nil))
}

// Seal returns version byte || 12-byte random nonce || ciphertext+tag.
//
// The nonce is random per chunk. GCM breaks badly if a nonce is ever reused with the same key;
// with 96 random bits that needs around 2^48 chunks before it becomes a real risk.
// aad (additional authenticated data) is the chunk's content hash: it is not encrypted, but it
// is bound to the ciphertext, so a valid sealed chunk cannot be swapped in under another name.
func (c *Codec) Seal(plain, aad []byte) ([]byte, error) {
	compressed := c.enc.EncodeAll(plain, nil)
	out := make([]byte, 1+c.aead.NonceSize(), 1+c.aead.NonceSize()+len(compressed)+c.aead.Overhead())
	out[0] = formatV1
	if _, err := rand.Read(out[1:]); err != nil {
		return nil, err
	}
	return c.aead.Seal(out, out[1:], compressed, aad), nil
}

// Open reverses Seal. It fails on any modification, a wrong key, or a mismatched aad.
func (c *Codec) Open(sealed, aad []byte) ([]byte, error) {
	ns := c.aead.NonceSize()
	if len(sealed) < 1+ns+c.aead.Overhead() || sealed[0] != formatV1 {
		return nil, errors.New("crypto: not a sealed chunk")
	}
	compressed, err := c.aead.Open(nil, sealed[1:1+ns], sealed[1+ns:], aad)
	if err != nil {
		return nil, fmt.Errorf("crypto: authentication failed (tampered data or wrong key): %w", err)
	}
	return c.dec.DecodeAll(compressed, nil)
}
