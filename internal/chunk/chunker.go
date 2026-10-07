// Package chunk splits a byte stream into content-defined chunks.
//
// Why content-defined and not fixed-size? With fixed 1 MiB blocks, inserting one byte at the
// start of a file shifts every later block boundary, so every block hashes differently and an
// "incremental" backup re-uploads the whole file. Here a boundary is placed where a rolling hash
// of the last few bytes matches a pattern, so boundaries follow the content: after an insert,
// the chunker re-synchronises at the next boundary and later chunks are byte-for-byte the same.
//
// The algorithm is a simplified FastCDC: a "gear" rolling hash (one shift and one add per byte)
// with a stricter mask before the target size and a looser one after it, which keeps chunk sizes
// close to the target.
package chunk

import (
	"bufio"
	"errors"
	"io"
	"math/bits"
)

// Config sets the chunk size limits in bytes.
type Config struct {
	Min, Avg, Max int
}

// Default is the production setting: ~1 MiB chunks, never below 256 KiB or above 4 MiB.
var Default = Config{Min: 256 << 10, Avg: 1 << 20, Max: 4 << 20}

// Validate checks the limits make sense; Avg must be a power of two because the masks are built from it.
func (c Config) Validate() error {
	if c.Min <= 0 || c.Min >= c.Avg || c.Avg >= c.Max {
		return errors.New("chunk: need 0 < Min < Avg < Max")
	}
	if c.Avg&(c.Avg-1) != 0 {
		return errors.New("chunk: Avg must be a power of two")
	}
	return nil
}

// gear holds 256 fixed pseudo-random 64-bit values, one per byte value. They are generated
// from a fixed seed so chunk boundaries are identical across runs and machines; changing them
// would change every boundary and break deduplication against existing backups.
var gear = func() [256]uint64 {
	var t [256]uint64
	x := uint64(0x9E3779B97F4A7C15)
	for i := range t {
		// splitmix64
		x += 0x9E3779B97F4A7C15
		z := x
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		t[i] = z ^ (z >> 31)
	}
	return t
}()

// Chunker reads a stream and returns one chunk per Next call.
type Chunker struct {
	r                 *bufio.Reader
	cfg               Config
	maskSmall, maskLg uint64
	buf               []byte
}

// New returns a Chunker over r. It panics on an invalid Config (a programming error).
func New(r io.Reader, cfg Config) *Chunker {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	b := bits.TrailingZeros(uint(cfg.Avg)) // log2(Avg)
	return &Chunker{
		r:   bufio.NewReaderSize(r, 1<<16),
		cfg: cfg,
		// More 1-bits = harder to match. Before Avg we want boundaries to be rare (b+1 bits),
		// after Avg we want them to come soon (b-1 bits). The bits are spread over the high half
		// of the hash, where the gear hash has mixed in the most bytes.
		maskSmall: spreadMask(b + 1),
		maskLg:    spreadMask(b - 1),
		buf:       make([]byte, 0, cfg.Max),
	}
}

func spreadMask(n int) uint64 {
	var m uint64
	for i := 0; i < n; i++ {
		m |= 1 << (63 - 2*i%64)
	}
	return m
}

// Next returns the next chunk, or io.EOF when the stream is finished. The returned slice is
// reused by the following call, so callers must finish with it (hash, encrypt, upload) first.
func (c *Chunker) Next() ([]byte, error) {
	c.buf = c.buf[:0]
	var h uint64
	for {
		b, err := c.r.ReadByte()
		if err == io.EOF {
			if len(c.buf) == 0 {
				return nil, io.EOF
			}
			return c.buf, nil
		}
		if err != nil {
			return nil, err
		}
		c.buf = append(c.buf, b)
		n := len(c.buf)
		h = (h << 1) + gear[b]
		switch {
		case n < c.cfg.Min:
			// Never cut below Min: tiny chunks would mean too many objects and too much metadata.
		case n < c.cfg.Avg:
			if h&c.maskSmall == 0 {
				return c.buf, nil
			}
		case n < c.cfg.Max:
			if h&c.maskLg == 0 {
				return c.buf, nil
			}
		default:
			return c.buf, nil // hard cap
		}
	}
}
