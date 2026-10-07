package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/manifest"
)

// writeFile streams one file's chunks to w, checking every chunk on the way:
//  1. GCM authentication (any change to the stored bytes fails),
//  2. the decrypted content's ID must equal the ID in the manifest,
//  3. at the end, the SHA-256 of the whole file must match the manifest.
//
// Checks 1 and 2 run before a chunk is written, so w never receives unverified bytes, which
// matters when w is pg_restore applying data to a live database.
func (e *Engine) writeFile(ctx context.Context, f manifest.File, w io.Writer) error {
	whole := sha256.New()
	out := io.MultiWriter(w, whole)
	for i, ref := range f.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		sealed, err := e.Store.Get(ctx, manifest.ChunkKey(ref.ID), ref.Version)
		if err != nil {
			return fmt.Errorf("%s chunk %d: %w", f.Path, i, err)
		}
		plain, err := e.Codec.Open(sealed, []byte(ref.ID))
		if err != nil {
			return fmt.Errorf("%s chunk %d: %w", f.Path, i, err)
		}
		if e.Codec.ChunkID(plain) != ref.ID || len(plain) != ref.Size {
			return fmt.Errorf("%s chunk %d: content does not match the manifest", f.Path, i)
		}
		if _, err := out.Write(plain); err != nil {
			return err
		}
	}
	if got := hex.EncodeToString(whole.Sum(nil)); got != f.SHA256 {
		return fmt.Errorf("%s: restored checksum %s does not match %s", f.Path, got, f.SHA256)
	}
	return nil
}

// RestoreDir writes a directory snapshot under dest. Each file is written as name.partial and
// renamed only after its checksum matches, so a failed restore never leaves a corrupt file
// under the real name.
func (e *Engine) RestoreDir(ctx context.Context, snap *manifest.Snapshot, dest string) error {
	if snap.SourceType != manifest.SourceDir {
		return fmt.Errorf("snapshot %s is a %s backup, not a directory", snap.ID, snap.SourceType)
	}
	root, err := filepath.Abs(dest)
	if err != nil {
		return err
	}
	for _, f := range snap.Files {
		target := filepath.Join(root, filepath.FromSlash(f.Path))
		// Refuse paths like ../../etc/passwd from a malicious or corrupted manifest.
		if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
			return fmt.Errorf("refusing to restore %q outside %s", f.Path, root)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		mode := os.FileMode(f.Mode)
		if mode == 0 {
			mode = 0o644
		}
		tmp := target + ".partial"
		fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
		if err != nil {
			return err
		}
		err = e.writeFile(ctx, f, fh)
		if cerr := fh.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			os.Remove(tmp)
			return err
		}
		if err := os.Rename(tmp, target); err != nil {
			return err
		}
	}
	return nil
}

// Verify reads and checks every chunk of a snapshot without writing anything.
func (e *Engine) Verify(ctx context.Context, snap *manifest.Snapshot) error {
	for _, f := range snap.Files {
		if err := e.writeFile(ctx, f, io.Discard); err != nil {
			return err
		}
	}
	return nil
}
