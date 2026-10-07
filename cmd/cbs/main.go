// Command cbs is the command-line tool. It talks to the vault directly (no catalog database),
// so it works on a laptop with just a folder or a bucket.
//
//	cbs keygen
//	cbs init-vault
//	cbs backup --dir PATH | --pg DSN
//	cbs snapshots
//	cbs show SNAPSHOT_ID
//	cbs restore SNAPSHOT_ID --to-dir PATH | --to-pg DSN
//	cbs verify SNAPSHOT_ID
//	cbs gc --keep-days N
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/config"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/engine"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/manifest"
)

const usage = `cbs: encrypted, deduplicated backups into an Object Lock vault

Commands:
  keygen                                   print a new random encryption key
  init-vault                               create the S3 vault bucket with Object Lock
  backup   --dir PATH | --pg DSN           create a snapshot
  snapshots                                list snapshots
  show     SNAPSHOT_ID                     show a snapshot's files and stats
  restore  SNAPSHOT_ID --to-dir PATH | --to-pg DSN
  verify   SNAPSHOT_ID                     re-read and check every chunk
  gc       --keep-days N                   delete expired snapshots and unreferenced chunks
                                           (do not run while a backup is in progress)

Environment:
  CBS_VAULT_URL        s3://bucket or file:///path (default file://./vault)
  CBS_S3_ENDPOINT      S3-compatible endpoint, e.g. http://localhost:4566 (LocalStack)
  CBS_ENCRYPTION_KEY   64 hex characters (cbs keygen)
  CBS_RETENTION        Object Lock retention for new objects (default 720h)
  CBS_LOCK_MODE        GOVERNANCE (default) or COMPLIANCE
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Print(usage)
		return nil
	}
	ctx, stop := config.SignalContext()
	defer stop()
	cmd, rest := args[0], args[1:]
	if cmd == "keygen" {
		k := make([]byte, 32)
		rand.Read(k)
		fmt.Println(hex.EncodeToString(k))
		return nil
	}

	cfg, err := config.Load("")
	if err != nil {
		return err
	}
	e, err := cfg.Engine(ctx, cmd == "init-vault" || cmd == "backup")
	if err != nil {
		return err
	}

	switch cmd {
	case "init-vault":
		fmt.Println("vault ready:", e.Store)
		return nil
	case "backup":
		fs := flag.NewFlagSet("backup", flag.ContinueOnError)
		dir := fs.String("dir", "", "directory to back up")
		pg := fs.String("pg", "", "PostgreSQL connection string to back up")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		return backup(ctx, e, *dir, *pg)
	case "snapshots":
		return listSnapshots(ctx, e)
	case "show", "verify":
		snap, err := loadArg(ctx, e, rest)
		if err != nil {
			return err
		}
		if cmd == "show" {
			return show(snap)
		}
		start := time.Now()
		if err := e.Verify(ctx, snap); err != nil {
			return err
		}
		fmt.Printf("snapshot %s OK: every chunk authenticated and every file checksum matches (%s)\n", snap.ID, time.Since(start).Round(time.Millisecond))
		return nil
	case "restore":
		if len(rest) == 0 {
			return errors.New("usage: cbs restore SNAPSHOT_ID --to-dir PATH | --to-pg DSN")
		}
		fs := flag.NewFlagSet("restore", flag.ContinueOnError)
		toDir := fs.String("to-dir", "", "directory to restore into")
		toPG := fs.String("to-pg", "", "empty PostgreSQL database to restore into")
		if err := fs.Parse(rest[1:]); err != nil {
			return err
		}
		snap, err := e.Load(ctx, rest[0])
		if err != nil {
			return err
		}
		start := time.Now()
		switch {
		case *toDir != "":
			err = e.RestoreDir(ctx, snap, *toDir)
		case *toPG != "":
			err = e.RestorePostgres(ctx, snap, *toPG)
		default:
			return errors.New("restore needs --to-dir or --to-pg")
		}
		if err != nil {
			return err
		}
		fmt.Printf("restored %s in %s (all checksums verified)\n", snap.ID, time.Since(start).Round(time.Millisecond))
		return nil
	case "gc":
		fs := flag.NewFlagSet("gc", flag.ContinueOnError)
		keep := fs.Int("keep-days", 0, "keep snapshots newer than this many days")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *keep < 1 {
			return errors.New("gc needs --keep-days of at least 1")
		}
		r, err := e.GC(ctx, time.Now().AddDate(0, 0, -*keep))
		if err != nil {
			return err
		}
		fmt.Printf("deleted %d snapshots and %d chunks; %d objects still locked by retention\n", r.SnapshotsDeleted, r.ChunksDeleted, r.StillLocked)
		return nil
	}
	return fmt.Errorf("unknown command %q (see cbs help)", cmd)
}

func backup(ctx context.Context, e *engine.Engine, dir, pg string) error {
	id := manifest.NewID(time.Now())
	var snap *manifest.Snapshot
	var err error
	switch {
	case dir != "" && pg == "":
		snap, err = e.BackupDir(ctx, id, dir)
	case pg != "" && dir == "":
		snap, err = e.BackupPostgres(ctx, id, pg)
	default:
		return errors.New("backup needs exactly one of --dir or --pg")
	}
	if err != nil {
		return err
	}
	st := snap.Stats
	fmt.Printf("snapshot %s\n", snap.ID)
	fmt.Printf("  source         %s %s\n", snap.SourceType, snap.Source)
	fmt.Printf("  logical size   %s\n", human(st.LogicalBytes))
	fmt.Printf("  uploaded       %s (compressed + encrypted)\n", human(st.UploadedBytes))
	fmt.Printf("  chunks         %d new, %d already in the vault\n", st.NewChunks, st.ReusedChunks)
	fmt.Printf("  took           %s\n", time.Duration(st.DurationMS)*time.Millisecond)
	return nil
}

func listSnapshots(ctx context.Context, e *engine.Engine) error {
	snaps, err := e.List(ctx)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tTYPE\tSOURCE\tSIZE\tUPLOADED\tNEW/REUSED CHUNKS")
	for _, s := range snaps {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d/%d\n", s.ID, s.SourceType, s.Source,
			human(s.Stats.LogicalBytes), human(s.Stats.UploadedBytes), s.Stats.NewChunks, s.Stats.ReusedChunks)
	}
	return tw.Flush()
}

func show(s *manifest.Snapshot) error {
	fmt.Printf("snapshot %s (%s %s), created %s\n", s.ID, s.SourceType, s.Source, s.CreatedAt.Format(time.RFC3339))
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PATH\tSIZE\tCHUNKS\tSHA256")
	for _, f := range s.Files {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", f.Path, human(f.Size), len(f.Chunks), f.SHA256[:16]+"...")
	}
	return tw.Flush()
}

func loadArg(ctx context.Context, e *engine.Engine, args []string) (*manifest.Snapshot, error) {
	if len(args) != 1 {
		return nil, errors.New("expected one SNAPSHOT_ID")
	}
	return e.Load(ctx, args[0])
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}
