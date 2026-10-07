package engine

// Integration tests against real PostgreSQL and real S3 (LocalStack in CI). They run when
// CBS_TEST_PG_URL (an admin connection, e.g. postgres://postgres@localhost:5432/postgres?sslmode=disable)
// and CBS_TEST_S3_ENDPOINT are set, and skip otherwise.

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/chunk"
	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/vault"
)

func testS3(t *testing.T) *vault.S3 {
	t.Helper()
	ep := os.Getenv("CBS_TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("CBS_TEST_S3_ENDPOINT not set")
	}
	ctx := context.Background()
	s, err := vault.NewS3(ctx, fmt.Sprintf("cbs-engine-%d", time.Now().UnixNano()), ep, time.Hour, vault.Governance)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

// tempDB creates an empty database and returns its DSN; it is dropped when the test ends.
func tempDB(t *testing.T, name string) (string, *sql.DB) {
	t.Helper()
	admin := os.Getenv("CBS_TEST_PG_URL")
	if admin == "" {
		t.Skip("CBS_TEST_PG_URL not set")
	}
	adb, err := sql.Open("postgres", admin)
	if err != nil {
		t.Fatal(err)
	}
	name = fmt.Sprintf("%s_%d", name, time.Now().UnixNano())
	if _, err := adb.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(admin)
	u.Path = "/" + name
	dsn := u.String()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		adb.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		adb.Close()
	})
	return dsn, db
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// fingerprint summarises a table's full content so two databases can be compared exactly.
func fingerprint(t *testing.T, db *sql.DB) string {
	t.Helper()
	var n int
	var sum string
	err := db.QueryRow(`SELECT count(*), md5(string_agg(id || ':' || customer || ':' || amount_cents || ':' || note, ',' ORDER BY id))
		FROM orders`).Scan(&n, &sum)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%d rows, md5 %s", n, sum)
}

func TestPostgresBackupIncrementalAndRestore(t *testing.T) {
	ctx := context.Background()
	store := testS3(t)
	srcDSN, src := tempDB(t, "cbs_src")
	e := testEngine(t, store)
	e.Chunking = chunk.Config{Min: 16 << 10, Avg: 64 << 10, Max: 256 << 10}

	mustExec(t, src, `CREATE TABLE orders (id int PRIMARY KEY, customer text NOT NULL, amount_cents bigint NOT NULL, note text NOT NULL)`)
	mustExec(t, src, `INSERT INTO orders SELECT g, 'customer-' || (g % 997), (g * 7919) % 100000, md5(g::text) || md5((g*3)::text)
		FROM generate_series(1, 50000) g`)
	before := fingerprint(t, src)

	first, err := e.BackupPostgres(ctx, "pg-1", srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	if first.Stats.NewChunks < 10 {
		t.Fatalf("expected a multi-chunk dump, got %d chunks", first.Stats.NewChunks)
	}

	// Change a handful of rows near the end of the table.
	mustExec(t, src, `UPDATE orders SET amount_cents = amount_cents + 1 WHERE id IN (49990, 49995, 50000)`)
	after := fingerprint(t, src)
	second, err := e.BackupPostgres(ctx, "pg-2", srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	total := second.Stats.NewChunks + second.Stats.ReusedChunks
	if second.Stats.NewChunks*4 > total {
		t.Fatalf("a 3-row change re-uploaded %d of %d chunks", second.Stats.NewChunks, total)
	}
	t.Logf("first backup: %d bytes logical, %d uploaded, %d chunks", first.Stats.LogicalBytes, first.Stats.UploadedBytes, first.Stats.NewChunks)
	t.Logf("after 3-row update: %d new chunks, %d reused, %d bytes uploaded", second.Stats.NewChunks, second.Stats.ReusedChunks, second.Stats.UploadedBytes)

	// Restore each snapshot into its own empty database and compare every row.
	for _, c := range []struct{ id, want string }{{"pg-1", before}, {"pg-2", after}} {
		dsn, db := tempDB(t, "cbs_restore")
		snap, err := e.Load(ctx, c.id)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.RestorePostgres(ctx, snap, dsn); err != nil {
			t.Fatal(err)
		}
		if got := fingerprint(t, db); got != c.want {
			t.Fatalf("%s restored %s, want %s", c.id, got, c.want)
		}
	}
	if before == after {
		t.Fatal("test bug: the update did not change the data")
	}
}

func TestPostgresBackupOfMissingDatabaseFails(t *testing.T) {
	admin := os.Getenv("CBS_TEST_PG_URL")
	if admin == "" {
		t.Skip("CBS_TEST_PG_URL not set")
	}
	u, _ := url.Parse(admin)
	u.Path = "/does_not_exist"
	e := testEngine(t, &vault.Local{Root: t.TempDir()})
	_, err := e.BackupPostgres(context.Background(), "x", u.String())
	if err == nil || !strings.Contains(err.Error(), "pg_dump failed") {
		t.Fatalf("want pg_dump failure, got %v", err)
	}
	if snaps, _ := e.List(context.Background()); len(snaps) != 0 {
		t.Fatal("a failed dump must not produce a snapshot")
	}
}
