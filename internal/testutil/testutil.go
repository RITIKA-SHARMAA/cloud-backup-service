// Package testutil gives integration tests a fresh, migrated catalog database.
package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/RITIKA-SHARMAA/cloud-backup-service/internal/catalog"
)

// Catalog creates an empty database, applies the schema and drops it after the test.
// It skips the test when CBS_TEST_PG_URL is not set.
func Catalog(t *testing.T) *sql.DB {
	t.Helper()
	admin := os.Getenv("CBS_TEST_PG_URL")
	if admin == "" {
		t.Skip("CBS_TEST_PG_URL not set")
	}
	adb, err := sql.Open("postgres", admin)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("cbs_catalog_%d", time.Now().UnixNano())
	if _, err := adb.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(admin)
	u.Path = "/" + name
	db, err := catalog.Open(context.Background(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		adb.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		adb.Close()
	})
	return db
}
