package main

import (
	"path/filepath"
	"testing"
)

func TestInitialiseDatabaseIsIdempotent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "demo.db")

	db, err := initialiseDatabase(path)
	if err != nil {
		t.Fatalf("first initialisation: %v", err)
	}
	if _, err := db.Exec(
		"INSERT INTO transient_jobs (event_id, task_name) VALUES ('evt-1', 'X')"); err != nil {
		t.Fatalf("seeding a row: %v", err)
	}
	db.Close()

	// Re-opening must reuse the existing table rather than recreating it.
	reopened, err := initialiseDatabase(path)
	if err != nil {
		t.Fatalf("second initialisation: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	if n := countJobs(t, reopened); n != 1 {
		t.Errorf("found %d rows after reopening, want the 1 seeded row", n)
	}
}

func TestInitialiseDatabaseRejectsAnUnusablePath(t *testing.T) {
	t.Parallel()

	// A directory is not a database file, so this must fail rather than panic.
	if _, err := initialiseDatabase(t.TempDir()); err == nil {
		t.Fatal("initialising a directory succeeded, want an error")
	}
}

func TestSchemaRequiresJobFields(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	if _, err := db.Exec("INSERT INTO transient_jobs (event_id) VALUES ('evt-1')"); err == nil {
		t.Error("insert without task_name succeeded, want a NOT NULL violation")
	}
	if _, err := db.Exec("INSERT INTO transient_jobs (task_name) VALUES ('X')"); err == nil {
		t.Error("insert without event_id succeeded, want a NOT NULL violation")
	}
}
