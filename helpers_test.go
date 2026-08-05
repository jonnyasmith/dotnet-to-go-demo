package main

import (
	"bytes"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/jmoiron/sqlx"
)

// discardLogger is the equivalent of injecting NullLogger<T>.Instance.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// captureLogger returns a logger plus the buffer it writes to, for the rare
// test where the log line *is* the observable behaviour.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, nil)), buf
}

// newTestDB creates a real SQLite database in a per-test temporary directory.
//
// Go integration tests hit the real engine rather than a mocked DbContext:
// SQLite is a library, so a genuine database costs about a millisecond.
// t.TempDir and t.Cleanup remove the file and close the pool automatically,
// which is what IAsyncLifetime.DisposeAsync does for you in xUnit.
func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()

	db, err := initialiseDatabase(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("initialising test database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing test database: %v", err)
		}
	})
	return db
}

// countJobs is a small query helper shared by the integration tests.
func countJobs(t *testing.T, db *sqlx.DB) int {
	t.Helper()

	var n int
	if err := db.Get(&n, "SELECT COUNT(*) FROM transient_jobs"); err != nil {
		t.Fatalf("counting jobs: %v", err)
	}
	return n
}
