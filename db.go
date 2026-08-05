package main

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	// Registers the "sqlite3" driver with database/sql.
	_ "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE IF NOT EXISTS transient_jobs (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id   TEXT     NOT NULL,
	task_name  TEXT     NOT NULL,
	created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`

// initialiseDatabase opens the SQLite file and ensures the schema exists.
//
// SQLite permits only one writer at a time, and five workers insert
// concurrently, so the pool is pinned to a single connection and the driver is
// told to wait rather than fail immediately on a held lock.
func initialiseDatabase(path string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("sqlite3", path+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialising schema: %w", err)
	}
	return db, nil
}
