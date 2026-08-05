// Package store owns the Postgres database: the schema, the connection pool and
// the single writer goroutine that every insert funnels through.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	// Registers the "pgx" driver with database/sql.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Job is one row of transient_jobs. The db tags let sqlx bind the struct
// directly as named parameters.
type Job struct {
	EventID  string `db:"event_id"`
	TaskName string `db:"task_name"`
}

const schema = `
CREATE TABLE IF NOT EXISTS transient_jobs (
	id         BIGSERIAL PRIMARY KEY,
	event_id   TEXT     NOT NULL UNIQUE,
	task_name  TEXT     NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`

// Queue delivery is at-least-once, so the same event_id will arrive twice.
// The UNIQUE constraint plus DO NOTHING makes a redelivery a no-op rather
// than a duplicate row.
const insertJob = `
INSERT INTO transient_jobs (event_id, task_name)
VALUES (:event_id, :task_name)
ON CONFLICT(event_id) DO NOTHING`

const (
	// maxBatch caps how many inserts share one transaction.
	maxBatch = 64
	// flushInterval bounds how long an insert waits for batch-mates.
	flushInterval = 5 * time.Millisecond
)

// ErrWriterStopped reports an insert submitted after the writer exited.
var ErrWriterStopped = errors.New("store: writer has stopped")

type writeRequest struct {
	job Job
	ack chan error
}

// Store is the database handle plus its serialised writer.
type Store struct {
	db     *sqlx.DB
	writes chan writeRequest
}

// Open connects to Postgres and ensures the schema exists.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	db, err := sqlx.ConnectContext(ctx, "pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to Postgres: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialising schema: %w", err)
	}
	return &Store{db: db, writes: make(chan writeRequest)}, nil
}

// Close releases the connection pool.
func (s *Store) Close() error { return s.db.Close() }

// RunWriter owns the write side of the database until ctx is cancelled.
//
// Callers must cancel it only after every producer of inserts has stopped:
// InsertJob blocks on an acknowledgement, so stopping the writer first would
// strand handlers that are mid-insert.
func (s *Store) RunWriter(ctx context.Context) {
	// Reused across flushes: the backing array is retained by batch[:0], so
	// steady-state batching allocates nothing.
	batch := make([]writeRequest, 0, maxBatch)

	timer := time.NewTimer(flushInterval)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Commit what is already accepted; the caller is entitled to an
			// answer even though the context is done.
			s.flush(context.WithoutCancel(ctx), batch)
			s.drain()
			return

		case req := <-s.writes:
			batch = append(batch, req)
			if len(batch) == 1 {
				timer.Reset(flushInterval)
			}
			if len(batch) >= maxBatch {
				if !timer.Stop() {
					<-timer.C
				}
				s.flush(ctx, batch)
				batch = batch[:0]
			}

		case <-timer.C:
			s.flush(ctx, batch)
			batch = batch[:0]
		}
	}
}

// drain rejects requests already queued when the writer stopped, so no caller
// waits forever on an acknowledgement that will never arrive.
func (s *Store) drain() {
	for {
		select {
		case req := <-s.writes:
			req.ack <- ErrWriterStopped
		default:
			return
		}
	}
}

func (s *Store) flush(ctx context.Context, batch []writeRequest) {
	if len(batch) == 0 {
		return
	}

	err := s.writeBatch(ctx, batch)
	for _, req := range batch {
		req.ack <- err
	}
}

// writeBatch commits the whole batch in one transaction. One fsync for up to
// maxBatch rows is the entire point of batching.
func (s *Store) writeBatch(ctx context.Context, batch []writeRequest) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareNamedContext(ctx, insertJob)
	if err != nil {
		return fmt.Errorf("preparing insert: %w", err)
	}
	defer stmt.Close()

	for _, req := range batch {
		if _, err := stmt.ExecContext(ctx, req.job); err != nil {
			return fmt.Errorf("inserting job %s: %w", req.job.EventID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing %d jobs: %w", len(batch), err)
	}
	return nil
}

// InsertJob hands the job to the writer and waits for the batch to commit.
//
// It takes primitives and maps them onto Job here so that callers need no type
// from this package: internal/jobs declares the interface this satisfies, and
// its handlers stay free of anything Postgres-specific.
func (s *Store) InsertJob(ctx context.Context, eventID, taskName string) error {
	ack := make(chan error, 1)
	req := writeRequest{job: Job{EventID: eventID, TaskName: taskName}, ack: ack}

	select {
	case s.writes <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CountJobs reports how many jobs have been persisted.
func (s *Store) CountJobs(ctx context.Context) (int, error) {
	var n int
	if err := s.db.GetContext(ctx, &n, "SELECT COUNT(*) FROM transient_jobs"); err != nil {
		return 0, fmt.Errorf("counting jobs: %w", err)
	}
	return n, nil
}

// EventIDs lists the persisted event ids in ascending order.
func (s *Store) EventIDs(ctx context.Context) ([]string, error) {
	var ids []string
	if err := s.db.SelectContext(ctx, &ids,
		"SELECT event_id FROM transient_jobs ORDER BY event_id"); err != nil {
		return nil, fmt.Errorf("listing event ids: %w", err)
	}
	return ids, nil
}
