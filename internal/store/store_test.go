package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// These tests run against a real SQLite file rather than a fake. The behaviour
// under test — the UNIQUE constraint, the column default, the single-writer
// batching — lives in the database and the driver, so a mock would only ever
// confirm that the test author remembered what the schema says.

// newTestStore opens a store on a throwaway database file and runs its writer
// for the rest of the test. Every test wants this shape unless it is
// specifically about the open/close lifecycle.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	startWriter(t, s)
	return s
}

// openTestStore opens path and closes the pool when the test ends. No writer
// is started, so InsertJob would block: prefer newTestStore.
func openTestStore(t *testing.T, path string) *Store {
	t.Helper()

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%q) returned error: %v", path, err)
	}
	t.Cleanup(func() {
		// database/sql makes Close idempotent, so a test that shuts the store
		// down early still gets a safety net here.
		if err := s.Close(); err != nil {
			t.Errorf("Close() returned error: %v", err)
		}
	})
	return s
}

// startWriter runs s.RunWriter on its own cancellable context and returns a
// function that stops it and waits for it to exit.
//
// The ordering is the whole point. InsertJob blocks until the writer
// acknowledges its batch, so the writer must outlive every insert; and the
// writer flushes what it has already accepted on the way out, so it must exit
// before the pool it writes through is closed. t.Cleanup runs
// last-registered-first, and openTestStore registered the Close before this
// call — so cleanup cancels the writer, waits, and only then closes.
func startWriter(t *testing.T, s *Store) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunWriter(ctx)
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// insertJobs pushes jobs through the writer in order, failing on the first
// error so that a broken setup is not mistaken for a broken assertion.
func insertJobs(t *testing.T, s *Store, jobs ...Job) {
	t.Helper()

	for _, job := range jobs {
		if err := s.InsertJob(t.Context(), job.EventID, job.TaskName); err != nil {
			t.Fatalf("InsertJob(%+v) returned error: %v", job, err)
		}
	}
}

func TestInsertJobPersistsRow(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	want := Job{EventID: "evt-0007-1a2b3c4d", TaskName: "ReconcileLedger"}

	// CURRENT_TIMESTAMP has one-second granularity, so the lower bound has to
	// be truncated or a row written mid-second looks like it predates the test.
	before := time.Now().UTC().Truncate(time.Second)
	insertJobs(t, s, want)

	var got struct {
		EventID   string    `db:"event_id"`
		TaskName  string    `db:"task_name"`
		CreatedAt time.Time `db:"created_at"`
	}
	if err := s.db.GetContext(t.Context(), &got,
		"SELECT event_id, task_name, created_at FROM transient_jobs"); err != nil {
		t.Fatalf("reading the row back: %v", err)
	}
	after := time.Now().UTC()

	if got.EventID != want.EventID {
		t.Errorf("event_id = %q, want %q", got.EventID, want.EventID)
	}
	if got.TaskName != want.TaskName {
		t.Errorf("task_name = %q, want %q", got.TaskName, want.TaskName)
	}
	// The insert never mentions created_at, so a timestamp inside the window
	// the test just spanned can only have come from the column's DEFAULT.
	if got.CreatedAt.Before(before) || got.CreatedAt.After(after) {
		t.Errorf("created_at = %v, want a time within [%v, %v]", got.CreatedAt, before, after)
	}
}

func TestInsertJobIsIdempotentPerEventID(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	const eventID = "evt-0007-1a2b3c4d"

	// Delivery is at-least-once, so the broker will replay this event. The
	// replay must be accepted — an error would make the handler retry forever
	// — and must neither add a row nor overwrite the one already there.
	insertJobs(t, s,
		Job{EventID: eventID, TaskName: "ReconcileLedger"},
		Job{EventID: eventID, TaskName: "RebuildIndex"},
	)

	if got, err := s.CountJobs(t.Context()); err != nil || got != 1 {
		t.Fatalf("CountJobs() = %d, %v, want 1, <nil>", got, err)
	}

	var got string
	if err := s.db.GetContext(t.Context(), &got,
		"SELECT task_name FROM transient_jobs WHERE event_id = ?", eventID); err != nil {
		t.Fatalf("reading task_name back: %v", err)
	}
	if want := "ReconcileLedger"; got != want {
		t.Errorf("task_name = %q, want %q: the first delivery wins", got, want)
	}
}

func TestEventIDsListsEveryIDAscending(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Inserted out of order so the assertion pins the documented ascending
	// contract rather than the insertion sequence. Note that SQLite answers
	// this query with a covering scan of the implicit index behind
	// event_id UNIQUE, so the rows arrive sorted whether or not EventIDs asks
	// for it: this guards the contract, not the ORDER BY clause itself.
	insertJobs(t, s,
		Job{EventID: "evt-0003-cccccccc", TaskName: "ReconcileLedger"},
		Job{EventID: "evt-0001-aaaaaaaa", TaskName: "RebuildIndex"},
		Job{EventID: "evt-0002-bbbbbbbb", TaskName: "PurgeCache"},
	)

	if got, err := s.CountJobs(t.Context()); err != nil || got != 3 {
		t.Fatalf("CountJobs() = %d, %v, want 3, <nil>", got, err)
	}

	got, err := s.EventIDs(t.Context())
	if err != nil {
		t.Fatalf("EventIDs() returned error: %v", err)
	}
	want := []string{"evt-0001-aaaaaaaa", "evt-0002-bbbbbbbb", "evt-0003-cccccccc"}
	if !slices.Equal(got, want) {
		t.Errorf("EventIDs() = %v, want %v", got, want)
	}
}

func TestInsertJobBatchesConcurrentWriters(t *testing.T) {
	t.Parallel()

	const (
		// More writers than maxBatch so full batches genuinely form, and
		// enough rows overall to need several of them.
		writers   = 100
		perWriter = 3
		total     = writers * perWriter
	)

	s := newTestStore(t)
	// Zero-padded so lexicographic order matches (writer, sequence) order.
	eventID := func(writer, seq int) string { return fmt.Sprintf("evt-%03d-%03d", writer, seq) }

	// Errors come back over a buffered channel rather than a shared slice: the
	// race detector is on, and an unsynchronised append is exactly the bug
	// this test would otherwise report as a batching failure.
	errs := make(chan error, total)
	ctx := t.Context()

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				errs <- s.InsertJob(ctx, eventID(w, i), "ReconcileLedger")
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("InsertJob() returned error: %v", err)
		}
	}

	if got, err := s.CountJobs(t.Context()); err != nil || got != total {
		t.Fatalf("CountJobs() = %d, %v, want %d, <nil>", got, err, total)
	}

	want := make([]string, 0, total)
	for w := range writers {
		for i := range perWriter {
			want = append(want, eventID(w, i))
		}
	}
	got, err := s.EventIDs(t.Context())
	if err != nil {
		t.Fatalf("EventIDs() returned error: %v", err)
	}
	// Equality both ways: every row landed, and none landed twice or garbled.
	if !slices.Equal(got, want) {
		t.Errorf("EventIDs() returned %d ids that do not match the %d inserted", len(got), total)
	}
}

func TestInsertJobFlushesPartialBatch(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// A lone insert can never fill a 64-row batch, so it only ever commits
	// because the flush timer fires. Running it off the test goroutine turns a
	// dead timer into a failure instead of a hang.
	done := make(chan error, 1)
	go func() {
		done <- s.InsertJob(t.Context(), "evt-0007-1a2b3c4d", "ReconcileLedger")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("InsertJob() returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("InsertJob() did not return: the partial batch was never flushed")
	}

	if got, err := s.CountJobs(t.Context()); err != nil || got != 1 {
		t.Errorf("CountJobs() = %d, %v, want 1, <nil>", got, err)
	}
}

func TestInsertJobFailsOnceWriterStopped(t *testing.T) {
	t.Parallel()

	s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	stopWriter := startWriter(t, s)

	// Not the documented order of operations, but a shutdown race can produce
	// it, and a handler that blocks here would wedge its worker for good.
	stopWriter()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.InsertJob(ctx, "evt-0007-1a2b3c4d", "ReconcileLedger")
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("InsertJob() = <nil>, want an error once the writer has stopped")
		}
		// Whether the caller is rejected by drain or by its own deadline
		// depends on how far the shutdown had got; both are correct.
		if !errors.Is(err, ErrWriterStopped) && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("InsertJob() = %v, want %v or a context error", err, ErrWriterStopped)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("InsertJob() blocked after the writer stopped")
	}
}

func TestInsertJobHonoursCallerContext(t *testing.T) {
	t.Parallel()

	t.Run("cancelled before submission", func(t *testing.T) {
		t.Parallel()

		// No writer on purpose: with nothing receiving on the write channel,
		// the cancelled context is the only case InsertJob's first select can
		// take, so the assertion is decided by the code, not by scheduling.
		s := openTestStore(t, filepath.Join(t.TempDir(), "jobs.db"))

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		assertInsertFails(t, s, ctx, context.Canceled)
	})

	t.Run("deadline expires waiting for the commit", func(t *testing.T) {
		t.Parallel()

		s := newTestStore(t)

		// The pool is pinned to a single connection, so holding a transaction
		// open stalls the writer inside its flush. That is the interesting
		// failure: the job has been accepted but will not be acknowledged, and
		// a caller with a deadline must give up rather than pin its worker to
		// a stuck writer.
		tx, err := s.db.BeginTxx(t.Context(), nil)
		if err != nil {
			t.Fatalf("BeginTxx() returned error: %v", err)
		}
		defer tx.Rollback()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		assertInsertFails(t, s, ctx, context.DeadlineExceeded)
	})
}

// assertInsertFails runs one insert off the test goroutine and requires it to
// come back with want. The surrounding select is the point: an InsertJob that
// ignored its context would block forever, and a hung test reports far less
// than a failed one.
func assertInsertFails(t *testing.T, s *Store, ctx context.Context, want error) {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- s.InsertJob(ctx, "evt-0007-1a2b3c4d", "ReconcileLedger")
	}()

	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Errorf("InsertJob() = %v, want %v", err, want)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("InsertJob() blocked instead of returning %v", want)
	}
}

func TestOpenIsIdempotentAcrossRestarts(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "jobs.db")
	want := Job{EventID: "evt-0007-1a2b3c4d", TaskName: "ReconcileLedger"}

	first := openTestStore(t, path)
	stopWriter := startWriter(t, first)
	insertJobs(t, first, want)

	// Shut the first store down completely: the row has to be on disk, not
	// merely visible to the connection that wrote it.
	stopWriter()
	if err := first.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	// Reopening must adopt the existing schema (CREATE TABLE IF NOT EXISTS)
	// rather than error or start empty.
	second := openTestStore(t, path)
	got, err := second.EventIDs(t.Context())
	if err != nil {
		t.Fatalf("EventIDs() after reopening returned error: %v", err)
	}
	if len(got) != 1 || got[0] != want.EventID {
		t.Errorf("EventIDs() after reopening = %v, want [%s]", got, want.EventID)
	}
}

func TestOpenRejectsUnusablePath(t *testing.T) {
	t.Parallel()

	// A directory exists and is readable but can never be a database file, so
	// it exercises the failure path without depending on filesystem
	// permissions. Open must report it, not hand back an unusable store.
	dir := t.TempDir()

	s, err := Open(dir)
	if err == nil {
		t.Fatalf("Open(%q) = %v, <nil>, want an error", dir, s)
	}
	if s != nil {
		t.Errorf("Open(%q) returned a store alongside its error", dir)
	}
}

func TestSchemaRejectsInvalidRows(t *testing.T) {
	t.Parallel()

	// These go straight at the pool. InsertJob's ON CONFLICT DO NOTHING is
	// precisely what would mask a missing constraint, and the table is the
	// last line of defence, so it has to be checked without that shield.
	const insert = `INSERT INTO transient_jobs (event_id, task_name) VALUES (?, ?)`

	tests := []struct {
		name string
		seed *Job // inserted through the writer first, when the case needs a clash
		args []any
		want sqlite3.ErrNoExtended
	}{
		{
			name: "event_id may not be null",
			args: []any{nil, "ReconcileLedger"},
			want: sqlite3.ErrConstraintNotNull,
		},
		{
			name: "task_name may not be null",
			args: []any{"evt-0007-1a2b3c4d", nil},
			want: sqlite3.ErrConstraintNotNull,
		},
		{
			name: "event_id may not repeat",
			seed: &Job{EventID: "evt-0007-1a2b3c4d", TaskName: "ReconcileLedger"},
			args: []any{"evt-0007-1a2b3c4d", "RebuildIndex"},
			want: sqlite3.ErrConstraintUnique,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStore(t)
			if tc.seed != nil {
				insertJobs(t, s, *tc.seed)
			}

			_, err := s.db.ExecContext(t.Context(), insert, tc.args...)

			var sqlErr sqlite3.Error
			if !errors.As(err, &sqlErr) {
				t.Fatalf("inserting %v = %v, want a sqlite3.Error", tc.args, err)
			}
			if sqlErr.ExtendedCode != tc.want {
				t.Errorf("extended code = %d (%v), want %d", sqlErr.ExtendedCode, sqlErr, tc.want)
			}
		})
	}
}

func TestEmptyDatabaseReadsCleanly(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Open creates the table, so "nothing written yet" must read as zero rows
	// rather than as a missing-table error.
	if got, err := s.CountJobs(t.Context()); err != nil || got != 0 {
		t.Errorf("CountJobs() = %d, %v, want 0, <nil>", got, err)
	}

	got, err := s.EventIDs(t.Context())
	if err != nil {
		t.Fatalf("EventIDs() returned error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("EventIDs() = %v, want no ids", got)
	}
}
