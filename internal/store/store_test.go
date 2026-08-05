package store

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var testDatabaseURL string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("jobs"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting Postgres test container:", err)
		os.Exit(1)
	}
	testDatabaseURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "getting Postgres connection string:", err)
		_ = testcontainers.TerminateContainer(container)
		os.Exit(1)
	}

	code := m.Run()
	if err := testcontainers.TerminateContainer(container); err != nil && code == 0 {
		fmt.Fprintln(os.Stderr, "stopping Postgres test container:", err)
		code = 1
	}
	os.Exit(code)
}

var nonIdentifier = regexp.MustCompile(`[^a-z0-9]+`)

func schemaFor(t *testing.T) string {
	t.Helper()
	return "test_" + nonIdentifier.ReplaceAllString(strings.ToLower(t.Name()), "_")
}

func withSearchPath(t *testing.T, schemaName string) string {
	t.Helper()
	u, err := url.Parse(testDatabaseURL)
	if err != nil {
		t.Fatalf("parsing database URL: %v", err)
	}
	q := u.Query()
	q.Set("search_path", schemaName)
	u.RawQuery = q.Encode()
	return u.String()
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Postgres-backed store test in short mode")
	}

	schemaName := schemaFor(t)
	admin, err := sqlx.ConnectContext(t.Context(), "pgx", testDatabaseURL)
	if err != nil {
		t.Fatalf("connecting to shared Postgres: %v", err)
	}
	if _, err := admin.ExecContext(t.Context(), `CREATE SCHEMA `+schemaName); err != nil {
		admin.Close()
		t.Fatalf("creating schema %s: %v", schemaName, err)
	}

	s, err := Open(t.Context(), withSearchPath(t, schemaName))
	if err != nil {
		admin.Close()
		t.Fatalf("Open() returned error: %v", err)
	}

	writerCtx, stopWriter := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunWriter(writerCtx)
	}()
	t.Cleanup(func() {
		stopWriter()
		<-done
		if err := s.Close(); err != nil {
			t.Errorf("Close() returned error: %v", err)
		}
		if _, err := admin.ExecContext(context.Background(), `DROP SCHEMA `+schemaName+` CASCADE`); err != nil {
			t.Errorf("dropping schema %s: %v", schemaName, err)
		}
		admin.Close()
	})
	return s
}

func insertJobs(t *testing.T, s *Store, jobs ...Job) {
	t.Helper()
	for _, job := range jobs {
		if err := s.InsertJob(t.Context(), job.EventID, job.TaskName); err != nil {
			t.Fatalf("InsertJob(%+v) returned error: %v", job, err)
		}
	}
}

func TestInsertJobPersistsRowWithDatabaseTimestamp(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	before := time.Now().UTC().Add(-time.Second)
	insertJobs(t, s, Job{EventID: "evt-0007", TaskName: "ReconcileLedger"})

	var got struct {
		EventID   string    `db:"event_id"`
		TaskName  string    `db:"task_name"`
		CreatedAt time.Time `db:"created_at"`
	}
	if err := s.db.GetContext(t.Context(), &got, `SELECT event_id, task_name, created_at FROM transient_jobs`); err != nil {
		t.Fatalf("reading row: %v", err)
	}
	if got.EventID != "evt-0007" || got.TaskName != "ReconcileLedger" {
		t.Errorf("row = %+v, want evt-0007/ReconcileLedger", got)
	}
	if got.CreatedAt.Before(before) || got.CreatedAt.After(time.Now().UTC()) {
		t.Errorf("created_at = %v, want current database timestamp", got.CreatedAt)
	}
}

func TestInsertJobIsIdempotentPerEventID(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	insertJobs(t, s,
		Job{EventID: "evt-1", TaskName: "first"},
		Job{EventID: "evt-1", TaskName: "second"},
	)
	if got, err := s.CountJobs(t.Context()); err != nil || got != 1 {
		t.Fatalf("CountJobs() = %d, %v, want 1, nil", got, err)
	}
	var task string
	if err := s.db.GetContext(t.Context(), &task, `SELECT task_name FROM transient_jobs WHERE event_id = $1`, "evt-1"); err != nil {
		t.Fatalf("reading task: %v", err)
	}
	if task != "first" {
		t.Errorf("task_name = %q, want first delivery to win", task)
	}
}

func TestEventIDsListsEveryIDAscending(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	insertJobs(t, s,
		Job{EventID: "evt-3", TaskName: "c"},
		Job{EventID: "evt-1", TaskName: "a"},
		Job{EventID: "evt-2", TaskName: "b"},
	)
	got, err := s.EventIDs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"evt-1", "evt-2", "evt-3"}; !slices.Equal(got, want) {
		t.Errorf("EventIDs() = %v, want %v", got, want)
	}
}

func TestInsertJobBatchesConcurrentWriters(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	const total = 200
	errs := make(chan error, total)
	var wg sync.WaitGroup
	for i := range total {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.InsertJob(t.Context(), fmt.Sprintf("evt-%03d", i), "task")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got, err := s.CountJobs(t.Context()); err != nil || got != total {
		t.Fatalf("CountJobs() = %d, %v, want %d, nil", got, err, total)
	}
}

func TestInsertJobHonoursCancelledContext(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.InsertJob(ctx, "evt-1", "task"); !errors.Is(err, context.Canceled) {
		t.Errorf("InsertJob() = %v, want context.Canceled", err)
	}
}

func TestInsertJobHonoursDeadlineWhileWaitingForCommit(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	s.db.SetMaxOpenConns(1)
	tx, err := s.db.BeginTxx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := s.InsertJob(ctx, "evt-blocked", "task"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("InsertJob() = %v, want context.DeadlineExceeded", err)
	}
}

func TestOpenIsIdempotentForExistingSchema(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	second, err := Open(t.Context(), withSearchPath(t, schemaFor(t)))
	if err != nil {
		t.Fatalf("second Open() returned error: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Errorf("closing second store: %v", err)
	}
	insertJobs(t, s, Job{EventID: "evt-after-reopen", TaskName: "task"})
}

func TestSchemaRejectsInvalidRows(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	for _, values := range [][2]any{{nil, "task"}, {"evt-1", nil}} {
		if _, err := s.db.ExecContext(t.Context(),
			`INSERT INTO transient_jobs (event_id, task_name) VALUES ($1, $2)`, values[0], values[1]); err == nil {
			t.Errorf("inserted invalid values %v", values)
		}
	}
}
