package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// End-to-end test: the real producer, the real worker pool and a real SQLite
// file on disk, wired exactly as main does it.
//
// `go test -short` skips it, which is how Go marks the slow tier — the
// equivalent of an xUnit trait you exclude from the inner loop.
func TestPipelineEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end test skipped in short mode")
	}
	t.Parallel()

	const (
		seed     = 2026
		messages = 120
	)

	path := filepath.Join(t.TempDir(), "e2e.db")
	db, err := initialiseDatabase(path)
	if err != nil {
		t.Fatalf("initialising database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	queue := make(chan []byte, 16)
	go NewProducer(seed, 0).Run(queue, messages)

	done := make(chan struct{})
	go func() {
		defer close(done)
		NewProcessor(db, discardLogger()).Run(queue, 5)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("pipeline did not shut down: a worker is stuck or the queue was never closed")
	}

	// The same seed replays the same stream, so the expected row count is
	// exact rather than approximate.
	wantJobs, wantIDs := expectedJobs(t, seed, messages)

	if got := countJobs(t, db); got != wantJobs {
		t.Fatalf("persisted %d jobs, want %d", got, wantJobs)
	}

	var gotIDs []string
	if err := db.Select(&gotIDs, "SELECT event_id FROM transient_jobs ORDER BY event_id"); err != nil {
		t.Fatalf("reading event ids: %v", err)
	}
	for i, want := range wantIDs {
		if gotIDs[i] != want {
			t.Errorf("event_id[%d] = %q, want %q", i, gotIDs[i], want)
		}
	}
}

// expectedJobs replays the producer stream for a seed and reports how many job
// events it contains, plus their event ids in the order SQL returns them.
func expectedJobs(t *testing.T, seed uint64, messages int) (int, []string) {
	t.Helper()

	var ids []string
	for _, msg := range collect(t, NewProducer(seed, 0), messages) {
		if gjson.GetBytes(msg, "eventType").String() == eventJob {
			ids = append(ids, gjson.GetBytes(msg, "eventId").String())
		}
	}

	// ORDER BY event_id, because workers race and insertion order is undefined.
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	return len(ids), ids
}
