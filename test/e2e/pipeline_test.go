// Package e2e exercises the whole processor the way cmd/processor/main.go
// wires it: the real producer, the real router, the real handlers and a real
// SQLite file. The unit tests beside each package prove the parts in
// isolation; these tests prove they still fit together, so they own the
// assertions no single package can make — exact persistence and clean
// shutdown.
//
// The directory holds nothing but this _test.go file, which is legal: the
// package exists only for the test binary.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonny/go-demo/internal/dispatch"
	"github.com/jonny/go-demo/internal/jobs"
	"github.com/jonny/go-demo/internal/queue"
	"github.com/jonny/go-demo/internal/store"
	"github.com/tidwall/gjson"
)

const (
	workerCount    = 5
	queueDepth     = 16
	retries        = 2
	retryBackoff   = 5 * time.Millisecond
	messageSpacing = 2 * time.Millisecond

	// pipelineSeed pins the producer's PCG source. Every job id below is a
	// consequence of this number, so changing it changes the fixture.
	pipelineSeed = uint64(0x5eed_1234_abcd_ef01)
	messageCount = 40

	// shutdownBudget is deliberately far longer than the work takes. It is a
	// deadlock detector, not a performance assertion: without it a stage that
	// never returns would hang the whole suite until the package timeout.
	shutdownBudget = 30 * time.Second

	// cancelBudget is tight, because "cancellation is honoured promptly" is
	// the actual claim under test.
	cancelBudget = 5 * time.Second
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// harness owns the long-lived stages of the pipeline — the database, its
// writer goroutine, the dead-letter reaper and the router — assembled exactly
// as main.go assembles them.
type harness struct {
	store       *store.Store
	router      *dispatch.Router
	deadLetters chan []byte

	stopWriter context.CancelFunc
	writer     sync.WaitGroup
	reaper     sync.WaitGroup

	// reaped is written only by the reaper goroutine and read only after
	// reaper.Wait, so the WaitGroup supplies the happens-before edge.
	reaped [][]byte
}

// newHarness starts every stage that must already be running before the first
// message is routed. The queue itself is left to the caller: one test drives it
// from the producer, another feeds it by hand.
func newHarness(t *testing.T) *harness {
	t.Helper()

	st, err := store.Open(filepath.Join(t.TempDir(), "pipeline.db"))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	// Backstop for the failure paths below, where shutdown is never reached.
	t.Cleanup(func() { st.Close() })

	h := &harness{
		store:       st,
		deadLetters: make(chan []byte, queueDepth),
	}

	// The writer outlives the run context on purpose: a handler interrupted
	// mid-insert must still receive an acknowledgement rather than block.
	writerCtx, stopWriter := context.WithCancel(context.Background())
	h.stopWriter = stopWriter
	h.writer.Add(1)
	go func() {
		defer h.writer.Done()
		st.RunWriter(writerCtx)
	}()

	h.reaper.Add(1)
	go func() {
		defer h.reaper.Done()
		for msg := range h.deadLetters {
			h.reaped = append(h.reaped, msg)
		}
	}()

	log := discardLogger()
	h.router = dispatch.New(log, dispatch.Config{
		Workers:    workerCount,
		Retries:    retries,
		Backoff:    retryBackoff,
		DeadLetter: h.deadLetters,
	})
	h.router.Register(jobs.EventHeartbeat, jobs.NewHeartbeatHandler(log))
	h.router.Register(jobs.EventJob, jobs.NewJobHandler(log, st))

	return h
}

// shutdown tears the stages down in dependency order, and must be called only
// once router.Run has returned. Nothing can dead-letter once the workers are
// done, and nothing can insert once the dead-letter reaper has finished, so
// closing in this order is what makes the shutdown race-free.
func (h *harness) shutdown() {
	close(h.deadLetters)
	h.reaper.Wait()
	h.stopWriter()
	h.writer.Wait()
}

// runToCompletion drives the router and the teardown on a background
// goroutine, failing the test rather than hanging if either never returns.
func (h *harness) runToCompletion(t *testing.T, ctx context.Context, messages <-chan []byte, budget time.Duration) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.router.Run(ctx, messages)
		h.shutdown()
	}()

	select {
	case <-done:
	case <-time.After(budget):
		t.Fatalf("pipeline did not shut down within %s", budget)
	}
}

// expectedJobIDs replays the producer with the same seed and derives the job
// ids the run must have persisted. Replaying beats hard-coding the ids: the
// fixture stays honest if the payload format changes, while still failing if
// the pipeline drops, duplicates or invents a message.
//
// Only the job envelopes carry an eventId; heartbeats are discarded by their
// handler, and their embedded timestamp would not replay identically anyway.
func expectedJobIDs(t *testing.T, seed uint64, count int) []string {
	t.Helper()

	// A buffer per message means Run never blocks, so it can be driven
	// synchronously with no spacing.
	replay := make(chan []byte, count)
	queue.NewProducer(seed, 0).Run(context.Background(), replay, count)

	var ids []string
	for msg := range replay {
		if gjson.GetBytes(msg, "eventType").String() != jobs.EventJob {
			continue
		}
		id := gjson.GetBytes(msg, "eventId").String()
		if id == "" {
			t.Fatalf("replayed job envelope carries no eventId: %s", msg)
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		t.Fatalf("seed %#x produced no job envelopes in %d messages; the fixture is useless", seed, count)
	}

	slices.Sort(ids)
	return ids
}

func payloads(msgs [][]byte) []string {
	out := make([]string, len(msgs))
	for i, msg := range msgs {
		out[i] = string(msg)
	}
	return out
}

// TestPipelinePersistsEveryJob is the headline end-to-end case: a healthy run
// persists exactly the jobs the producer emitted, nothing more and nothing
// less, and dead-letters none of them.
func TestPipelinePersistsEveryJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end pipeline: it drives a real SQLite file and sleeps between messages")
	}
	t.Parallel()

	h := newHarness(t)

	messages := make(chan []byte, queueDepth)
	go queue.NewProducer(pipelineSeed, messageSpacing).Run(context.Background(), messages, messageCount)

	h.runToCompletion(t, context.Background(), messages, shutdownBudget)

	want := expectedJobIDs(t, pipelineSeed, messageCount)
	got, err := h.store.EventIDs(context.Background())
	if err != nil {
		t.Fatalf("listing event ids: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Errorf("persisted event ids mismatch\n got %d: %v\nwant %d: %v", len(got), got, len(want), want)
	}

	if n := len(h.reaped); n != 0 {
		t.Errorf("healthy run dead-lettered %d messages: %v", n, payloads(h.reaped))
	}
}

// TestExhaustedRetriesDeadLetter proves the retry budget is spent and the
// message then leaves the pipeline intact, rather than being dropped or
// retried forever.
func TestExhaustedRetriesDeadLetter(t *testing.T) {
	t.Parallel()

	const failingEvent = "AlwaysFails"

	h := newHarness(t)

	var attempts atomic.Int64
	h.router.Register(failingEvent, func(context.Context, []byte) error {
		attempts.Add(1)
		// Unwrapped, so the router treats it as transient and retries.
		return errors.New("simulated downstream outage")
	})

	const failures = 3
	messages := make(chan []byte, failures)
	want := make([]string, 0, failures)
	for i := range failures {
		msg := fmt.Appendf(nil, `{"eventType":%q,"eventId":"fail-%02d"}`, failingEvent, i)
		want = append(want, string(msg))
		messages <- msg
	}
	close(messages)

	h.runToCompletion(t, context.Background(), messages, shutdownBudget)

	got := payloads(h.reaped)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("dead-lettered payloads = %v, want %v", got, want)
	}

	// Retries are additional attempts, so each message is tried retries+1 times
	// before it is given up on.
	if wantAttempts := int64(failures * (retries + 1)); attempts.Load() != wantAttempts {
		t.Errorf("handler attempts = %d, want %d", attempts.Load(), wantAttempts)
	}
}

// countingStore records the inserts the store acknowledged, so the cancellation
// test can compare the surviving rows against the work that actually completed.
type countingStore struct {
	inner *store.Store
	acked atomic.Int64
}

func (c *countingStore) InsertJob(ctx context.Context, job store.Job) error {
	if err := c.inner.InsertJob(ctx, job); err != nil {
		return err
	}
	c.acked.Add(1)
	return nil
}

// TestCancellationLeavesDatabaseConsistent cancels the run mid-flight and
// checks the two things that matter: everything stops quickly, and the
// database holds committed work only.
func TestCancellationLeavesDatabaseConsistent(t *testing.T) {
	t.Parallel()

	// Far more messages than the cancellation will allow through, so the run
	// is guaranteed to be interrupted rather than merely finishing early.
	const totalMessages = 500

	h := newHarness(t)

	counter := &countingStore{inner: h.store}
	// Register replaces the handler installed by the harness, which is the
	// cheapest way to observe the inserts without touching the store.
	h.router.Register(jobs.EventJob, jobs.NewJobHandler(discardLogger(), counter))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages := make(chan []byte, queueDepth)
	go queue.NewProducer(pipelineSeed, messageSpacing).Run(ctx, messages, totalMessages)

	// Long enough that real work has been committed, short enough that the
	// producer is still going.
	time.AfterFunc(25*time.Millisecond, cancel)

	start := time.Now()
	h.runToCompletion(t, ctx, messages, cancelBudget)
	if elapsed := time.Since(start); elapsed > cancelBudget {
		t.Errorf("shutdown took %s, want under %s", elapsed, cancelBudget)
	}

	// The run context is cancelled, so the queries need one that is not.
	query := context.WithoutCancel(ctx)
	rows, err := h.store.CountJobs(query)
	if err != nil {
		t.Fatalf("counting jobs after cancellation: %v", err)
	}

	acked := int(counter.acked.Load())
	// Rows may exceed acknowledged inserts by at most one per worker: when the
	// commit and the cancellation land together, InsertJob is free to report
	// ctx.Err() for a row that did in fact make it to disk. Anything beyond
	// that margin means rows appeared that no handler ever completed.
	if rows < acked || rows > acked+workerCount {
		t.Errorf("persisted %d rows, want between %d and %d (acknowledged inserts, plus at most one in flight per worker)",
			rows, acked, acked+workerCount)
	}
	if rows > totalMessages {
		t.Errorf("persisted %d rows from at most %d produced messages", rows, totalMessages)
	}

	ids, err := h.store.EventIDs(query)
	if err != nil {
		t.Fatalf("listing event ids after cancellation: %v", err)
	}
	// A torn shutdown would show up here as a partial or duplicated row.
	if len(ids) != rows {
		t.Errorf("counted %d rows but listed %d event ids", rows, len(ids))
	}
	for _, id := range ids {
		if id == "" {
			t.Errorf("persisted a row with an empty event id: %v", ids)
			break
		}
	}
}
