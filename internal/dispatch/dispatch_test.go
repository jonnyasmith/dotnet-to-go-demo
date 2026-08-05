package dispatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Representative payloads from the producer. They are spelled out here instead
// of imported from internal/jobs so these tests stay free of the message
// schemas the router deliberately does not understand.
const (
	heartbeatMsg  = `{"eventType":"SystemHeartbeat","timestamp":"2026-08-05T09:41:00Z"}`
	jobMsg        = `{"eventType":"TransientJob","eventId":"evt-0007-1a2b3c4d","payload":{"taskName":"ReconcileLedger"}}`
	unroutableMsg = `{"eventType":"LedgerReconciled","eventId":"evt-0008-9f8e7d6c"}`
)

// errHandler stands in for a handler's own failure, which the router must
// surface unchanged rather than fold into a routing error.
var errHandler = errors.New("handler failed")

// discardLogger keeps the router's chatter out of the test output: it warns per
// retry and errors per dead letter, which would otherwise bury real failures.
func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// failIfCalled returns a handler that fails the test if the router ever routes
// to it, proving a rejected message never reaches user code.
func failIfCalled(t *testing.T) Handler {
	t.Helper()
	return func(_ context.Context, rawJSON []byte) error {
		t.Errorf("handler called with %q, want no delivery", rawJSON)
		return nil
	}
}

// runAsync starts the pool on its own goroutine so the caller can put a
// deadline on Run returning; a router that missed a closed queue or a cancelled
// context would otherwise hang until the whole package times out.
func runAsync(ctx context.Context, r *Router, queue <-chan []byte) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx, queue)
	}()
	return done
}

func awaitRun(t *testing.T, done <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(within):
		t.Fatalf("Run did not return within %s", within)
	}
}

func awaitDeadLetter(t *testing.T, dead <-chan []byte, within time.Duration) []byte {
	t.Helper()
	select {
	case payload := <-dead:
		return payload
	case <-time.After(within):
		t.Fatalf("no dead letter arrived within %s", within)
		return nil
	}
}

func TestHandleRoutesToRegisteredHandler(t *testing.T) {
	t.Parallel()

	var got []byte
	r := New(discardLogger(), Config{})
	r.Register("TransientJob", failIfCalled(t))
	r.Register("SystemHeartbeat", func(_ context.Context, rawJSON []byte) error {
		got = rawJSON
		return nil
	})

	if err := r.Handle(context.Background(), []byte(heartbeatMsg)); err != nil {
		t.Fatalf("Handle() = %v, want nil", err)
	}
	if !bytes.Equal(got, []byte(heartbeatMsg)) {
		t.Errorf("handler received %q, want %q", got, heartbeatMsg)
	}
}

func TestHandleRoutingFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		want    error
	}{
		{"missing discriminator", `{"eventId":"evt-0009-0b1c2d3e"}`, ErrNoDiscriminator},
		{"empty object", `{}`, ErrNoDiscriminator},
		{"malformed json", `not json at all`, ErrNoDiscriminator},
		{"empty payload", ``, ErrNoDiscriminator},
		{"unregistered event type", unroutableMsg, UnknownEventError{EventType: "LedgerReconciled"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := New(discardLogger(), Config{})
			r.Register("SystemHeartbeat", failIfCalled(t))

			if err := r.Handle(context.Background(), []byte(tt.payload)); !errors.Is(err, tt.want) {
				t.Errorf("Handle(%q) = %v, want %v", tt.payload, err, tt.want)
			}
		})
	}
}

func TestHandleUnknownEventCarriesEventType(t *testing.T) {
	t.Parallel()

	r := New(discardLogger(), Config{})

	err := r.Handle(context.Background(), []byte(unroutableMsg))

	// errors.As is the reason UnknownEventError is a type rather than a
	// formatted sentinel: callers recover the value without parsing text.
	var unknown UnknownEventError
	if !errors.As(err, &unknown) {
		t.Fatalf("Handle() = %v, want an UnknownEventError", err)
	}
	if unknown.EventType != "LedgerReconciled" {
		t.Errorf("EventType = %q, want %q", unknown.EventType, "LedgerReconciled")
	}
}

func TestHandlePropagatesHandlerError(t *testing.T) {
	t.Parallel()

	r := New(discardLogger(), Config{})
	r.Register("TransientJob", func(context.Context, []byte) error {
		return fmt.Errorf("insert job: %w", errHandler)
	})

	err := r.Handle(context.Background(), []byte(jobMsg))

	if !errors.Is(err, errHandler) {
		t.Errorf("Handle() = %v, want it to wrap %v", err, errHandler)
	}
}

func TestRegisterReplacesRoute(t *testing.T) {
	t.Parallel()

	var first, second int
	r := New(discardLogger(), Config{})
	r.Register("SystemHeartbeat", func(context.Context, []byte) error {
		first++
		return nil
	})
	r.Register("SystemHeartbeat", func(context.Context, []byte) error {
		second++
		return nil
	})

	if err := r.Handle(context.Background(), []byte(heartbeatMsg)); err != nil {
		t.Fatalf("Handle() = %v, want nil", err)
	}
	if first != 0 {
		t.Errorf("replaced handler ran %d times, want 0", first)
	}
	if second != 1 {
		t.Errorf("replacement handler ran %d times, want 1", second)
	}
}

func TestRunDrainsQueueAcrossWorkers(t *testing.T) {
	t.Parallel()

	const messages = 500

	var handled atomic.Int64
	r := New(discardLogger(), Config{Workers: 8})
	r.Register("SystemHeartbeat", func(context.Context, []byte) error {
		handled.Add(1)
		return nil
	})

	queue := make(chan []byte, messages)
	for range messages {
		queue <- []byte(heartbeatMsg)
	}
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 5*time.Second)

	// Run has returned, so every worker has finished: the count needs no
	// synchronisation beyond the atomic itself, and no sleep to settle.
	if got := handled.Load(); got != messages {
		t.Errorf("handled %d messages, want %d", got, messages)
	}
}

func TestRunContinuesAfterFailures(t *testing.T) {
	t.Parallel()

	var handled atomic.Int64
	dead := make(chan []byte, 4)
	r := New(discardLogger(), Config{Workers: 1, DeadLetter: dead})
	r.Register("SystemHeartbeat", func(context.Context, []byte) error {
		handled.Add(1)
		return nil
	})
	r.Register("TransientJob", func(context.Context, []byte) error {
		return errHandler
	})

	// One worker, so the two poison messages sit strictly between the two
	// heartbeats: the second heartbeat only lands if the pool survived them.
	queue := make(chan []byte, 4)
	queue <- []byte(heartbeatMsg)
	queue <- []byte(jobMsg)
	queue <- []byte(unroutableMsg)
	queue <- []byte(heartbeatMsg)
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 5*time.Second)

	if got := handled.Load(); got != 2 {
		t.Errorf("handled %d heartbeats, want 2", got)
	}
	if got := len(dead); got != 2 {
		t.Errorf("dead-lettered %d messages, want 2", got)
	}
}

func TestRunRetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	const backoff = 2 * time.Millisecond

	var attempts atomic.Int64
	dead := make(chan []byte, 1)
	r := New(discardLogger(), Config{Workers: 1, Retries: 2, Backoff: backoff, DeadLetter: dead})
	r.Register("TransientJob", func(context.Context, []byte) error {
		if attempts.Add(1) < 3 {
			return errHandler
		}
		return nil
	})

	queue := make(chan []byte, 1)
	queue <- []byte(jobMsg)
	close(queue)

	start := time.Now()
	awaitRun(t, runAsync(context.Background(), r, queue), 5*time.Second)
	elapsed := time.Since(start)

	if got := attempts.Load(); got != 3 {
		t.Errorf("handler ran %d times, want 3 (first attempt plus Retries=2)", got)
	}
	if got := len(dead); got != 0 {
		t.Errorf("dead-lettered %d messages, want 0 once the message eventually succeeded", got)
	}
	// The delay doubles, so the two retries wait backoff + 2*backoff. A
	// constant backoff would come in under this floor; timers never fire early,
	// so the bound cannot flake in the other direction.
	if want := 3 * backoff; elapsed < want {
		t.Errorf("run took %s, want at least %s from a doubling backoff", elapsed, want)
	}
}

func TestRunDeadLettersAfterRetryExhaustion(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int64
	dead := make(chan []byte, 4)
	r := New(discardLogger(), Config{Workers: 1, Retries: 2, Backoff: time.Millisecond, DeadLetter: dead})
	r.Register("TransientJob", func(context.Context, []byte) error {
		attempts.Add(1)
		return errHandler
	})

	queue := make(chan []byte, 1)
	queue <- []byte(jobMsg)
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 5*time.Second)

	if got := attempts.Load(); got != 3 {
		t.Errorf("handler ran %d times, want 3 (first attempt plus Retries=2)", got)
	}
	if got := awaitDeadLetter(t, dead, time.Second); !bytes.Equal(got, []byte(jobMsg)) {
		t.Errorf("dead letter = %q, want %q", got, jobMsg)
	}
	if extra := len(dead); extra != 0 {
		t.Errorf("%d further dead letters queued, want the payload sent exactly once", extra)
	}
}

func TestRunDoesNotRetryPermanentErrors(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int64
	dead := make(chan []byte, 2)
	// A backoff far longer than the deadline below: any retry at all overruns
	// the guard, so "not retried" is asserted without the test sleeping.
	r := New(discardLogger(), Config{Workers: 1, Retries: 3, Backoff: time.Minute, DeadLetter: dead})
	r.Register("TransientJob", func(context.Context, []byte) error {
		attempts.Add(1)
		return Permanent(errHandler)
	})

	queue := make(chan []byte, 1)
	queue <- []byte(jobMsg)
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 2*time.Second)

	if got := attempts.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1: a permanent error is not retried", got)
	}
	if got := awaitDeadLetter(t, dead, time.Second); !bytes.Equal(got, []byte(jobMsg)) {
		t.Errorf("dead letter = %q, want %q", got, jobMsg)
	}
	if extra := len(dead); extra != 0 {
		t.Errorf("%d further dead letters queued, want the payload sent exactly once", extra)
	}
}

func TestRunDeadLettersUnroutableWithoutRetrying(t *testing.T) {
	t.Parallel()

	dead := make(chan []byte, 2)
	// Same trick as the permanent-error case: the minute-long backoff turns any
	// retry into a blown deadline.
	r := New(discardLogger(), Config{Workers: 1, Retries: 3, Backoff: time.Minute, DeadLetter: dead})
	r.Register("SystemHeartbeat", failIfCalled(t))

	queue := make(chan []byte, 1)
	queue <- []byte(unroutableMsg)
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 2*time.Second)

	if got := awaitDeadLetter(t, dead, time.Second); !bytes.Equal(got, []byte(unroutableMsg)) {
		t.Errorf("dead letter = %q, want %q", got, unroutableMsg)
	}
	if extra := len(dead); extra != 0 {
		t.Errorf("%d further dead letters queued, want the payload sent exactly once", extra)
	}
}

func TestRunStopsOnCancellation(t *testing.T) {
	t.Parallel()

	const backlog = 256

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		once      sync.Once
		firstCall = make(chan struct{})
		release   = make(chan struct{})
		handled   atomic.Int64
	)

	r := New(discardLogger(), Config{Workers: 2})
	r.Register("SystemHeartbeat", func(context.Context, []byte) error {
		handled.Add(1)
		once.Do(func() { close(firstCall) })
		// Pin the workers inside a handler so the backlog is provably still
		// queued at the moment the context is cancelled.
		<-release
		return nil
	})

	queue := make(chan []byte, backlog)
	for range backlog {
		queue <- []byte(heartbeatMsg)
	}
	// The queue is deliberately never closed: only cancellation can end this Run.
	done := runAsync(ctx, r, queue)

	<-firstCall
	cancel()
	close(release)

	awaitRun(t, done, 5*time.Second)

	// Each loop of a worker picks randomly between the ready cancellation and
	// the ready queue, so draining the whole backlog after cancelling has
	// probability 2^-backlog: this asserts abandonment without a race.
	if got := handled.Load(); got >= backlog {
		t.Errorf("handled %d of %d messages, want the backlog abandoned on cancel", got, backlog)
	}
}

func TestRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", errHandler, true},
		{"wrapped plain error", fmt.Errorf("deliver: %w", errHandler), true},
		{"permanent", Permanent(errHandler), false},
		{"wrapped permanent", fmt.Errorf("handle: %w", Permanent(errHandler)), false},
		{"unknown event", UnknownEventError{EventType: "LedgerReconciled"}, false},
		{"wrapped unknown event", fmt.Errorf("route: %w", UnknownEventError{EventType: "LedgerReconciled"}), false},
		{"no discriminator", ErrNoDiscriminator, false},
		{"context cancelled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := Retryable(tt.err); got != tt.want {
				t.Errorf("Retryable(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

// FuzzHandle drives arbitrary bytes through the discriminator lookup, where the
// router hands untrusted input to gjson. The router is shared across iterations
// because Handle only reads the registry, which keeps each case cheap enough
// for the seed corpus to run on every `go test`.
func FuzzHandle(f *testing.F) {
	f.Add([]byte(heartbeatMsg))
	f.Add([]byte(jobMsg))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json at all`))

	r := New(discardLogger(), Config{})
	r.Register("SystemHeartbeat", func(context.Context, []byte) error { return nil })
	r.Register("TransientJob", func(context.Context, []byte) error { return errHandler })

	ctx := context.Background()

	f.Fuzz(func(t *testing.T, rawJSON []byte) {
		// Reaching the assertion at all is half the point: a panic in routing
		// would take down every worker in the pool.
		err := r.Handle(ctx, rawJSON)

		var unknown UnknownEventError
		switch {
		case err == nil,
			errors.Is(err, ErrNoDiscriminator),
			errors.Is(err, errHandler),
			errors.As(err, &unknown):
		default:
			t.Errorf("Handle(%q) = %v, want nil or a known routing error", rawJSON, err)
		}
	})
}
