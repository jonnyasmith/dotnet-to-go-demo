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
func runAsync(ctx context.Context, r *Router, queue <-chan Delivery) <-chan struct{} {
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

type recordingDelivery struct {
	payload []byte
	acked   bool
	nacked  bool
	requeue bool
}

func (d *recordingDelivery) Body() []byte { return d.payload }
func (d *recordingDelivery) Ack() error {
	d.acked = true
	return nil
}
func (d *recordingDelivery) Nack(requeue bool) error {
	d.nacked = true
	d.requeue = requeue
	return nil
}

func delivery(payload string) *recordingDelivery {
	return &recordingDelivery{payload: []byte(payload)}
}

func TestRunAcknowledgesOnlyAfterHandlerSucceeds(t *testing.T) {
	t.Parallel()

	msg := delivery(jobMsg)
	var ackedDuringHandler bool
	r := New(discardLogger(), Config{Workers: 1})
	r.Register("TransientJob", func(context.Context, []byte) error {
		ackedDuringHandler = msg.acked
		return nil
	})

	queue := make(chan Delivery, 1)
	queue <- msg
	close(queue)
	awaitRun(t, runAsync(context.Background(), r, queue), time.Second)

	if ackedDuringHandler {
		t.Error("delivery was acknowledged before its handler completed")
	}
	if !msg.acked || msg.nacked {
		t.Errorf("delivery outcome = acked %t, nacked %t; want acknowledged only", msg.acked, msg.nacked)
	}
}

func TestRunNacksPermanentFailureWithoutRequeue(t *testing.T) {
	t.Parallel()

	msg := delivery(jobMsg)
	r := New(discardLogger(), Config{Workers: 1, Retries: 3, Backoff: time.Minute})
	r.Register("TransientJob", func(context.Context, []byte) error { return Permanent(errHandler) })

	queue := make(chan Delivery, 1)
	queue <- msg
	close(queue)
	awaitRun(t, runAsync(context.Background(), r, queue), time.Second)

	if msg.acked || !msg.nacked || msg.requeue {
		t.Errorf("delivery outcome = acked %t, nacked %t, requeue %t; want nack without requeue", msg.acked, msg.nacked, msg.requeue)
	}
}

func TestRunNacksCancelledInFlightDeliveryWithRequeue(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	msg := delivery(jobMsg)
	entered := make(chan struct{})
	r := New(discardLogger(), Config{Workers: 1})
	r.Register("TransientJob", func(ctx context.Context, _ []byte) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})

	queue := make(chan Delivery, 1)
	queue <- msg
	done := runAsync(ctx, r, queue)
	<-entered
	cancel()
	awaitRun(t, done, time.Second)

	if msg.acked || !msg.nacked || !msg.requeue {
		t.Errorf("delivery outcome = acked %t, nacked %t, requeue %t; want nack with requeue", msg.acked, msg.nacked, msg.requeue)
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

	const (
		messages = 500
		workers  = 8
	)

	var (
		handled  atomic.Int64
		admitted atomic.Int64
		barrier  sync.WaitGroup
	)
	barrier.Add(workers)

	r := New(discardLogger(), Config{Workers: workers})
	r.Register("SystemHeartbeat", func(context.Context, []byte) error {
		handled.Add(1)
		// A worker parked here cannot pick up another message, so the first
		// `workers` arrivals are necessarily `workers` distinct goroutines.
		// That makes the barrier a proof of fan-out: a pool that ignored
		// cfg.Workers and started one goroutine could never release it, and
		// would fail on awaitRun's deadline rather than pass quietly.
		if admitted.Add(1) <= workers {
			barrier.Done()
			barrier.Wait()
		}
		return nil
	})

	queue := make(chan Delivery, messages)
	for range messages {
		queue <- delivery(heartbeatMsg)
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
	r := New(discardLogger(), Config{Workers: 1})
	r.Register("SystemHeartbeat", func(context.Context, []byte) error {
		handled.Add(1)
		return nil
	})
	r.Register("TransientJob", func(context.Context, []byte) error {
		return errHandler
	})

	// One worker, so the two poison messages sit strictly between the two
	// heartbeats: the second heartbeat only lands if the pool survived them.
	queue := make(chan Delivery, 4)
	queue <- delivery(heartbeatMsg)
	failed := delivery(jobMsg)
	queue <- failed
	unroutable := delivery(unroutableMsg)
	queue <- unroutable
	queue <- delivery(heartbeatMsg)
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 5*time.Second)

	if got := handled.Load(); got != 2 {
		t.Errorf("handled %d heartbeats, want 2", got)
	}
	if !failed.nacked || failed.requeue || !unroutable.nacked || unroutable.requeue {
		t.Error("failed deliveries were not nacked without requeue")
	}
}

func TestRunRetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	const backoff = 2 * time.Millisecond

	var attempts atomic.Int64
	r := New(discardLogger(), Config{Workers: 1, Retries: 2, Backoff: backoff})
	r.Register("TransientJob", func(context.Context, []byte) error {
		if attempts.Add(1) < 3 {
			return errHandler
		}
		return nil
	})

	msg := delivery(jobMsg)
	queue := make(chan Delivery, 1)
	queue <- msg
	close(queue)

	start := time.Now()
	awaitRun(t, runAsync(context.Background(), r, queue), 5*time.Second)
	elapsed := time.Since(start)

	if got := attempts.Load(); got != 3 {
		t.Errorf("handler ran %d times, want 3 (first attempt plus Retries=2)", got)
	}
	if !msg.acked || msg.nacked {
		t.Errorf("delivery outcome = acked %t, nacked %t; want acknowledged", msg.acked, msg.nacked)
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
	r := New(discardLogger(), Config{Workers: 1, Retries: 2, Backoff: time.Millisecond})
	r.Register("TransientJob", func(context.Context, []byte) error {
		attempts.Add(1)
		return errHandler
	})

	msg := delivery(jobMsg)
	queue := make(chan Delivery, 1)
	queue <- msg
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 5*time.Second)

	if got := attempts.Load(); got != 3 {
		t.Errorf("handler ran %d times, want 3 (first attempt plus Retries=2)", got)
	}
	if msg.acked || !msg.nacked || msg.requeue {
		t.Errorf("delivery outcome = acked %t, nacked %t, requeue %t; want nack without requeue", msg.acked, msg.nacked, msg.requeue)
	}
}

func TestRunDoesNotRetryPermanentErrors(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int64
	// A backoff far longer than the deadline below: any retry at all overruns
	// the guard, so "not retried" is asserted without the test sleeping.
	r := New(discardLogger(), Config{Workers: 1, Retries: 3, Backoff: time.Minute})
	r.Register("TransientJob", func(context.Context, []byte) error {
		attempts.Add(1)
		return Permanent(errHandler)
	})

	msg := delivery(jobMsg)
	queue := make(chan Delivery, 1)
	queue <- msg
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 2*time.Second)

	if got := attempts.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1: a permanent error is not retried", got)
	}
	if msg.acked || !msg.nacked || msg.requeue {
		t.Errorf("delivery outcome = acked %t, nacked %t, requeue %t; want nack without requeue", msg.acked, msg.nacked, msg.requeue)
	}
}

func TestRunDeadLettersUnroutableWithoutRetrying(t *testing.T) {
	t.Parallel()

	// Same trick as the permanent-error case: the minute-long backoff turns any
	// retry into a blown deadline.
	r := New(discardLogger(), Config{Workers: 1, Retries: 3, Backoff: time.Minute})
	r.Register("SystemHeartbeat", failIfCalled(t))

	msg := delivery(unroutableMsg)
	queue := make(chan Delivery, 1)
	queue <- msg
	close(queue)

	awaitRun(t, runAsync(context.Background(), r, queue), 2*time.Second)

	if msg.acked || !msg.nacked || msg.requeue {
		t.Errorf("delivery outcome = acked %t, nacked %t, requeue %t; want nack without requeue", msg.acked, msg.nacked, msg.requeue)
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

	queue := make(chan Delivery, backlog)
	for range backlog {
		queue <- delivery(heartbeatMsg)
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

func TestRunAbandonsInFlightMessagesWithoutDeadLettering(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entered := make(chan struct{})
	r := New(discardLogger(), Config{Workers: 1, Retries: 3, Backoff: time.Millisecond})
	r.Register("TransientJob", func(ctx context.Context, _ []byte) error {
		close(entered)
		<-ctx.Done()
		// Exactly what a store insert returns when shutdown interrupts it: the
		// message is healthy, the pipeline is simply stopping.
		return fmt.Errorf("persisting job: %w", ctx.Err())
	})

	msg := delivery(jobMsg)
	queue := make(chan Delivery, 1)
	queue <- msg
	close(queue)

	done := runAsync(ctx, r, queue)
	<-entered
	cancel()
	awaitRun(t, done, 5*time.Second)

	// Cancellation is not poison. Dead-lettering here would file a perfectly
	// good message as unprocessable and log an error for a clean shutdown.
	if !msg.nacked || !msg.requeue {
		t.Errorf("delivery outcome = nacked %t, requeue %t; want nack with requeue", msg.nacked, msg.requeue)
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
