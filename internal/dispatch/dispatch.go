// Package dispatch routes raw queue messages to handlers via an explicit
// registry. It knows nothing about message schemas or databases: handlers
// capture their own dependencies, so this package stays reusable and its unit
// tests need no infrastructure.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

// Handler processes one raw message.
type Handler func(ctx context.Context, rawJSON []byte) error

// Delivery is one broker message and the acknowledgement operations that
// decide whether the broker removes, dead-letters, or redelivers it.
type Delivery interface {
	Body() []byte
	Ack() error
	Nack(requeue bool) error
}

// ErrNoDiscriminator reports a message that cannot be routed at all.
var ErrNoDiscriminator = errors.New("message carries no eventType discriminator")

// UnknownEventError reports a message whose event type has no registered
// handler. It is a distinct type so callers can recover the offending value
// with errors.As rather than parsing a message string.
type UnknownEventError struct {
	EventType string
}

func (e UnknownEventError) Error() string {
	return fmt.Sprintf("unrecognised event type %q", e.EventType)
}

// PermanentError marks a failure that retrying cannot fix, such as a malformed
// payload. Handlers wrap validation failures in it so the pool dead-letters
// them immediately instead of spending the retry budget on a certain failure.
type PermanentError struct {
	Err error
}

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the pool will not retry it.
func Permanent(err error) error { return PermanentError{Err: err} }

// Retryable reports whether the pool should attempt err again.
func Retryable(err error) bool {
	var permanent PermanentError
	var unknown UnknownEventError
	switch {
	case err == nil,
		errors.Is(err, ErrNoDiscriminator),
		errors.As(err, &unknown),
		errors.As(err, &permanent),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return false
	default:
		return true
	}
}

// Config tunes the worker pool. The zero value runs one worker with no retries.
type Config struct {
	// Workers is the number of goroutines draining the queue.
	Workers int
	// Retries is the number of *additional* attempts after the first failure.
	Retries int
	// Backoff is the delay before the second attempt; it doubles thereafter.
	Backoff time.Duration
}

// Router owns the routing registry and the worker pool.
type Router struct {
	log    *slog.Logger
	cfg    Config
	routes map[string]Handler
}

// New builds an empty router. Register the handlers before calling Run.
func New(log *slog.Logger, cfg Config) *Router {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	return &Router{log: log, cfg: cfg, routes: make(map[string]Handler)}
}

// Register adds or replaces the handler for an event type.
func (r *Router) Register(eventType string, h Handler) {
	r.routes[eventType] = h
}

// Handle routes exactly one message and returns the outcome. It performs no
// retries, so tests can assert on a single attempt.
//
// Only the discriminator is read: gjson walks the bytes in place, so a message
// whose handler discards it is never deserialised.
func (r *Router) Handle(ctx context.Context, rawJSON []byte) error {
	eventType := gjson.GetBytes(rawJSON, "eventType")
	if !eventType.Exists() {
		return ErrNoDiscriminator
	}

	handler, ok := r.routes[eventType.String()]
	if !ok {
		return UnknownEventError{EventType: eventType.String()}
	}
	return handler(ctx, rawJSON)
}

// Run launches the pool and blocks until the queue is closed and drained, or
// until ctx is cancelled. Cancellation abandons queued messages; closing the
// queue drains them.
func (r *Router) Run(ctx context.Context, queue <-chan Delivery) {
	var wg sync.WaitGroup

	for id := range r.cfg.Workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.work(ctx, id, queue)
		}(id + 1)
	}

	wg.Wait()
}

func (r *Router) work(ctx context.Context, id int, queue <-chan Delivery) {
	for {
		select {
		case <-ctx.Done():
			r.log.Debug("cancelled, worker shutting down", "worker", id)
			return
		case delivery, open := <-queue:
			if !open {
				r.log.Debug("queue closed, worker shutting down", "worker", id)
				return
			}
			r.deliver(ctx, id, delivery)
		}
	}
}

// deliver attempts one message, retrying transient failures with exponential
// backoff before dead-lettering it. A message interrupted by cancellation is
// abandoned rather than dead-lettered: it was never judged on its own merits.
func (r *Router) deliver(ctx context.Context, id int, delivery Delivery) {
	var err error
	payload := delivery.Body()

	if ctx.Err() != nil {
		r.nack(id, delivery, true, ctx.Err())
		return
	}

	for attempt := range r.cfg.Retries + 1 {
		if attempt > 0 {
			r.log.Warn("retrying message", "worker", id, "attempt", attempt+1, "error", err)
			if !sleep(ctx, r.cfg.Backoff<<(attempt-1)) {
				r.nack(id, delivery, true, ctx.Err())
				return
			}
		}

		if err = r.Handle(ctx, payload); err == nil {
			// Job handlers return only after the store has committed, so this is
			// deliberately an acknowledgement-after-commit boundary.
			if ackErr := delivery.Ack(); ackErr != nil {
				r.log.Error("acknowledging message", "worker", id, "error", ackErr)
			}
			return
		}
		if ctx.Err() != nil {
			r.nack(id, delivery, true, err)
			return
		}
		if !Retryable(err) {
			break
		}
	}

	r.log.Error("message dead-lettered", "worker", id, "error", err)
	r.nack(id, delivery, false, err)
}

func (r *Router) nack(id int, delivery Delivery, requeue bool, cause error) {
	if err := delivery.Nack(requeue); err != nil {
		r.log.Error("negative-acknowledging message", "worker", id, "requeue", requeue,
			"cause", cause, "error", err)
	}
}

// sleep waits for d, reporting false if ctx was cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
