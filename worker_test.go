package main

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jmoiron/sqlx"
)

// Unit tests for the router. No database is involved: the substitute handlers
// ignore the *sqlx.DB argument, so a nil pool is safe and the tests stay fast.

func TestHandleRoutesByDiscriminator(t *testing.T) {
	t.Parallel()

	var got []byte
	p := &Processor{log: discardLogger(), routes: map[string]Handler{}}
	p.Register("TransientJob", func(_ *sqlx.DB, raw []byte) error {
		got = raw
		return nil
	})

	payload := []byte(`{"eventType":"TransientJob","eventId":"evt-1"}`)
	if err := p.Handle(payload); err != nil {
		t.Fatalf("Handle returned %v, want nil", err)
	}
	if string(got) != string(payload) {
		t.Errorf("handler received %s, want the payload unmodified", got)
	}
}

func TestHandleRoutingFailures(t *testing.T) {
	t.Parallel()

	// Table-driven subtests are Go's answer to [Theory] with [InlineData].
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{"missing discriminator", `{"eventId":"evt-1"}`, ErrNoDiscriminator},
		{"empty object", `{}`, ErrNoDiscriminator},
		{"malformed json", `not json at all`, ErrNoDiscriminator},
		{"unregistered type", `{"eventType":"MysteryEvent"}`, UnknownEventError{EventType: "MysteryEvent"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := NewProcessor(nil, discardLogger())
			err := p.Handle([]byte(tt.payload))

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Handle(%s) = %v, want %v", tt.payload, err, tt.wantErr)
			}
		})
	}
}

func TestHandleSurfacesUnknownEventType(t *testing.T) {
	t.Parallel()

	p := NewProcessor(nil, discardLogger())
	err := p.Handle([]byte(`{"eventType":"MysteryEvent"}`))

	// errors.As recovers the typed error, so callers can act on the value
	// rather than matching on the message text.
	var unknown UnknownEventError
	if !errors.As(err, &unknown) {
		t.Fatalf("Handle returned %T, want UnknownEventError", err)
	}
	if unknown.EventType != "MysteryEvent" {
		t.Errorf("EventType = %q, want %q", unknown.EventType, "MysteryEvent")
	}
}

func TestHandlePropagatesHandlerError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("handler exploded")
	p := &Processor{log: discardLogger(), routes: map[string]Handler{
		"TransientJob": func(*sqlx.DB, []byte) error { return sentinel },
	}}

	if err := p.Handle([]byte(`{"eventType":"TransientJob"}`)); !errors.Is(err, sentinel) {
		t.Fatalf("Handle returned %v, want the handler's error", err)
	}
}

func TestRunDrainsQueueAcrossWorkers(t *testing.T) {
	t.Parallel()

	const messages = 500

	var handled atomic.Int64
	p := &Processor{log: discardLogger(), routes: map[string]Handler{
		"TransientJob": func(*sqlx.DB, []byte) error {
			handled.Add(1)
			return nil
		},
	}}

	queue := make(chan []byte, 8)
	go func() {
		defer close(queue)
		for range messages {
			queue <- []byte(`{"eventType":"TransientJob"}`)
		}
	}()

	p.Run(queue, 5)

	// Run returned, so every worker has exited: no sleeps, no polling.
	if n := handled.Load(); n != messages {
		t.Errorf("handled %d messages, want %d", n, messages)
	}
}

func TestRunKeepsWorkingAfterAHandlerFails(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var succeeded int

	p := &Processor{log: discardLogger(), routes: map[string]Handler{
		"Good": func(*sqlx.DB, []byte) error {
			mu.Lock()
			defer mu.Unlock()
			succeeded++
			return nil
		},
		"Bad": func(*sqlx.DB, []byte) error { return errors.New("nope") },
	}}

	queue := make(chan []byte, 6)
	for _, e := range []string{"Bad", "Good", "Unrouted", "Good", "Bad", "Good"} {
		queue <- []byte(`{"eventType":"` + e + `"}`)
	}
	close(queue)

	p.Run(queue, 3)

	if succeeded != 3 {
		t.Errorf("succeeded = %d, want 3: a failing message must not stop the pool", succeeded)
	}
}
