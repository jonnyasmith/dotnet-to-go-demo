package jobs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jonny/go-demo/internal/dispatch"
)

// insertion is what the fake records. JobStore deals in primitives, so this
// test package needs no type from internal/store at all.
type insertion struct {
	eventID  string
	taskName string
}

// These are unit tests with no database. That is the whole reason JobStore is
// an interface declared in this package: the fake below is the entire test
// double, with no mocking framework behind it.
type fakeStore struct {
	mu       sync.Mutex
	inserted []insertion
	seenCtx  context.Context
	err      error
}

func (f *fakeStore) InsertJob(ctx context.Context, eventID, taskName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.seenCtx = ctx
	if f.err != nil {
		return f.err
	}
	f.inserted = append(f.inserted, insertion{eventID: eventID, taskName: taskName})
	return nil
}

func (f *fakeStore) insertions() []insertion {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inserted
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestJobHandlerMapsEnvelopeOntoJob(t *testing.T) {
	t.Parallel()

	fake := &fakeStore{}
	raw := []byte(`{"eventType":"TransientJob","eventId":"evt-0007-1a2b3c4d","payload":{"taskName":"ReconcileLedger"}}`)

	if err := NewJobHandler(discardLogger(), fake)(context.Background(), raw); err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}

	got := fake.insertions()
	if len(got) != 1 {
		t.Fatalf("store received %d jobs, want 1", len(got))
	}
	want := insertion{eventID: "evt-0007-1a2b3c4d", taskName: "ReconcileLedger"}
	if got[0] != want {
		t.Errorf("store received %+v, want %+v", got[0], want)
	}
}

func TestJobHandlerRejectsBadPayloadsPermanently(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantMsg string
	}{
		{"malformed json", `{"eventType":"TransientJob"`, "deserialising"},
		{"missing eventId", `{"eventType":"TransientJob","payload":{"taskName":"X"}}`, "incomplete"},
		{"missing taskName", `{"eventType":"TransientJob","eventId":"evt-1","payload":{}}`, "incomplete"},
		{"empty taskName", `{"eventType":"TransientJob","eventId":"evt-1","payload":{"taskName":""}}`, "incomplete"},
		{"empty payload object", `{"eventType":"TransientJob","eventId":"evt-1"}`, "incomplete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeStore{}
			err := NewJobHandler(discardLogger(), fake)(context.Background(), []byte(tt.raw))

			if err == nil {
				t.Fatalf("handler accepted %s, want an error", tt.raw)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantMsg)
			}
			// A bad payload is bad forever: retrying it would burn the budget
			// on a certain failure.
			if dispatch.Retryable(err) {
				t.Errorf("error %v is retryable, want permanent", err)
			}
			if n := len(fake.insertions()); n != 0 {
				t.Errorf("store received %d jobs, want 0", n)
			}
		})
	}
}

func TestJobHandlerWrapsValidationFailuresInPermanentError(t *testing.T) {
	t.Parallel()

	err := NewJobHandler(discardLogger(), &fakeStore{})(
		context.Background(), []byte(`{"eventType":"TransientJob","eventId":"evt-1"}`))

	var permanent dispatch.PermanentError
	if !errors.As(err, &permanent) {
		t.Fatalf("handler returned %T, want a dispatch.PermanentError", err)
	}
}

func TestJobHandlerLeavesStoreFailuresRetryable(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("database is locked")
	fake := &fakeStore{err: sentinel}
	raw := []byte(`{"eventType":"TransientJob","eventId":"evt-1","payload":{"taskName":"X"}}`)

	err := NewJobHandler(discardLogger(), fake)(context.Background(), raw)

	if !errors.Is(err, sentinel) {
		t.Fatalf("handler returned %v, want it to wrap the store error", err)
	}
	// A locked database may well succeed on the next attempt, so this one
	// must stay eligible for retry.
	if !dispatch.Retryable(err) {
		t.Error("store failure is not retryable, want retryable")
	}
}

func TestJobHandlerPassesItsContextToTheStore(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "carried")

	fake := &fakeStore{}
	raw := []byte(`{"eventType":"TransientJob","eventId":"evt-1","payload":{"taskName":"X"}}`)

	if err := NewJobHandler(discardLogger(), fake)(ctx, raw); err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}

	// Without propagation the store cannot honour cancellation or deadlines.
	if got := fake.seenCtx.Value(ctxKey{}); got != "carried" {
		t.Errorf("store saw context value %v, want %q", got, "carried")
	}
}

func TestHeartbeatHandlerLogsAndDiscards(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(buf, nil))

	// The handler takes no store, so the only observable behaviour is the log
	// line: there is no database call to assert the absence of.
	if err := NewHeartbeatHandler(log)(context.Background(), []byte(`{"eventType":"SystemHeartbeat"}`)); err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}

	if !strings.Contains(buf.String(), "heartbeat received and discarded") {
		t.Errorf("log output = %q, want the discard message", buf.String())
	}
}

func TestHandlersToleratePathologicalPayloads(t *testing.T) {
	t.Parallel()

	payloads := [][]byte{nil, {}, []byte(`null`), []byte(`[]`), []byte(`"a string"`)}

	for _, raw := range payloads {
		// Neither handler may panic: the router hands over whatever the queue
		// delivered, and a panic in a worker would take the pool down.
		if err := NewHeartbeatHandler(discardLogger())(context.Background(), raw); err != nil {
			t.Errorf("heartbeat handler returned %v for %q, want nil", err, raw)
		}
		if err := NewJobHandler(discardLogger(), &fakeStore{})(context.Background(), raw); err == nil {
			t.Errorf("job handler accepted %q, want an error", raw)
		}
	}
}
