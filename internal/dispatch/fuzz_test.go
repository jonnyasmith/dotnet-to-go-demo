// This file is package dispatch_test — an external test package — rather than
// package dispatch like the rest of the tests here. That is what lets it import
// internal/jobs: jobs imports dispatch, so an in-package test file that reached
// for the real handlers would close an import cycle. The external test package
// is compiled separately, so the cycle never forms.
package dispatch_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/jonny/go-demo/internal/dispatch"
	"github.com/jonny/go-demo/internal/jobs"
)

// fuzzStore satisfies jobs.JobStore. It never fails, so every error the target
// observes came from parsing or validating the input rather than from storage.
type fuzzStore struct {
	mu       sync.Mutex
	eventID  string
	taskName string
	calls    int
}

func (f *fuzzStore) InsertJob(_ context.Context, eventID, taskName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	f.eventID, f.taskName = eventID, taskName
	return nil
}

// FuzzHandle drives arbitrary bytes through the whole routing path with the
// real handlers behind it, so the fuzzer reaches json.Unmarshal and the
// envelope validation rather than stopping at the gjson discriminator lookup.
//
// Without -fuzz the target still runs over its seed corpus on every `go test`,
// which is what turns any crasher written to testdata/fuzz into a permanent
// regression test.
func FuzzHandle(f *testing.F) {
	f.Add([]byte(`{"eventType":"SystemHeartbeat","timestamp":"2026-08-05T09:41:00Z"}`))
	f.Add([]byte(`{"eventType":"TransientJob","eventId":"evt-0007-1a2b3c4d","payload":{"taskName":"ReconcileLedger"}}`))
	f.Add([]byte(`{"eventType":"TransientJob","eventId":"evt-1","payload":{}}`))
	f.Add([]byte(`{"eventType":"TransientJob"`))
	f.Add([]byte(`{"eventType":"LedgerReconciled"}`))
	f.Add([]byte(`{"eventType":"TransientJob","eventId":"","payload":{"taskName":"\u0000"}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json at all`))

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	f.Fuzz(func(t *testing.T, rawJSON []byte) {
		// A fresh store per iteration: the assertions below are about what this
		// one input persisted, not about an accumulated total.
		fake := &fuzzStore{}
		r := dispatch.New(log, dispatch.Config{})
		r.Register(jobs.EventHeartbeat, jobs.NewHeartbeatHandler(log))
		r.Register(jobs.EventJob, jobs.NewJobHandler(log, fake))

		// Reaching the assertions at all is half the point: a panic in parsing
		// would take down every worker in the pool.
		err := r.Handle(ctx, rawJSON)

		if err != nil && fake.calls != 0 {
			t.Errorf("Handle(%q) = %v but still persisted %d jobs", rawJSON, err, fake.calls)
		}

		// The store is the last line of defence for the UNIQUE constraint, so
		// no input may reach it with a key the schema cannot use.
		if fake.calls > 0 && (fake.eventID == "" || fake.taskName == "") {
			t.Errorf("Handle(%q) persisted eventId=%q taskName=%q, want both non-empty",
				rawJSON, fake.eventID, fake.taskName)
		}

		// The fake never fails, so every reachable error is a parsing, routing
		// or validation failure — and none of those improve on a second
		// attempt. A retryable one here would burn the pool's budget on a
		// certain failure.
		if dispatch.Retryable(err) {
			t.Errorf("Handle(%q) = %v, want a permanent error for malformed input", rawJSON, err)
		}

		var unknown dispatch.UnknownEventError
		var permanent dispatch.PermanentError
		switch {
		case err == nil,
			errors.Is(err, dispatch.ErrNoDiscriminator),
			errors.As(err, &unknown),
			errors.As(err, &permanent):
		default:
			t.Errorf("Handle(%q) = %v, want nil or a known routing error", rawJSON, err)
		}
	})
}
