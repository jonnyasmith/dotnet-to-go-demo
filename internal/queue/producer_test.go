package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jonny/go-demo/internal/jobs"
	"github.com/tidwall/gjson"
)

// eventValidators maps each recognised event type to the extra shape checks it
// owes. A message whose eventType is absent from this map is one the consumer
// side would have no handler for, so the lookup doubles as the "is this a type
// we recognise" assertion.
var eventValidators = map[string]func(t *testing.T, msg gjson.Result){
	jobs.EventHeartbeat: func(t *testing.T, msg gjson.Result) {
		t.Helper()

		ts := msg.Get("timestamp").String()
		if _, err := time.Parse(time.RFC3339, ts); err != nil {
			t.Errorf("timestamp %q is not RFC3339: %v", ts, err)
		}
	},
	jobs.EventJob: func(t *testing.T, msg gjson.Result) {
		t.Helper()

		if id := msg.Get("eventId").String(); id == "" {
			t.Error("job envelope has an empty eventId")
		}
		if name := msg.Get("payload.taskName").String(); name == "" {
			t.Error("job envelope has an empty payload.taskName")
		}
	},
}

// collect runs p to completion and returns every message it emitted. The
// producer's sleep is replaced so spacing costs nothing, and the channel is
// buffered to count so Run never blocks on the (single-goroutine) reader.
func collect(t *testing.T, p *Producer, count int) [][]byte {
	t.Helper()

	p.sleep = func(context.Context, time.Duration) bool { return true }

	out := make(chan []byte, max(count, 1))
	p.Run(t.Context(), out, count)

	return drain(t, out)
}

// drain reads out until it is closed. It fails the test rather than blocking
// forever if Run ever forgets to close the channel.
func drain(t *testing.T, out <-chan []byte) [][]byte {
	t.Helper()

	var got [][]byte
	for {
		select {
		case msg, ok := <-out:
			if !ok {
				return got
			}
			got = append(got, msg)
		case <-time.After(time.Second):
			t.Fatal("output channel was never closed")
			return got
		}
	}
}

// runAsync runs p in its own goroutine and fails if Run does not return
// promptly, so a producer that ignores cancellation reports a failure instead
// of wedging the test binary.
func runAsync(t *testing.T, ctx context.Context, p *Producer, out chan []byte, count int) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx, out, count)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return")
	}
}

func equalStreams(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}

	return true
}

func TestProducerSameSeedReplaysIdenticalStream(t *testing.T) {
	t.Parallel()

	const count = 64

	// Heartbeat payloads embed time.Now() at second precision, so two runs are
	// only byte-comparable while the wall clock second holds still. Retry the
	// rare straddle instead of weakening the comparison.
	for range 3 {
		second := time.Now().Unix()
		first := collect(t, NewProducer(42, time.Millisecond), count)
		repeat := collect(t, NewProducer(42, time.Millisecond), count)
		if time.Now().Unix() != second {
			continue
		}

		if len(first) != count || len(repeat) != count {
			t.Fatalf("emitted %d and %d messages, want %d each", len(first), len(repeat), count)
		}
		for i := range first {
			if !bytes.Equal(first[i], repeat[i]) {
				t.Errorf("message %d differs between runs:\n first: %s\nrepeat: %s", i, first[i], repeat[i])
			}
		}

		return
	}

	t.Fatal("the clock ticked over during every attempt, so no comparison was made")
}

func TestProducerDifferentSeedsDiverge(t *testing.T) {
	t.Parallel()

	const count = 64

	first := collect(t, NewProducer(1, time.Millisecond), count)
	second := collect(t, NewProducer(2, time.Millisecond), count)

	if equalStreams(first, second) {
		t.Error("seeds 1 and 2 produced identical streams, so the seed is being ignored")
	}
}

func TestProducerEmitsBothEventTypes(t *testing.T) {
	t.Parallel()

	const count = 200

	seen := make(map[string]int, len(eventValidators))
	for i, raw := range collect(t, NewProducer(7, time.Millisecond), count) {
		eventType := gjson.GetBytes(raw, "eventType").String()
		if _, ok := eventValidators[eventType]; !ok {
			t.Errorf("message %d has unrecognised eventType %q: %s", i, eventType, raw)
			continue
		}
		seen[eventType]++
	}

	for eventType := range eventValidators {
		if seen[eventType] == 0 {
			t.Errorf("no %s messages in a run of %d", eventType, count)
		}
	}
}

func TestProducerEmitsWellFormedMessages(t *testing.T) {
	t.Parallel()

	const count = 200

	for i, raw := range collect(t, NewProducer(7, time.Millisecond), count) {
		if !json.Valid(raw) {
			t.Errorf("message %d is not valid JSON: %s", i, raw)
			continue
		}

		msg := gjson.ParseBytes(raw)
		validate, ok := eventValidators[msg.Get("eventType").String()]
		if !ok {
			continue // already reported by TestProducerEmitsBothEventTypes
		}

		t.Run(fmt.Sprintf("message%03d", i), func(t *testing.T) {
			validate(t, msg)
		})
	}
}

func TestRunClosesOutputChannel(t *testing.T) {
	t.Parallel()

	const count = 8

	p := NewProducer(3, time.Millisecond)
	p.sleep = func(context.Context, time.Duration) bool { return true }

	out := make(chan []byte, count)
	p.Run(t.Context(), out, count)

	// A forgotten close would leave drain waiting on an empty channel.
	if got := len(drain(t, out)); got != count {
		t.Errorf("drained %d messages, want %d", got, count)
	}
}

func TestRunSleepsOncePerMessage(t *testing.T) {
	t.Parallel()

	const (
		count = 5
		// Deliberately long: the fake sleep must mean none of it is spent.
		interval = 250 * time.Millisecond
	)

	p := NewProducer(11, interval)
	var requested []time.Duration
	p.sleep = func(_ context.Context, d time.Duration) bool {
		requested = append(requested, d)
		return true
	}

	out := make(chan []byte, count)
	p.Run(t.Context(), out, count)
	drain(t, out)

	if len(requested) != count {
		t.Fatalf("slept %d times, want once per message (%d)", len(requested), count)
	}
	for i, d := range requested {
		if d != interval {
			t.Errorf("sleep %d requested %v, want the configured %v", i, d, interval)
		}
	}
}

func TestRunStopsEarlyWhenCancelled(t *testing.T) {
	t.Parallel()

	const count = 100

	t.Run("cancelled before Run", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		p := NewProducer(5, time.Millisecond)
		// Mirror sleepCtx's contract so the fake cannot mask the cancellation.
		p.sleep = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

		// Unbuffered with no reader until Run has returned: the send can never
		// proceed, so Run's select is forced onto its ctx.Done() branch. A
		// producer that only checks cancellation via sleep would block here.
		out := make(chan []byte)
		runAsync(t, ctx, p, out, count)

		if got := len(drain(t, out)); got != 0 {
			t.Errorf("emitted %d messages under a cancelled context, want none", got)
		}
	})

	t.Run("cancelled part way through", func(t *testing.T) {
		t.Parallel()

		const stopAfter = 3

		p := NewProducer(5, time.Millisecond)
		calls := 0
		p.sleep = func(context.Context, time.Duration) bool {
			calls++
			return calls < stopAfter
		}

		out := make(chan []byte, count)
		runAsync(t, t.Context(), p, out, count)

		if got := len(drain(t, out)); got != stopAfter {
			t.Errorf("emitted %d messages, want %d before the sleep reported cancellation", got, stopAfter)
		}
	})
}

func TestRunWithNonPositiveCount(t *testing.T) {
	t.Parallel()

	for _, count := range []int{0, -1} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			t.Parallel()

			if got := len(collect(t, NewProducer(9, time.Millisecond), count)); got != 0 {
				t.Errorf("emitted %d messages, want none", got)
			}
		})
	}
}

// The tests above swap the sleep field out, so the real implementation — the
// one production actually runs — needs covering directly.
func TestSleepCtx(t *testing.T) {
	t.Parallel()

	t.Run("waits and reports completion", func(t *testing.T) {
		t.Parallel()

		if !sleepCtx(context.Background(), time.Millisecond) {
			t.Error("sleepCtx reported cancellation on a live context")
		}
	})

	t.Run("non-positive duration does not wait", func(t *testing.T) {
		t.Parallel()

		start := time.Now()
		if !sleepCtx(context.Background(), 0) {
			t.Error("sleepCtx(0) reported cancellation on a live context")
		}
		if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
			t.Errorf("sleepCtx(0) took %v, want an immediate return", elapsed)
		}
	})

	t.Run("cancelled context reports false", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Both branches must observe cancellation: the timer path and the
		// short-circuit for a non-positive duration.
		if sleepCtx(ctx, time.Hour) {
			t.Error("sleepCtx ignored a cancelled context and waited")
		}
		if sleepCtx(ctx, 0) {
			t.Error("sleepCtx(0) ignored a cancelled context")
		}
	})
}
