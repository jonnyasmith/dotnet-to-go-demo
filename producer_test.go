package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func TestProducerIsReproducibleForAGivenSeed(t *testing.T) {
	t.Parallel()

	first := collect(t, NewProducer(42, 0), 20)
	second := collect(t, NewProducer(42, 0), 20)

	if len(first) != len(second) {
		t.Fatalf("stream lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if string(first[i]) != string(second[i]) {
			t.Fatalf("message %d differs:\n  %s\n  %s", i, first[i], second[i])
		}
	}
}

func TestProducerEmitsBothEventTypes(t *testing.T) {
	t.Parallel()

	counts := map[string]int{}
	for _, msg := range collect(t, NewProducer(7, 0), 200) {
		counts[gjson.GetBytes(msg, "eventType").String()]++
	}

	if counts[eventHeartbeat] == 0 || counts[eventJob] == 0 {
		t.Fatalf("expected both event types over 200 messages, got %v", counts)
	}
	if total := counts[eventHeartbeat] + counts[eventJob]; total != 200 {
		t.Errorf("produced %d recognised messages, want 200 (extra types: %v)", total, counts)
	}
}

func TestProducerEmitsWellFormedEnvelopes(t *testing.T) {
	t.Parallel()

	for i, msg := range collect(t, NewProducer(1, 0), 100) {
		if !json.Valid(msg) {
			t.Fatalf("message %d is not valid JSON: %s", i, msg)
		}
		if gjson.GetBytes(msg, "eventType").String() != eventJob {
			continue
		}
		if gjson.GetBytes(msg, "eventId").String() == "" {
			t.Errorf("job message %d has an empty eventId: %s", i, msg)
		}
		if gjson.GetBytes(msg, "payload.taskName").String() == "" {
			t.Errorf("job message %d has an empty taskName: %s", i, msg)
		}
	}
}

func TestProducerClosesTheQueue(t *testing.T) {
	t.Parallel()

	queue := make(chan []byte, 4)
	NewProducer(3, 0).Run(queue, 4)

	// Draining a closed channel terminates; if Run forgot to close it, the
	// range below would block and the test would fail on the package timeout.
	var n int
	for range queue {
		n++
	}
	if n != 4 {
		t.Errorf("drained %d messages, want 4", n)
	}
}

func TestProducerHonoursInterval(t *testing.T) {
	t.Parallel()

	queue := make(chan []byte, 8)
	start := time.Now()
	NewProducer(5, 5*time.Millisecond).Run(queue, 4)

	// Four messages at 5ms spacing cannot complete instantly. Asserting a
	// lower bound keeps the test robust on a loaded machine.
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Errorf("produced 4 messages in %v, want at least 15ms of spacing", elapsed)
	}
}

// collect drains a producer's full output into a slice.
func collect(t *testing.T, p *Producer, count int) [][]byte {
	t.Helper()

	queue := make(chan []byte, count)
	p.Run(queue, count)

	msgs := make([][]byte, 0, count)
	for msg := range queue {
		msgs = append(msgs, msg)
	}
	return msgs
}
