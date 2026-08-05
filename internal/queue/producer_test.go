package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jonny/go-demo/internal/jobs"
	"github.com/tidwall/gjson"
)

func testProducer(seed uint64) *Producer {
	p := NewProducer("amqp://unused", DefaultTopology, seed, 0)
	p.now = func() time.Time { return time.Date(2026, 8, 5, 9, 41, 0, 0, time.UTC) }
	return p
}

func payloads(p *Producer, count int) [][]byte {
	result := make([][]byte, count)
	for i := range count {
		result[i] = p.payload(i)
	}
	return result
}

func TestPayloadsReplayForSameSeed(t *testing.T) {
	t.Parallel()
	first, second := payloads(testProducer(42), 64), payloads(testProducer(42), 64)
	for i := range first {
		if !bytes.Equal(first[i], second[i]) {
			t.Errorf("payload %d differs: %s != %s", i, first[i], second[i])
		}
	}
}

func TestPayloadsAreWellFormedRecognisedEvents(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for i, raw := range payloads(testProducer(7), 200) {
		if !json.Valid(raw) {
			t.Fatalf("payload %d is invalid JSON: %s", i, raw)
		}
		eventType := gjson.GetBytes(raw, "eventType").String()
		seen[eventType] = true
		switch eventType {
		case jobs.EventHeartbeat:
			if _, err := time.Parse(time.RFC3339, gjson.GetBytes(raw, "timestamp").String()); err != nil {
				t.Errorf("payload %d has invalid timestamp: %v", i, err)
			}
		case jobs.EventJob:
			if gjson.GetBytes(raw, "eventId").String() == "" || gjson.GetBytes(raw, "payload.taskName").String() == "" {
				t.Errorf("payload %d has incomplete job fields: %s", i, raw)
			}
		default:
			t.Errorf("payload %d has unknown eventType %q", i, eventType)
		}
	}
	if !seen[jobs.EventHeartbeat] || !seen[jobs.EventJob] {
		t.Errorf("event types seen = %v, want heartbeat and job", seen)
	}
}

func TestSleepCtxStopsOnCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepCtx(ctx, time.Hour) || sleepCtx(ctx, 0) {
		t.Error("sleepCtx ignored cancellation")
	}
}
