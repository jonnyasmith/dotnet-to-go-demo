package main

import (
	"strings"
	"testing"
)

// Integration tests: these run against a real SQLite database created per
// test, so they exercise the SQL, the named parameters, and the schema.

func TestJobHandlerPersistsTheJob(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	handler := newJobHandler(discardLogger())

	raw := []byte(`{"eventType":"TransientJob","eventId":"evt-0001","payload":{"taskName":"ProcessNightshadeData"}}`)
	if err := handler(db, raw); err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}

	var row struct {
		EventID   string `db:"event_id"`
		TaskName  string `db:"task_name"`
		CreatedAt string `db:"created_at"`
	}
	if err := db.Get(&row, "SELECT event_id, task_name, created_at FROM transient_jobs"); err != nil {
		t.Fatalf("reading back the job: %v", err)
	}

	if row.EventID != "evt-0001" {
		t.Errorf("event_id = %q, want %q", row.EventID, "evt-0001")
	}
	if row.TaskName != "ProcessNightshadeData" {
		t.Errorf("task_name = %q, want %q", row.TaskName, "ProcessNightshadeData")
	}
	if row.CreatedAt == "" {
		t.Error("created_at was not populated")
	}
}

func TestJobHandlerRejectsBadPayloads(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newTestDB(t)
			err := newJobHandler(discardLogger())(db, []byte(tt.raw))

			if err == nil {
				t.Fatalf("handler accepted %s, want an error", tt.raw)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantMsg)
			}
			if n := countJobs(t, db); n != 0 {
				t.Errorf("%d rows written, want 0: a rejected payload must not reach the table", n)
			}
		})
	}
}

func TestJobHandlerIsSafeUnderConcurrentWriters(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	p := NewProcessor(db, discardLogger())

	const messages = 100
	queue := make(chan []byte, 16)
	go func() {
		defer close(queue)
		for i := range messages {
			queue <- []byte(`{"eventType":"TransientJob","eventId":"evt-` +
				string(rune('a'+i%26)) + string(rune('a'+i/26)) +
				`","payload":{"taskName":"ReconcileLedger"}}`)
		}
	}()

	p.Run(queue, 5)

	// SQLite allows a single writer; this fails with "database is locked" if
	// the pool limit or busy timeout in initialiseDatabase is removed.
	if n := countJobs(t, db); n != messages {
		t.Errorf("persisted %d jobs, want %d", n, messages)
	}
}

func TestHeartbeatHandlerLogsAndDiscards(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	log, buf := captureLogger()

	if err := newHeartbeatHandler(log)(db, []byte(`{"eventType":"SystemHeartbeat"}`)); err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}

	if n := countJobs(t, db); n != 0 {
		t.Errorf("%d rows written, want 0: heartbeats must not touch the database", n)
	}
	if !strings.Contains(buf.String(), "heartbeat received and discarded") {
		t.Errorf("log output = %q, want the discard message", buf.String())
	}
}
