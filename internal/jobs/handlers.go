// Package jobs holds the message schemas and their handlers.
package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jonny/go-demo/internal/dispatch"
)

// Event type discriminators.
const (
	EventHeartbeat = "SystemHeartbeat"
	EventJob       = "TransientJob"
)

// JobStore is the slice of the database this package actually uses.
//
// The interface is declared here, by the consumer, rather than beside the
// implementation: that is the Go convention, and it means a test can supply a
// three-line fake instead of a database. It takes primitives rather than a
// row struct so satisfying it costs the caller no import of internal/store
// either — the seam is narrow in types as well as in method count.
type JobStore interface {
	InsertJob(ctx context.Context, eventID, taskName string) error
}

// Envelope is the strictly typed binding for a TransientJob message.
type Envelope struct {
	EventID string `json:"eventId"`
	Payload struct {
		TaskName string `json:"taskName"`
	} `json:"payload"`
}

// NewHeartbeatHandler discards the message without deserialising it.
func NewHeartbeatHandler(log *slog.Logger) dispatch.Handler {
	return func(context.Context, []byte) error {
		log.Info("heartbeat received and discarded")
		return nil
	}
}

// NewJobHandler binds the envelope and records it via the store.
func NewJobHandler(log *slog.Logger, jobs JobStore) dispatch.Handler {
	return func(ctx context.Context, rawJSON []byte) error {
		var envelope Envelope
		if err := json.Unmarshal(rawJSON, &envelope); err != nil {
			// A malformed payload will still be malformed next time.
			return dispatch.Permanent(fmt.Errorf("deserialising transient job: %w", err))
		}

		eventID, taskName := envelope.EventID, envelope.Payload.TaskName
		if eventID == "" || taskName == "" {
			return dispatch.Permanent(fmt.Errorf("incomplete transient job: eventId=%q taskName=%q",
				eventID, taskName))
		}

		if err := jobs.InsertJob(ctx, eventID, taskName); err != nil {
			// Left retryable: a locked database or a closed transaction may
			// well succeed on the next attempt.
			return fmt.Errorf("persisting job %s: %w", eventID, err)
		}

		log.Info("job persisted", "eventId", eventID, "taskName", taskName)
		return nil
	}
}
