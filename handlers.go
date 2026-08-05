package main

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
)

// transientJob is the strictly typed binding for a TransientJob envelope.
type transientJob struct {
	EventID string `json:"eventId"`
	Payload struct {
		TaskName string `json:"taskName"`
	} `json:"payload"`
}

// newHeartbeatHandler discards the message without deserialising it.
// The logger is captured here rather than taken as a parameter, which keeps
// the Handler signature uniform across the registry.
func newHeartbeatHandler(log *slog.Logger) Handler {
	return func(_ *sqlx.DB, _ []byte) error {
		log.Info("heartbeat received and discarded")
		return nil
	}
}

// newJobHandler binds the envelope to a struct and records it in SQLite.
func newJobHandler(log *slog.Logger) Handler {
	return func(db *sqlx.DB, rawJSON []byte) error {
		var job transientJob
		if err := json.Unmarshal(rawJSON, &job); err != nil {
			return fmt.Errorf("deserialising transient job: %w", err)
		}
		if job.EventID == "" || job.Payload.TaskName == "" {
			return fmt.Errorf("incomplete transient job: eventId=%q taskName=%q",
				job.EventID, job.Payload.TaskName)
		}

		const insert = `
			INSERT INTO transient_jobs (event_id, task_name, created_at)
			VALUES (:event_id, :task_name, CURRENT_TIMESTAMP)`

		args := map[string]any{
			"event_id":  job.EventID,
			"task_name": job.Payload.TaskName,
		}
		if _, err := db.NamedExec(insert, args); err != nil {
			return fmt.Errorf("inserting job %s: %w", job.EventID, err)
		}

		log.Info("job persisted", "eventId", job.EventID, "taskName", job.Payload.TaskName)
		return nil
	}
}
