package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

// transientJob is the strictly typed binding for a TransientJob envelope.
type transientJob struct {
	EventID string `json:"eventId" db:"event_id"`
	Payload struct {
		TaskName string `json:"taskName" db:"task_name"`
	} `json:"payload"`
}

// handleHeartbeat discards the message without binding it to a struct.
func handleHeartbeat(_ *sqlx.DB, _ []byte) error {
	log.Print("heartbeat received and discarded")
	return nil
}

// handleTransientJob binds the envelope and records the job in SQLite.
func handleTransientJob(db *sqlx.DB, rawJSON []byte) error {
	var job transientJob
	if err := json.Unmarshal(rawJSON, &job); err != nil {
		return fmt.Errorf("deserialising transient job: %w", err)
	}
	if job.EventID == "" || job.Payload.TaskName == "" {
		return fmt.Errorf("incomplete transient job: eventId=%q taskName=%q", job.EventID, job.Payload.TaskName)
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

	log.Printf("job %s (%s) persisted", job.EventID, job.Payload.TaskName)
	return nil
}
