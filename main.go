package main

import (
	"log/slog"
	"math/rand/v2"
	"os"
	"time"
)

const (
	databasePath   = "demo.db"
	workerCount    = 5
	messageCount   = 40
	messageSpacing = 25 * time.Millisecond
	queueDepth     = 16
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	db, err := initialiseDatabase(databasePath)
	if err != nil {
		// A database that will not open is a fatal startup condition.
		log.Error("fatal: cannot initialise database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	queue := make(chan []byte, queueDepth)
	producer := NewProducer(rand.Uint64(), messageSpacing)
	go producer.Run(queue, messageCount)

	// Run blocks until the producer closes the queue and every worker has
	// finished its in-flight message.
	NewProcessor(db, log).Run(queue, workerCount)

	var jobs int
	if err := db.Get(&jobs, "SELECT COUNT(*) FROM transient_jobs"); err != nil {
		log.Error("could not summarise persisted jobs", "error", err)
		return
	}
	log.Info("shutdown complete", "jobsPersisted", jobs, "database", databasePath)
}
