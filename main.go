package main

import (
	"log"
	"sync"
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
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	db, err := initialiseDatabase(databasePath)
	if err != nil {
		// A database that will not open is a fatal startup condition.
		log.Fatalf("fatal: %v", err)
	}
	defer db.Close()

	queue := make(chan []byte, queueDepth)

	var wg sync.WaitGroup
	startWorkers(workerCount, db, queue, &wg)

	go produce(queue, messageCount, messageSpacing)

	// produce closes the queue once drained, so this returns only after every
	// worker has finished its in-flight message.
	wg.Wait()

	var jobs int
	if err := db.Get(&jobs, "SELECT COUNT(*) FROM transient_jobs"); err != nil {
		log.Printf("could not summarise persisted jobs: %v", err)
		return
	}
	log.Printf("shutdown complete: %d job(s) persisted in %s", jobs, databasePath)
}
