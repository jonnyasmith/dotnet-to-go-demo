package main

import (
	"log"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/tidwall/gjson"
)

// handlerFunc processes one raw queue message.
type handlerFunc func(db *sqlx.DB, rawJSON []byte) error

// routes is the routing strategy: an explicit registry keyed by the
// "eventType" discriminator. No reflection, no mediator library.
var routes = map[string]handlerFunc{
	eventHeartbeat: handleHeartbeat,
	eventJob:       handleTransientJob,
}

// startWorkers launches n goroutines that drain queue until it is closed.
// The caller waits on wg for a graceful shutdown.
func startWorkers(n int, db *sqlx.DB, queue <-chan []byte, wg *sync.WaitGroup) {
	for id := range n {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for payload := range queue {
				dispatch(id, db, payload)
			}
			log.Printf("worker %d: queue closed, shutting down", id)
		}(id + 1)
	}
}

// dispatch inspects only the discriminator, then hands the untouched bytes to
// the registered handler. Unrecognised events are logged and dropped.
func dispatch(workerID int, db *sqlx.DB, payload []byte) {
	eventType := gjson.GetBytes(payload, "eventType")
	if !eventType.Exists() {
		log.Printf("worker %d: dropping message with no eventType discriminator", workerID)
		return
	}

	handler, ok := routes[eventType.String()]
	if !ok {
		log.Printf("worker %d: unrecognised event type %q, dropping message", workerID, eventType.String())
		return
	}

	if err := handler(db, payload); err != nil {
		log.Printf("worker %d: handler for %q failed: %v", workerID, eventType.String(), err)
	}
}
