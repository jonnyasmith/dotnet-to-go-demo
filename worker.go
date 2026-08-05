package main

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/tidwall/gjson"
)

// Handler processes one raw queue message. Dependencies other than the
// database are captured by the closure that builds the handler, which keeps
// this signature narrow enough to store in the routing map.
type Handler func(db *sqlx.DB, rawJSON []byte) error

// ErrNoDiscriminator reports a message that cannot be routed at all.
var ErrNoDiscriminator = errors.New("message carries no eventType discriminator")

// UnknownEventError reports a message whose event type has no registered
// handler. It is a distinct type so callers can recover the offending value
// with errors.As rather than parsing a string.
type UnknownEventError struct {
	EventType string
}

func (e UnknownEventError) Error() string {
	return fmt.Sprintf("unrecognised event type %q", e.EventType)
}

// Processor owns the routing registry and the worker pool.
type Processor struct {
	db     *sqlx.DB
	log    *slog.Logger
	routes map[string]Handler
}

// NewProcessor wires the default registry. The registry is a plain map keyed
// by the discriminator: routing is a lookup, never reflection.
func NewProcessor(db *sqlx.DB, log *slog.Logger) *Processor {
	return &Processor{
		db:  db,
		log: log,
		routes: map[string]Handler{
			eventHeartbeat: newHeartbeatHandler(log),
			eventJob:       newJobHandler(log),
		},
	}
}

// Register adds or replaces a route. Tests use it to substitute a handler;
// production code calls it to extend the registry.
func (p *Processor) Register(eventType string, h Handler) {
	p.routes[eventType] = h
}

// Handle routes exactly one message. It returns the routing or handler error
// instead of logging it, so callers — including tests — can inspect the
// outcome. The bytes are never copied into a string to read the discriminator.
func (p *Processor) Handle(rawJSON []byte) error {
	eventType := gjson.GetBytes(rawJSON, "eventType")
	if !eventType.Exists() {
		return ErrNoDiscriminator
	}

	handler, ok := p.routes[eventType.String()]
	if !ok {
		return UnknownEventError{EventType: eventType.String()}
	}
	return handler(p.db, rawJSON)
}

// Run launches n workers against queue and blocks until the queue is closed
// and every in-flight message has been handled.
func (p *Processor) Run(queue <-chan []byte, n int) {
	var wg sync.WaitGroup

	for id := range n {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for payload := range queue {
				if err := p.Handle(payload); err != nil {
					p.log.Warn("message dropped", "worker", id, "error", err)
				}
			}
			p.log.Debug("queue closed, worker shutting down", "worker", id)
		}(id + 1)
	}

	wg.Wait()
}
