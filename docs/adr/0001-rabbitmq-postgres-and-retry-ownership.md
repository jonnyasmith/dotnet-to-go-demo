# ADR 0001: Use RabbitMQ and Postgres, with retries split by ownership

## Status

Accepted

## Context

The original demo used a buffered Go channel as a queue and SQLite as storage.
That made broker redelivery, acknowledgement, dead-lettering, durability, and
backpressure simulated behaviours. SQLite also made the batching writer look
like a workaround for a single-writer database rather than a throughput choice.

## Decision

Use RabbitMQ for delivery and Postgres for persistence, both supplied by Docker
Compose in development and by testcontainers-go in integration tests.

RabbitMQ owns durable delivery outcomes. Jobs use a durable queue with manual
acknowledgement and prefetch equal to the worker count. A successful handler is
acknowledged only after its store call has committed. Permanent failures and
transient failures that exhaust their budget are nacked without requeue and
routed through a fanout dead-letter exchange. Shutdown nacks in-flight work
with requeue. Messages and broker definitions are durable, and seed publishing
uses publisher confirms.

The application still owns fast retries. Transient failures are retried in the
worker with exponential backoff before the broker sees a final nack. Malformed,
invalid, and unknown events skip that budget. Routing stays in the application
on the `eventType` discriminator; broker routing keys do not select handlers.

Postgres is accessed through pgx's `database/sql` adapter so the existing sqlx
and consumer-declared `JobStore` seam remain. The single batching writer is
retained, but the connection pool is no longer pinned to one connection.

## Alternatives considered

Azurite would resemble the queue originally imitated, but it does not exercise
the push delivery, manual ack/nack, dead-letter exchange, and prefetch mechanics
this demo is intended to teach. Kafka and Redpanda are a poor match for
per-message acknowledgement. Keeping SQLite would preserve cgo and obscure the
reason for batching. Moving retries into RabbitMQ would add delayed-message or
TTL machinery and make brief failures pay a broker round trip. Routing event
types with a topic exchange would duplicate and weaken the application router.

## Consequences

Running the full test suite now requires Docker; `go test -short ./...` remains
the fast, offline loop. The application reconnects with capped exponential
backoff and redeclares topology on every connection. RabbitMQ's consumer
acknowledgement timeout bounds the maximum useful in-process retry delay.
