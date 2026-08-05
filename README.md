# Transient job queue processor

A small event-driven Go worker that demonstrates real at-least-once delivery.
RabbitMQ supplies durable messages, manual acknowledgements, prefetch
backpressure, redelivery, and dead-letter routing. Postgres stores transient
jobs idempotently through a batching writer.

The application routes JSON envelopes on their `eventType` field. Successful
job deliveries are acknowledged only after their Postgres batch commits.
Transient handler failures retry in-process with exponential backoff; exhausted
and permanent failures are nacked to RabbitMQ's dead-letter exchange. Work
interrupted by shutdown is nacked with requeue.

## Run the demo

Prerequisites are Go 1.24 or newer and a running Docker daemon.

Start RabbitMQ and Postgres, then run the processor on the host:

```sh
docker compose up -d
go run ./cmd/processor
```

The defaults target Compose's published ports, so no environment setup is
needed. Stop the processor with Ctrl-C. RabbitMQ's management UI is available
at <http://localhost:15672> using `guest` / `guest`.

To build and run the processor in Compose as well:

```sh
docker compose --profile processor up --build
```

The processor service waits for RabbitMQ and Postgres healthchecks. It is behind
the `processor` profile so `docker compose up -d` leaves dependencies running
for the faster host edit-run loop.

## Configuration

Configuration is environment-only:

| Variable | Default | Meaning |
| --- | --- | --- |
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection URL |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/jobs?sslmode=disable` | Postgres connection URL |
| `WORKER_COUNT` | `5` | Workers and RabbitMQ prefetch limit |
| `RETRY_BUDGET` | `2` | Additional in-process attempts after the first |

On startup the demo declares its durable topology, creates the Postgres table
if needed, and publishes 40 persistent seed messages with publisher confirms.
It keeps running to demonstrate reconnects and drains dead letters to the log.

## Tests

```sh
go test -short ./...       # fast unit tests; no Docker
go test ./... -race        # full suite; starts containers automatically
```

Container-backed packages share one RabbitMQ or Postgres container per package.
Postgres tests isolate parallel cases in separate schemas, while broker tests
use unique queue and exchange names. No manually started Compose stack is used
by the tests.

## Layout

| Package | Role |
| --- | --- |
| `cmd/processor` | Environment parsing and composition root |
| `internal/dispatch` | Handler registry, worker pool, retry and ack/nack decisions |
| `internal/queue` | RabbitMQ topology, reconnecting consumer, confirmed seed publisher |
| `internal/store` | Postgres schema, pool, and batching writer |
| `internal/jobs` | Event schemas and handlers |
| `test/e2e` | Real-broker failure and redelivery tests |

The architecture decision and rejected alternatives are recorded in
[ADR 0001](docs/adr/0001-rabbitmq-postgres-and-retry-ownership.md).
