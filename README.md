# Go Transient Job Queue Processor

A demo of an event-driven worker: a producer pushes raw JSON onto a channel,
a pool of workers reads the `eventType` discriminator without deserialising the
whole message, and a map-based registry routes each message to a handler. Job
events are persisted to SQLite; heartbeats are discarded.

## Prerequisites

- Go 1.22 or newer (`go version`).
- A C toolchain. `github.com/mattn/go-sqlite3` is a cgo binding around the
  SQLite C library, so `CGO_ENABLED=1` (the default for native builds) and a
  working compiler are required. On macOS run `xcode-select --install`; on
  Debian/Ubuntu install `build-essential`.

This matters more than it looks: with `CGO_ENABLED=0`, or when
cross-compiling, the build still succeeds — it links a stub — and the failure
only appears at runtime:

```
level=ERROR msg="fatal: cannot initialise database" error="connecting to demo.db:
Binary was compiled with 'CGO_ENABLED=0', go-sqlite3 requires cgo to work. This is a stub"
```

## Run it

```sh
go mod download   # restore dependencies, like dotnet restore
go run .          # build and run in one step
```

Or build a binary:

```sh
go build -o go-demo .
./go-demo
```

The app creates `demo.db` in the working directory, processes 40 messages
across 5 workers, and exits once the queue is drained:

```
time=… level=INFO msg="heartbeat received and discarded"
time=… level=INFO msg="job persisted" eventId=evt-0003-1a2b3c4d taskName=ReconcileLedger
time=… level=INFO msg="shutdown complete" jobsPersisted=17 database=demo.db
```

Inspect the results:

```sh
sqlite3 demo.db "SELECT * FROM transient_jobs LIMIT 5;"
```

`demo.db` is gitignored; delete it any time to start fresh.

## Layout

Every file is in `package main` at the repository root. Go builds a package
from a directory, so these files see each other's identifiers without imports
or an assembly reference; lowercase identifiers are package-private and
uppercase ones are exported.

| File | Role |
| --- | --- |
| `main.go` | Composition root: opens the database, wires producer and processor, waits for shutdown. |
| `db.go` | `initialiseDatabase` — connection pool and `CREATE TABLE IF NOT EXISTS`. |
| `producer.go` | `Producer` — generates heartbeat and job JSON on a `chan []byte`. |
| `worker.go` | `Processor` — routing registry, discriminator lookup, worker pool. |
| `handlers.go` | The two handlers, built as closures over their dependencies. |

### Design notes

**Routing is a map lookup.** `Processor.routes` is a
`map[string]func(*sqlx.DB, []byte) error`. Adding an event type means adding a
map entry — there is no mediator, no assembly scanning, no reflection.

**The discriminator is read without full deserialisation.**
`gjson.GetBytes(raw, "eventType")` walks the bytes and returns the one value,
so heartbeats never allocate a struct.

**Dependencies are injected by closure.** `newJobHandler(log)` returns a
function that has captured the logger. That is Go's usual substitute for a DI
container: no registration, no lifetimes, and the registry's value type stays
a plain function.

**Shutdown is driven by closing the channel.** The producer closes the queue
when it finishes; each worker's `for range queue` loop then ends; the
`sync.WaitGroup` in `Processor.Run` releases once all five have returned. No
cancellation token, no timeout.

**SQLite takes one writer at a time.** `initialiseDatabase` pins the pool to a
single connection and sets a busy timeout. Without both, concurrent workers
lose inserts to `database is locked` — there is a test that proves it.

## Testing

Coming from xUnit, the mechanics differ more than the ideas do.

| .NET | Go |
| --- | --- |
| Separate test project | `*_test.go` beside the code, same package |
| `[Fact]` / `[Theory]` | `func TestXxx(t *testing.T)`, discovered by name |
| `[InlineData]` | A slice of structs plus `t.Run` (table-driven) |
| `Assert.Equal(a, b)` | `if a != b { t.Errorf(...) }` — no assertion library in the stdlib |
| `IAsyncLifetime` / fixtures | `t.TempDir()`, `t.Cleanup(...)` |
| `InternalsVisibleTo` | Unnecessary: tests in the same package see unexported identifiers |
| Trait-based filtering | `testing.Short()` plus `go test -short` |
| Moq / NSubstitute | A hand-written function or struct; interfaces are satisfied implicitly |

`t.Errorf` records a failure and continues; `t.Fatalf` stops that test
immediately. Use `Fatalf` when the rest of the test cannot run meaningfully.

### Commands

```sh
go test ./...                 # everything
go test ./... -v              # per-test output
go test ./... -race           # detect data races — use this on concurrent code
go test ./... -short          # skip the end-to-end tier
go test -run TestHandleRouting ./...   # filter by regex, like --filter
go test ./... -cover                   # coverage summary
go test ./... -count=1                 # bypass the result cache
```

Go caches successful results, so an unchanged package reports `(cached)`.
`-count=1` is the idiomatic way to force a re-run.

### The three tiers here

**Unit** — `worker_test.go`, `producer_test.go`. No database, no I/O. The
router tests register a substitute handler and pass a `nil` pool, because the
routing decision does not depend on one. `TestProducerIsReproducibleForAGivenSeed`
relies on the producer taking an explicit seed; the package-level generator in
`math/rand/v2` could not be pinned this way.

**Integration** — `handlers_test.go`, `db_test.go`. These use a real SQLite
database from `newTestDB`, created under `t.TempDir()` and removed
automatically. Mocking the database here would only test the mock: SQLite is a
library, so a genuine database costs about a millisecond.

**End-to-end** — `pipeline_test.go`. The real producer, the real pool and a
real file on disk, wired as `main` wires them. A fixed seed makes the expected
row count exact rather than approximate, and a `select` with a timeout turns a
deadlock into a failure instead of a hung suite.

### Writing a new test

Add the test first and watch it fail — `go test` reports the failing line and
the values involved, so a red run is your specification:

```go
func TestJobHandlerTrimsTaskName(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	raw := []byte(`{"eventType":"TransientJob","eventId":"evt-1","payload":{"taskName":"  X  "}}`)

	if err := newJobHandler(discardLogger())(db, raw); err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}

	var got string
	if err := db.Get(&got, "SELECT task_name FROM transient_jobs"); err != nil {
		t.Fatalf("reading back the job: %v", err)
	}
	if got != "X" {
		t.Errorf("task_name = %q, want %q", got, "X")
	}
}
```

`t.Parallel()` opts a test into running alongside its siblings. It is safe here
because each test owns its own database; tests that share mutable state must
leave it out.
