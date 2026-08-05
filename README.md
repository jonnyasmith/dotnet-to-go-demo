# Transient job queue processor

A small event-driven worker. A producer synthesises 40 raw JSON messages — a
coin flip between `SystemHeartbeat` and `TransientJob` — onto a buffered
channel, one every 25 ms. Five worker goroutines drain that channel, read the
`eventType` discriminator straight out of the bytes, and route each message
through a registry to a handler: heartbeats are logged and discarded, job
envelopes are bound to a struct and persisted to SQLite through a single
batching writer. Failures are retried with exponential backoff and
dead-lettered once the budget runs out, and Ctrl-C unwinds the pipeline in
order. There is no HTTP server and nothing to configure.

## What this is for

I work in .NET and wanted to know what Go actually asks of you: how a module is
laid out, where the package boundaries fall, what testing looks like with no
mocking library and no DI container, and which C# habits stop paying. Building
something small with real edges — concurrency, a database, cancellation,
partial failure — answered that faster than reading about it would have.

The domain is therefore thin and the mechanics are not. The producer is a
stand-in rather than a real broker, and there is nothing to configure. The
worker pool, the retry and dead-letter policy, the batching writer, the context
plumbing and the shutdown ordering are all real, as are the tests around them.

The repository is public, so the notes throughout are written for someone with
a .NET background reading it cold; the comparisons to xUnit, `InternalsVisibleTo`
and `IHostApplicationLifetime` are the reference points I had at the time. They
are a first pass at Go rather than seasoned advice. Where something was a
judgement call I have tried to say so, including the calls I would revisit —
those are collected under [Known compromises](#known-compromises) at the end.

## Prerequisites

- Go 1.24 or newer (`go version`).
- A C toolchain. `github.com/mattn/go-sqlite3` is a cgo binding around the
  SQLite C library, so `CGO_ENABLED=1` (the default for native builds) and a
  working compiler are required. On macOS run `xcode-select --install`; on
  Debian/Ubuntu install `build-essential`.
- Optionally the `sqlite3` CLI, to inspect the database afterwards.

This matters more than it looks: with `CGO_ENABLED=0`, or when
cross-compiling, the build still succeeds — it links a stub — and the failure
only appears at runtime:

```
level=ERROR msg=fatal error="connecting to demo.db: Binary was compiled with
'CGO_ENABLED=0', go-sqlite3 requires cgo to work. This is a stub"
```

## Run it

```sh
go mod download          # restore dependencies, like dotnet restore
go run ./cmd/processor   # build and run in one step
```

Note the package path. `go run .` used to work, when every file sat in
`package main` at the repository root; the root is now just a module directory
containing no Go files, so that command fails with `no Go files in …`. The
argument to `go run` is a *package*, and the entry point lives at
`./cmd/processor`.

Or build a binary:

```sh
go build -o processor ./cmd/processor
./processor
```

The app creates `demo.db` in the working directory, processes 40 messages
across 5 workers, and exits once the queue is drained. Logging is `log/slog`
with the text handler, writing to stderr:

```
time=2026-08-05T07:41:04.576+01:00 level=INFO msg="heartbeat received and discarded"
time=2026-08-05T07:41:04.634+01:00 level=INFO msg="job persisted" eventId=evt-0002-c0b6d292 taskName=ReconcileLedger
time=2026-08-05T07:41:04.685+01:00 level=INFO msg="job persisted" eventId=evt-0004-8a09dd66 taskName=PurgeStaleSessions
time=2026-08-05T07:41:04.706+01:00 level=INFO msg="heartbeat received and discarded"
time=2026-08-05T07:41:04.738+01:00 level=INFO msg="job persisted" eventId=evt-0006-a54a082b taskName=ReconcileLedger
...
time=2026-08-05T07:41:05.624+01:00 level=INFO msg="shutdown complete" jobsPersisted=19 database=demo.db
```

`jobsPersisted` differs between runs: `main` seeds the producer with
`rand.Uint64()`, so the heartbeat/job split is genuinely random. Tests pin the
seed instead, which is why they can assert an exact count. `dead letter` warnings
appear only when a handler exhausts its retries, which the demo's own
well-formed messages never do — provoking them is the tests' job.

Inspect the results:

```sh
sqlite3 demo.db "SELECT COUNT(*) FROM transient_jobs;"
sqlite3 -header -box demo.db \
  "SELECT event_id, task_name, created_at FROM transient_jobs LIMIT 3;"
```

```
┌───────────────────┬────────────────────┬─────────────────────┐
│     event_id      │     task_name      │     created_at      │
├───────────────────┼────────────────────┼─────────────────────┤
│ evt-0002-c0b6d292 │ ReconcileLedger    │ 2026-08-05 06:41:04 │
│ evt-0004-8a09dd66 │ PurgeStaleSessions │ 2026-08-05 06:41:04 │
│ evt-0006-a54a082b │ ReconcileLedger    │ 2026-08-05 06:41:04 │
└───────────────────┴────────────────────┴─────────────────────┘
```

The database opens in WAL mode, so `demo.db-wal` and `demo.db-shm` appear
alongside it. All three are gitignored; delete them any time to start fresh.

## In a container

```sh
docker build -t go-demo-processor .
docker run --rm -v "$PWD/data:/data" go-demo-processor
sqlite3 data/demo.db "SELECT COUNT(*) FROM transient_jobs;"
```

The `Dockerfile` is two stages: `golang:1.24-alpine` with `gcc` and `musl-dev`
added to build the binary, then `alpine:latest` to run it. Alpine at both ends
is not incidental. cgo means the binary is dynamically linked, and building it
against musl produces something that will not start on a glibc image — the
usual `FROM scratch` ending is unavailable for the same reason. The result is
about 14 MB.

It runs as a non-root user with `/data` as both the working directory and a
volume, because the database path is relative. SQLite needs write permission on
the *directory* rather than just the file: WAL mode creates `demo.db-wal` and
`demo.db-shm` beside it, so a container that can write the file but not its
parent fails at the first insert.

## Layout

```
cmd/processor/     main package — the composition root
internal/dispatch/ routing registry, worker pool, retry policy
internal/store/    SQLite schema, connection, batching writer
internal/jobs/     message schemas and their handlers
internal/queue/    the stand-in message source
test/e2e/          whole-pipeline tests, no production code
```

**A directory is a package.** There is no project file listing sources: every
`.go` file in a directory belongs to the same package, and the compiler compiles
the directory. Nothing to add to a `<Compile Include>`, no assembly references —
you import by path, and an unused import is a compile error.

**`internal/` is a compiler rule, not a naming convention.** A package under
`internal/` may only be imported by code rooted at `internal/`'s parent
directory. `github.com/jonny/go-demo/internal/store` is therefore importable
from anywhere inside this module and by nothing outside it, and that is enforced
at build time. It is the closest thing Go has to an access modifier above the
identifier level: roughly C#'s `internal` with no `InternalsVisibleTo` escape
hatch, except the boundary is a path in the import graph rather than an
assembly. Below that, visibility is per identifier and decided by case —
capitalised is exported, lowercase is package-private.

| Package | Role |
| --- | --- |
| `cmd/processor` | `package main`: opens the store, starts the writer, wires the producer, router and dead-letter reaper, then shuts them down in order. The only `main` function in the tree. `cmd/` is where Go projects put entry points; a second binary would be a sibling directory. |
| `internal/dispatch` | `Router`: the registry, the worker pool, the retry and dead-letter policy, and the error types that classify a failure. Knows nothing about SQLite or about any particular message. |
| `internal/store` | Everything SQLite: the schema, the pinned connection, the single batching writer goroutine, and the read queries used for summaries and assertions. |
| `internal/jobs` | The two discriminator constants, the `Envelope` binding, and the handler constructors. The only package that knows what a message *means*. |
| `internal/queue` | `Producer`: a seedable stand-in for the real queue, emitting the same JSON an Azure Storage Queue would deliver. |
| `test/e2e` | End-to-end tests only — no production code. It sits outside `internal/` and imports the packages the way an outside caller would. |

Dependencies run one way: `cmd/processor` → `queue` → `jobs` → {`dispatch`,
`store`}, and neither `dispatch` nor `store` imports anything of ours. Go
rejects import cycles outright, so that arrow is structural rather than
aspirational.

## Design notes

**Routing is a map lookup.** `Router.routes` is a `map[string]Handler`;
`Register` puts a function in it and `Handle` takes one out. No mediator, no
assembly scanning, no reflection, no attributes, no container resolving
`IRequestHandler<T>`. Dispatch costs a hash lookup, the whole registry fits on
one screen, and a type nobody registered dead-letters as an `UnknownEventError`
instead of silently doing nothing.

**The discriminator is read without deserialising the message.**
`gjson.GetBytes(rawJSON, "eventType")` walks the bytes in place and returns that
one value, so a heartbeat is routed, logged and dropped without ever allocating
a struct. The router reads exactly one field and hands the untouched bytes on;
each handler then decides whether it needs the rest. `NewJobHandler` does and
calls `json.Unmarshal`; `NewHeartbeatHandler` does not and never looks.

**Dependencies are injected by closure.** `jobs.NewJobHandler(log, st)` returns
a `dispatch.Handler` — a plain `func(ctx, []byte) error` that has captured the
logger and the store. No container, no service registration, no lifetimes,
nothing resolved at startup. Constructor injection is still the pattern; the
container is simply absent, and the wiring is a handful of lines in `run` that
the compiler checks.

**The consumer declares the interface.** `jobs.JobStore` is declared in
`internal/jobs`, beside the code that *uses* it, and names the single method
that package needs. `store.Store` never mentions `JobStore` and does not import
`jobs`; Go interfaces are satisfied implicitly, so there is no `: IJobStore` on
the declaration to keep in sync. This is deliberately the opposite of the .NET
habit of shipping `IFoo` next to `Foo` — an interface with exactly one
implementation, written by the implementer, buys nothing here. Declaring it at
the consumer means the dependency is described as narrowly as the consumer's
actual needs. It is narrow in its *types* too: the method takes an event id and
a task name rather than a `store.Job`, so satisfying it costs the caller no
import of `internal/store` either. A test can therefore supply a struct with
one method — no mocking library, and nothing from the SQLite package at all.

**`context.Context` is threaded through everything.** Treat it as
`CancellationToken` with two extra jobs (deadlines and request-scoped values).
By convention it is the first parameter of any function that can block, and it
is passed explicitly rather than pulled from ambient state.
`signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)`
produces the root: a context cancelled by Ctrl-C or SIGTERM, which is what
`IHostApplicationLifetime` does for you in a .NET host. A second signal restores
the default disposition, so an unresponsive process can still be killed.

The writer deliberately gets a *different* context. `run` hands
`store.RunWriter` its own `context.WithCancel(context.Background())`. Were it to
share `ctx`, Ctrl-C would stop the writer while handlers were parked inside
`InsertJob` waiting for their acknowledgement, and those in-flight inserts would
fail on the way out. Instead the writer outlives the cancellation and is stopped
explicitly, after the workers have finished. The final `CountJobs` call uses
`context.WithoutCancel(ctx)` for the same reason: the summary query must still
run on an already-cancelled context.

**One batching writer goroutine.** SQLite permits a single writer. The obvious
design — every worker inserting on its own pooled connection — buys nothing but
lock contention and `database is locked`. So the pool is pinned with
`SetMaxOpenConns(1)` and every insert funnels into one goroutine: `InsertJob`
sends a request on a channel and blocks on an ack channel, while `RunWriter`
accumulates requests until it holds 64 or 5 ms has elapsed, commits the lot in
one transaction, and answers every waiter with the same result. Five workers
still run concurrently; they merely fan in at the point where concurrency stops
paying, because the real cost is the fsync at commit and one commit can carry 64
rows. This is the shape Go pushes you towards: rather than guard the database
with a mutex, arrange for exactly one goroutine to own it.

**Idempotency is a schema concern.** Real queues deliver at-least-once, so the
same `eventId` *will* arrive twice. Instead of a "have I seen this?" lookup —
which is a race wearing a query's clothes — `event_id` is `UNIQUE` and the
insert ends `ON CONFLICT(event_id) DO NOTHING`. A redelivery becomes a silent
no-op that the handler cannot distinguish from a first delivery, which is
precisely what makes it safe to retry.

**Retry, backoff, and opting out of both.** `Config.Retries` counts the
*additional* attempts after the first, and `Backoff` doubles each time (20 ms,
then 40 ms). A message that exhausts the budget is sent to `Config.DeadLetter`,
a `chan<- []byte` drained by a reaper goroutine in `main`.

Not every failure deserves that budget. `dispatch.Retryable` returns false for a
missing discriminator, an unrecognised event type, a cancelled context, and
anything wrapped in `dispatch.Permanent`. A malformed payload will be just as
malformed in 40 ms, so `NewJobHandler` wraps its deserialisation and validation
failures in `Permanent` and they dead-letter on the first attempt; a store
failure is returned unwrapped and so gets retried. `PermanentError` implements
`Unwrap`, which keeps `errors.Is` and `errors.As` working through the wrapper —
those two are the Go replacement for `catch (SomeSpecificException)`, and
`UnknownEventError` is a struct precisely so a caller can recover the offending
event type with `errors.As` rather than parsing a message string.

Cancellation is not poison, and the pool keeps the two apart. When Ctrl-C
interrupts a handler mid-insert it fails with `context.Canceled`, which is not
the message's fault: `deliver` checks `ctx.Err()` after a failed attempt and
abandons the payload with a debug line instead of dead-lettering it. Without
that check, a clean shutdown files any message caught mid-insert as
unprocessable and logs an error for it — the loudest possible way to report
that nothing went wrong. It needs a worker to be inside `InsertJob` at the
moment of cancellation, so interrupting the demo often misses the window
entirely; `test/e2e` reproduces it by cancelling under enough load to keep the
writer busy.

**Shutdown ordering.** `run` unwinds in dependency order, and the order is the
whole trick:

1. `router.Run` returns — either the producer closed the queue and all five
   workers drained it, or `ctx` was cancelled.
2. `close(deadLetters)`, then wait for the reaper. Safe only now: sending on a
   closed channel is a panic, so the close has to follow the last sender.
3. `stopWriter()`, then wait for the writer. Safe only now: nothing is left to
   insert, and stopping the writer earlier would strand a handler blocked on an
   acknowledgement that would never come.
4. `CountJobs` on `context.WithoutCancel(ctx)`, log the summary, and let the
   deferred `st.Close()` release the connection.

Each step closes the tap upstream of the thing it is shutting down. Swap any two
and you get either a send on a closed channel — a panic, not an exception you
can catch usefully — or a hang.

## Testing

Coming from xUnit, the mechanics differ more than the ideas do.

| .NET | Go |
| --- | --- |
| Separate test project | `*_test.go` beside the code, usually in the same package |
| `[Fact]` / `[Theory]` | `func TestXxx(t *testing.T)`, discovered by name |
| `[InlineData]` | A slice of structs plus `t.Run` (table-driven) |
| `Assert.Equal(a, b)` | `if got != want { t.Errorf(...) }` — no assertion library in the stdlib |
| `IAsyncLifetime` / fixtures | `t.TempDir()`, `t.Cleanup(...)` |
| `InternalsVisibleTo` | Unnecessary: tests in the same package see unexported identifiers |
| Trait-based filtering | `testing.Short()` plus `go test -short` |
| `dotnet test --filter` | `go test -run` with a regex over test names |
| Moq / NSubstitute | A hand-written function or struct; interfaces are satisfied implicitly |
| FsCheck / AutoFixture | `func FuzzXxx(f *testing.F)`, built into `go test` |
| Parallel by default, `[Collection]` to serialise | Serial within a package; `t.Parallel()` opts in. Separate packages run concurrently |
| `dotnet test` | `go test ./...` |

`t.Errorf` records a failure and continues; `t.Fatalf` stops that test
immediately. Use `Fatalf` when the rest of the test cannot run meaningfully.

### Commands

```sh
go test ./...                          # everything
go test ./... -v                       # per-test output
go test ./... -race                    # detect data races
go test ./... -short                   # skip the slow end-to-end pipeline test
go test ./... -cover                   # coverage summary per package
go test ./... -count=1                 # bypass the result cache
go test ./... -run TestRetry           # unanchored name regex, like --filter
go test ./internal/store/ -race -count=1        # one package, uncached
go test ./internal/dispatch/ -fuzz FuzzHandle   # fuzz until it fails or you stop it

# bounded, and skipping the unit tests so only the fuzzing runs
go test ./internal/dispatch/ -run FuzzHandle -fuzz FuzzHandle -fuzztime 10s
```

Go caches successful results, so an unchanged package reports `(cached)`.
`-count=1` is the idiomatic way to force a re-run.

`-race` deserves special mention on a codebase like this one. It instruments
every memory access and fails the run on an unsynchronised one, even when the
test itself passed — without it, a worker pool test proves only that a
particular interleaving happened to work. Treat it as the default here.

Fuzzing needs no extra dependency: `-fuzz` takes a regex matching exactly one
`FuzzXxx` target and generates inputs until something fails, `-fuzztime`
elapses, or you interrupt it. Without `-fuzz` that same target still runs as an
ordinary test over its seed corpus, so the seeds are regression-tested on every
plain `go test`. An input that does fail is written to the package's
`testdata/fuzz/` directory and becomes a permanent seed — you commit it, and the
fuzz finding turns into a repeatable regression test.

### The three tiers here

**Unit** — `internal/dispatch`, `internal/jobs`, `internal/queue`. No database,
no files, and no waiting on the wall clock. The router's tests register
substitute handlers; `NewJobHandler` takes a `JobStore`, so a test hands it a
struct with one method rather than a database; the producer's unexported
`sleep` and `now` fields are both swapped out, so message spacing can be
asserted without spending it and a seeded run replays byte for byte. These
tests live in the same package as the code, which is what lets them reach an
unexported field at all — the `InternalsVisibleTo` question never arises.

`internal/dispatch/fuzz_test.go` is the exception, and the reason is worth
knowing. It declares `package dispatch_test`, an *external* test package
compiled separately from `dispatch` itself. That is what lets it import
`internal/jobs` and fuzz the real handlers: `jobs` imports `dispatch`, so an
in-package test file reaching for them would close an import cycle and fail to
build. A directory may hold both packages at once, and this is what the second
one is for.

**Integration** — `internal/store`. Real SQLite files created under
`t.TempDir()` and removed automatically. Mocking the database here would only
test the mock: SQLite is a library rather than a server, so a genuine database
costs about a millisecond and actually exercises the `UNIQUE` constraint, the
transaction boundary and the batching timer, which are the parts worth
verifying.

**End-to-end** — `test/e2e`, plus `cmd/processor/main_test.go`. The real
producer, router and store, against a real file on disk, wired as `run` wires
them. A pinned seed makes the expected row count exact rather than approximate,
and every wait is a `select` with a timeout so a deadlock fails the test instead
of hanging the suite. The full-pipeline test calls `testing.Short()` and skips
under `-short`; the quicker cases around dead-lettering and mid-run cancellation
always run. `cmd/processor/main_test.go` goes one step further and drives `run`
itself, sending a real `SIGINT` to the test process — the only way to cover the
`signal.NotifyContext` path.

## Adding a new event type

Three steps, no ceremony.

**1. Declare the discriminator** in `internal/jobs`, beside the existing two:

```go
const EventInvoice = "InvoiceRaised"
```

**2. Write a constructor returning a `dispatch.Handler`**, capturing whatever it
needs. Wrap anything a retry cannot fix in `dispatch.Permanent`; return
everything else as-is so the pool retries it:

```go
func NewInvoiceHandler(log *slog.Logger, jobs JobStore) dispatch.Handler {
	return func(ctx context.Context, rawJSON []byte) error {
		var envelope Envelope
		if err := json.Unmarshal(rawJSON, &envelope); err != nil {
			return dispatch.Permanent(fmt.Errorf("deserialising invoice: %w", err))
		}
		// ... validate, then persist via jobs.InsertJob(ctx, ...)
		return nil
	}
}
```

**3. Register it** in `cmd/processor/main.go`, next to the others:

```go
router.Register(jobs.EventInvoice, jobs.NewInvoiceHandler(log, st))
```

That is all of it: no attribute, no interface to implement, no container
registration, no scanning. The compiler rejects a function of the wrong shape,
and an event type you forget to register dead-letters as an `UnknownEventError`
rather than disappearing.

## Continuous integration

`.github/workflows/ci.yml` runs `go build ./...`, `go vet ./...` and
`go test ./... -race -count=1` on ubuntu-latest against Go 1.24. `go vet` is
worth calling out: a set of correctness checks shipped with the toolchain and
run automatically as part of `go test`, catching things like a `Printf` verb
that does not match its argument. There is no separate linter package to
choose, install and configure before the first useful signal.

## Known compromises

Things I would change, or at least argue about, on a second pass.

**The batching writer is more machinery than the problem needed.** Accumulating
64 rows or 5 ms before committing is a real optimisation, but the workers here
produce a few dozen messages and would have been fine inserting one at a time.
It also introduces shared fate: `writeBatch` returns one error for the whole
transaction and every waiter in the batch receives it, so a single bad row
fails 64 callers, all of which then retry. Correct, because the retry is
idempotent, but the blast radius is wider than the failure.

**Two identical sleep helpers.** `dispatch.sleep` and `queue.sleepCtx` are the
same fifteen lines. Extracting them would mean a shared utility package
imported by both, which is the seed of the `Common`/`Shared` project that
eventually holds everything; Go's own advice is that a little copying beats a
little dependency. I am not certain that is the right call at two copies.

**The `TransientJob` wire format is written down twice** — once as a format
string in `internal/queue`, once as struct tags on `jobs.Envelope`. Renaming a
field in one place leaves the other compiling and wrong. A real system would
have a schema somewhere; the e2e tests are what currently catch the mismatch.

**The producer's `sleep` field exists for the tests.** Injecting a clock to
make timing assertions cheap is a defensible seam, but it is a seam the
production code does not otherwise need, and it is worth being honest that the
design moved to suit the test.

**Test helper conventions drifted.** `discardLogger` in three packages,
`discardTestLogger` in a fourth; `runAsync` names two different contracts in
two files. Harmless, and exactly the sort of thing a shared house style would
have settled up front.
