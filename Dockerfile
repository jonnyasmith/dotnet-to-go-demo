# syntax=docker/dockerfile:1

# ---- Builder ----------------------------------------------------------------
# Alpine keeps the toolchain small, but it links against musl rather than glibc,
# so the binary produced here only runs on a musl runtime (see the final stage).
FROM golang:1.24-alpine AS builder

# github.com/mattn/go-sqlite3 is a cgo wrapper around the amalgamated SQLite C
# source, so the build needs a C compiler and musl's headers. Without these the
# only way to build is CGO_ENABLED=0, which compiles happily and then fails at
# runtime with "go-sqlite3 requires cgo to work. This is a stub".
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

# Resolve dependencies in their own layer so editing the source does not
# re-download the module cache on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=1 is the whole reason for the toolchain above; -s -w drop the
# symbol table and DWARF data, which the processor never needs in production.
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /out/processor ./cmd/processor

# ---- Runtime ----------------------------------------------------------------
# Alpine again, not scratch or distroless: the cgo binary is dynamically linked
# against musl libc and would not start without it.
FROM alpine:latest

# ca-certificates for outbound TLS, tzdata so time.LoadLocation resolves names
# rather than falling back to UTC.
RUN apk add --no-cache ca-certificates tzdata

# Run unprivileged. SQLite needs write permission on the *directory*, not just
# the database file, because it creates demo.db-wal and demo.db-shm alongside it.
RUN addgroup -S appgroup \
    && adduser -S -G appgroup appuser \
    && mkdir -p /data \
    && chown appuser:appgroup /data

COPY --from=builder /out/processor /usr/local/bin/processor

USER appuser:appgroup

# The processor opens demo.db by relative path, so the working directory decides
# where the database lands. Mount a volume here to keep it beyond the container.
WORKDIR /data
VOLUME ["/data"]

ENTRYPOINT ["processor"]
