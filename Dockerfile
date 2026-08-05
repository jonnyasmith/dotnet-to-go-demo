# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /processor ./cmd/processor

FROM scratch
COPY --from=builder /processor /processor
USER 65532:65532
ENTRYPOINT ["/processor"]
