// Command processor runs the transient job queue demo against RabbitMQ and
// Postgres.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jonny/go-demo/internal/dispatch"
	"github.com/jonny/go-demo/internal/jobs"
	"github.com/jonny/go-demo/internal/queue"
	"github.com/jonny/go-demo/internal/store"
)

const (
	defaultAMQPURL     = "amqp://guest:guest@localhost:5672/"
	defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/jobs?sslmode=disable"
	messageCount       = 40
	messageSpacing     = 25 * time.Millisecond
	queueDepth         = 16
	retryBackoff       = 20 * time.Millisecond
)

type config struct {
	AMQPURL     string
	DatabaseURL string
	Workers     int
	Retries     int
	Seed        uint64
	Topology    queue.Topology
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := configFromEnv()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log, cfg); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func configFromEnv() (config, error) {
	workers, err := positiveEnv("WORKER_COUNT", 5)
	if err != nil {
		return config{}, err
	}
	retries, err := nonNegativeEnv("RETRY_BUDGET", 2)
	if err != nil {
		return config{}, err
	}
	return config{
		AMQPURL:     envOr("AMQP_URL", defaultAMQPURL),
		DatabaseURL: envOr("DATABASE_URL", defaultDatabaseURL),
		Workers:     workers,
		Retries:     retries,
		Seed:        rand.Uint64(),
		Topology:    queue.DefaultTopology,
	}, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func positiveEnv(name string, fallback int) (int, error) {
	value, err := nonNegativeEnv(name, fallback)
	if err != nil {
		return 0, err
	}
	if value == 0 {
		return 0, fmt.Errorf("%s must be greater than zero", name)
	}
	return value, nil
}

func nonNegativeEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer, got %q", name, raw)
	}
	return value, nil
}

func run(ctx context.Context, log *slog.Logger, cfg config) error {
	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	writerCtx, stopWriter := context.WithCancel(context.Background())
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		st.RunWriter(writerCtx)
	}()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	deliveries := make(chan dispatch.Delivery, queueDepth)
	consumer := queue.Consumer{
		URL: cfg.AMQPURL, Topology: cfg.Topology, Prefetch: cfg.Workers, Log: log,
		DrainDeadLetters: true,
	}
	var consumers sync.WaitGroup
	consumers.Add(1)
	go func() {
		defer consumers.Done()
		consumer.Run(runCtx, deliveries)
	}()

	router := dispatch.New(log, dispatch.Config{
		Workers: cfg.Workers, Retries: cfg.Retries, Backoff: retryBackoff,
	})
	router.Register(jobs.EventHeartbeat, jobs.NewHeartbeatHandler(log))
	router.Register(jobs.EventJob, jobs.NewJobHandler(log, st))
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		router.Run(runCtx, deliveries)
	}()
	shutdown := func() {
		cancel()
		consumers.Wait()
		workers.Wait()
		stopWriter()
		writer.Wait()
	}

	publisher := queue.NewProducer(cfg.AMQPURL, cfg.Topology, cfg.Seed, messageSpacing)
	if err := publisher.Run(runCtx, messageCount); err != nil && ctx.Err() == nil {
		shutdown()
		return err
	}

	<-ctx.Done()
	shutdown()

	persisted, err := st.CountJobs(context.WithoutCancel(ctx))
	if err != nil {
		return err
	}
	log.Info("shutdown complete", "jobsPersisted", persisted)
	return nil
}
