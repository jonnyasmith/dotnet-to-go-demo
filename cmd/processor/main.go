// Command processor runs the transient job queue demo: a producer feeds raw
// JSON to a pool of workers, which route each message to a handler.
package main

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jonny/go-demo/internal/dispatch"
	"github.com/jonny/go-demo/internal/jobs"
	"github.com/jonny/go-demo/internal/queue"
	"github.com/jonny/go-demo/internal/store"
)

const (
	databasePath   = "demo.db"
	workerCount    = 5
	messageCount   = 40
	messageSpacing = 25 * time.Millisecond
	queueDepth     = 16
	retries        = 2
	retryBackoff   = 20 * time.Millisecond
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	// Ctrl-C or SIGTERM cancels ctx; a second signal restores the default
	// behaviour and kills the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(databasePath)
	if err != nil {
		return err
	}
	defer st.Close()

	// The writer gets its own context: it must outlive ctx so that handlers
	// interrupted mid-insert still receive an answer rather than blocking.
	writerCtx, stopWriter := context.WithCancel(context.Background())
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		st.RunWriter(writerCtx)
	}()

	deadLetters := make(chan []byte, queueDepth)
	var reaper sync.WaitGroup
	reaper.Add(1)
	go func() {
		defer reaper.Done()
		var n int
		for msg := range deadLetters {
			n++
			log.Warn("dead letter", "payload", string(msg))
		}
		if n > 0 {
			log.Warn("dead letters recorded", "count", n)
		}
	}()

	router := dispatch.New(log, dispatch.Config{
		Workers:    workerCount,
		Retries:    retries,
		Backoff:    retryBackoff,
		DeadLetter: deadLetters,
	})
	router.Register(jobs.EventHeartbeat, jobs.NewHeartbeatHandler(log))
	router.Register(jobs.EventJob, jobs.NewJobHandler(log, st))

	messages := make(chan []byte, queueDepth)
	go queue.NewProducer(rand.Uint64(), messageSpacing).Run(ctx, messages, messageCount)

	// Blocks until the producer closes the queue and every worker returns.
	router.Run(ctx, messages)

	// Shut the stages down in dependency order: nothing can dead-letter once
	// the workers are done, and nothing can insert once they have stopped.
	close(deadLetters)
	reaper.Wait()
	stopWriter()
	writer.Wait()

	persisted, err := st.CountJobs(context.WithoutCancel(ctx))
	if err != nil {
		return err
	}
	log.Info("shutdown complete", "jobsPersisted", persisted, "database", databasePath)
	return nil
}
