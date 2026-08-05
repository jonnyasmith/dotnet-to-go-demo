package e2e_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/jonny/go-demo/internal/dispatch"
	"github.com/jonny/go-demo/internal/queue"
)

var rabbitURL string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	container, err := rabbitmq.Run(ctx, "rabbitmq:4-management-alpine")
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting RabbitMQ test container:", err)
		os.Exit(1)
	}
	rabbitURL, err = container.AmqpURL(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "getting RabbitMQ URL:", err)
		_ = testcontainers.TerminateContainer(container)
		os.Exit(1)
	}
	code := m.Run()
	if testcontainers.TerminateContainer(container) != nil {
		code = 1
	}
	os.Exit(code)
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var topologySequence atomic.Int64

func testTopology() queue.Topology {
	id := topologySequence.Add(1)
	return queue.Topology{
		JobsQueue:          fmt.Sprintf("e2e-jobs-%d", id),
		DeadLetterExchange: fmt.Sprintf("e2e-dead-%d", id),
		DeadLetterQueue:    fmt.Sprintf("e2e-dead-%d", id),
	}
}

func publish(t *testing.T, topology queue.Topology, payload []byte) {
	t.Helper()
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := queue.DeclareTopology(ch, topology); err != nil {
		t.Fatal(err)
	}
	if err := ch.PublishWithContext(t.Context(), "", topology.JobsQueue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", Body: payload,
	}); err != nil {
		t.Fatal(err)
	}
}

func startPipeline(t *testing.T, ctx context.Context, topology queue.Topology, router *dispatch.Router) <-chan struct{} {
	t.Helper()
	deliveries := make(chan dispatch.Delivery, 4)
	done := make(chan struct{})
	go queue.Consumer{URL: rabbitURL, Topology: topology, Prefetch: 1, Log: discardLogger()}.Run(ctx, deliveries)
	go func() {
		defer close(done)
		router.Run(ctx, deliveries)
	}()
	return done
}

func awaitDeadLetter(t *testing.T, topology queue.Topology, want []byte) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := amqp.Dial(rabbitURL)
		if err != nil {
			t.Fatal(err)
		}
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		delivery, ok, err := ch.Get(topology.DeadLetterQueue, true)
		ch.Close()
		conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if string(delivery.Body) != string(want) {
				t.Errorf("dead-letter payload = %s, want %s", delivery.Body, want)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("message did not reach the RabbitMQ dead-letter queue")
}

func TestRetryExhaustionRoutesToBrokerDeadLetterQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping RabbitMQ-backed end-to-end test in short mode")
	}
	t.Parallel()
	topology := testTopology()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int64
	router := dispatch.New(discardLogger(), dispatch.Config{Workers: 1, Retries: 2, Backoff: time.Millisecond})
	router.Register("AlwaysFails", func(context.Context, []byte) error {
		attempts.Add(1)
		return errors.New("downstream unavailable")
	})
	done := startPipeline(t, ctx, topology, router)
	payload := []byte(`{"eventType":"AlwaysFails","eventId":"fail-1"}`)
	publish(t, topology, payload)
	awaitDeadLetter(t, topology, payload)
	if got := attempts.Load(); got != 3 {
		t.Errorf("handler attempts = %d, want 3", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not stop")
	}
}

func TestPermanentFailureDeadLettersWithoutRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping RabbitMQ-backed end-to-end test in short mode")
	}
	t.Parallel()
	topology := testTopology()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int64
	router := dispatch.New(discardLogger(), dispatch.Config{Workers: 1, Retries: 5, Backoff: time.Minute})
	router.Register("Poison", func(context.Context, []byte) error {
		attempts.Add(1)
		return dispatch.Permanent(errors.New("invalid payload"))
	})
	done := startPipeline(t, ctx, topology, router)
	payload := []byte(`{"eventType":"Poison"}`)
	publish(t, topology, payload)
	awaitDeadLetter(t, topology, payload)
	if got := attempts.Load(); got != 1 {
		t.Errorf("handler attempts = %d, want 1", got)
	}
	cancel()
	<-done
}

func TestCancellationRequeuesInFlightDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping RabbitMQ-backed end-to-end test in short mode")
	}
	t.Parallel()
	topology := testTopology()
	firstCtx, stopFirst := context.WithCancel(context.Background())
	entered := make(chan struct{})
	firstRouter := dispatch.New(discardLogger(), dispatch.Config{Workers: 1})
	firstRouter.Register("Work", func(ctx context.Context, _ []byte) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	firstDone := startPipeline(t, firstCtx, topology, firstRouter)
	publish(t, topology, []byte(`{"eventType":"Work","eventId":"requeue-1"}`))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first handler did not receive the message")
	}
	stopFirst()
	<-firstDone

	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	received := make(chan struct{})
	secondRouter := dispatch.New(discardLogger(), dispatch.Config{Workers: 1})
	secondRouter.Register("Work", func(context.Context, []byte) error {
		close(received)
		return nil
	})
	secondDone := startPipeline(t, secondCtx, topology, secondRouter)
	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("requeued message was not delivered to the next run")
	}
	stopSecond()
	<-secondDone
}
