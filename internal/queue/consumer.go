package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/jonny/go-demo/internal/dispatch"
)

type brokerDelivery struct{ delivery amqp.Delivery }

func (d *brokerDelivery) Body() []byte            { return d.delivery.Body }
func (d *brokerDelivery) Ack() error              { return d.delivery.Ack(false) }
func (d *brokerDelivery) Nack(requeue bool) error { return d.delivery.Nack(false, requeue) }

// Consumer reconnects with capped exponential backoff and redeclares topology
// before each consume attempt.
type Consumer struct {
	URL      string
	Topology Topology
	Prefetch int
	Log      *slog.Logger
	// DrainDeadLetters enables the operator-facing consumer that logs and
	// acknowledges messages routed to the dead-letter queue.
	DrainDeadLetters bool
	MinBackoff       time.Duration
	MaxBackoff       time.Duration
}

func (c Consumer) Run(ctx context.Context, out chan<- dispatch.Delivery) {
	defer close(out)
	backoff := c.MinBackoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}
	maxBackoff := c.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Second
	}

	for ctx.Err() == nil {
		err := c.consumeOnce(ctx, out)
		if ctx.Err() != nil {
			return
		}
		c.Log.Warn("RabbitMQ consumer disconnected", "error", err, "reconnectIn", backoff)
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (c Consumer) consumeOnce(ctx context.Context, out chan<- dispatch.Delivery) error {
	conn, err := amqp.Dial(c.URL)
	if err != nil {
		return fmt.Errorf("connecting to RabbitMQ: %w", err)
	}
	defer conn.Close()

	jobsChannel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("opening jobs channel: %w", err)
	}
	defer jobsChannel.Close()
	if err := DeclareTopology(jobsChannel, c.Topology); err != nil {
		return err
	}
	if err := jobsChannel.Qos(max(c.Prefetch, 1), 0, false); err != nil {
		return fmt.Errorf("setting prefetch: %w", err)
	}
	jobs, err := jobsChannel.Consume(c.Topology.JobsQueue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consuming jobs: %w", err)
	}

	var dead <-chan amqp.Delivery
	if c.DrainDeadLetters {
		deadChannel, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("opening dead-letter channel: %w", err)
		}
		defer deadChannel.Close()
		dead, err = deadChannel.Consume(c.Topology.DeadLetterQueue, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("consuming dead letters: %w", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, open := <-jobs:
			if !open {
				return errors.New("jobs delivery stream closed")
			}
			select {
			case out <- &brokerDelivery{delivery: delivery}:
			case <-ctx.Done():
				_ = delivery.Nack(false, true)
				return ctx.Err()
			}
		case delivery, open := <-dead:
			if !open {
				return errors.New("dead-letter delivery stream closed")
			}
			c.Log.Warn("dead letter", "payload", string(delivery.Body))
			if err := delivery.Ack(false); err != nil {
				c.Log.Error("acknowledging dead letter", "error", err)
			}
		}
	}
}
