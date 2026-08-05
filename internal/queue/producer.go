// Package queue connects the processor to RabbitMQ and publishes the demo's
// seed messages.
package queue

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/jonny/go-demo/internal/jobs"
)

var taskNames = []string{"ProcessNightshadeData", "ReconcileLedger", "PurgeStaleSessions"}

// Producer creates deterministic demo payloads and publishes them persistently
// with publisher confirms.
type Producer struct {
	URL      string
	Topology Topology
	rng      *rand.Rand
	interval time.Duration
	sleep    func(context.Context, time.Duration) bool
	now      func() time.Time
}

func NewProducer(url string, topology Topology, seed uint64, interval time.Duration) *Producer {
	return &Producer{
		URL: url, Topology: topology,
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		interval: interval, sleep: sleepCtx, now: time.Now,
	}
}

// Payloads returns the next count messages without publishing them. Tests use
// a second producer with the same seed to derive independent expectations.
func (p *Producer) Payloads(count int) [][]byte {
	payloads := make([][]byte, count)
	for i := range count {
		payloads[i] = p.payload(i)
	}
	return payloads
}

func (p *Producer) Run(ctx context.Context, count int) error {
	conn, err := amqp.Dial(p.URL)
	if err != nil {
		return fmt.Errorf("connecting publisher to RabbitMQ: %w", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("opening publisher channel: %w", err)
	}
	defer ch.Close()
	if err := DeclareTopology(ch, p.Topology); err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enabling publisher confirms: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	for i := range count {
		if err := ch.PublishWithContext(ctx, "", p.Topology.JobsQueue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         p.payload(i),
		}); err != nil {
			return fmt.Errorf("publishing message %d: %w", i, err)
		}
		select {
		case confirmation := <-confirms:
			if !confirmation.Ack {
				return fmt.Errorf("publishing message %d: broker negatively acknowledged it", i)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		if i < count-1 && !p.sleep(ctx, p.interval) {
			return ctx.Err()
		}
	}
	return nil
}

func (p *Producer) payload(seq int) []byte {
	if p.rng.IntN(2) == 0 {
		return fmt.Appendf(nil, `{"eventType":%q,"timestamp":%q}`,
			jobs.EventHeartbeat, p.now().UTC().Format(time.RFC3339))
	}
	return fmt.Appendf(nil, `{"eventType":%q,"eventId":%q,"payload":{"taskName":%q}}`,
		jobs.EventJob, fmt.Sprintf("evt-%04d-%08x", seq, p.rng.Uint32()),
		taskNames[p.rng.IntN(len(taskNames))])
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
