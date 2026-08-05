// Package queue simulates the message source an Azure Storage Queue would be.
package queue

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jonny/go-demo/internal/jobs"
)

var taskNames = []string{
	"ProcessNightshadeData",
	"ReconcileLedger",
	"PurgeStaleSessions",
}

// Producer pushes raw JSON onto a channel.
//
// The generator is a field rather than the package-level source so a test can
// pin the seed and replay a byte-for-byte identical stream. That replay only
// holds if the clock is pinned too, which is why now is a field as well.
type Producer struct {
	rng      *rand.Rand
	interval time.Duration
	// sleep is swapped out by tests so spacing can be asserted without
	// spending the wall-clock time.
	sleep func(ctx context.Context, d time.Duration) bool
	// now is swapped out by tests so heartbeat payloads stop depending on
	// when the test happened to run.
	now func() time.Time
}

// NewProducer builds a producer with a fixed seed and message spacing.
func NewProducer(seed uint64, interval time.Duration) *Producer {
	return &Producer{
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		interval: interval,
		sleep:    sleepCtx,
		now:      time.Now,
	}
}

// Run emits up to count messages, then closes out. Closing the channel is what
// lets the worker pool drain and exit, so Run owns the channel it is given.
// A cancelled context stops production early; the channel is still closed.
func (p *Producer) Run(ctx context.Context, out chan<- []byte, count int) {
	defer close(out)

	for i := range count {
		select {
		case out <- p.payload(i):
		case <-ctx.Done():
			return
		}
		if !p.sleep(ctx, p.interval) {
			return
		}
	}
}

// payload returns either a heartbeat or a job envelope, chosen at random.
func (p *Producer) payload(seq int) []byte {
	if p.rng.IntN(2) == 0 {
		return fmt.Appendf(nil,
			`{"eventType":%q,"timestamp":%q}`,
			jobs.EventHeartbeat, p.now().UTC().Format(time.RFC3339),
		)
	}

	return fmt.Appendf(nil,
		`{"eventType":%q,"eventId":%q,"payload":{"taskName":%q}}`,
		jobs.EventJob,
		fmt.Sprintf("evt-%04d-%08x", seq, p.rng.Uint32()),
		taskNames[p.rng.IntN(len(taskNames))],
	)
}

// sleepCtx waits for d, reporting false if ctx was cancelled first.
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
