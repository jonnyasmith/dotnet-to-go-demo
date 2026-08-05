package main

import (
	"fmt"
	"math/rand/v2"
	"time"
)

const (
	eventHeartbeat = "SystemHeartbeat"
	eventJob       = "TransientJob"
)

var taskNames = []string{
	"ProcessNightshadeData",
	"ReconcileLedger",
	"PurgeStaleSessions",
}

// Producer simulates an Azure Storage Queue by pushing raw JSON onto a channel.
// The generator is a field rather than the package-level source so a test can
// pin the seed and get a byte-for-byte reproducible stream.
type Producer struct {
	rng      *rand.Rand
	interval time.Duration
}

// NewProducer builds a producer with a fixed seed and message spacing.
func NewProducer(seed uint64, interval time.Duration) *Producer {
	return &Producer{
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		interval: interval,
	}
}

// Run emits count messages, then closes out. Closing the channel is what lets
// the worker pool drain and exit, so Run owns the channel it is given.
func (p *Producer) Run(out chan<- []byte, count int) {
	defer close(out)

	for i := range count {
		out <- p.payload(i)
		if p.interval > 0 {
			time.Sleep(p.interval)
		}
	}
}

// payload returns either a heartbeat or a job envelope, chosen at random.
func (p *Producer) payload(seq int) []byte {
	if p.rng.IntN(2) == 0 {
		return fmt.Appendf(nil,
			`{"eventType":%q,"timestamp":%q}`,
			eventHeartbeat, time.Now().UTC().Format(time.RFC3339),
		)
	}

	return fmt.Appendf(nil,
		`{"eventType":%q,"eventId":%q,"payload":{"taskName":%q}}`,
		eventJob,
		fmt.Sprintf("evt-%04d-%08x", seq, p.rng.Uint32()),
		taskNames[p.rng.IntN(len(taskNames))],
	)
}
