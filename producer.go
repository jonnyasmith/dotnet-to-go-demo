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

// produce simulates an Azure Storage Queue by pushing raw JSON onto out.
// It emits count messages at the given interval, then closes the channel,
// which is what lets the worker pool drain and exit cleanly.
func produce(out chan<- []byte, count int, interval time.Duration) {
	defer close(out)

	for i := range count {
		out <- randomPayload(i)
		time.Sleep(interval)
	}
}

// randomPayload returns either a heartbeat or a job envelope, chosen at random.
func randomPayload(seq int) []byte {
	if rand.IntN(2) == 0 {
		return []byte(fmt.Sprintf(
			`{"eventType":%q,"timestamp":%q}`,
			eventHeartbeat, time.Now().UTC().Format(time.RFC3339),
		))
	}

	return []byte(fmt.Sprintf(
		`{"eventType":%q,"eventId":%q,"payload":{"taskName":%q}}`,
		eventJob,
		fmt.Sprintf("evt-%04d-%08x", seq, rand.Uint32()),
		taskNames[rand.IntN(len(taskNames))],
	))
}
