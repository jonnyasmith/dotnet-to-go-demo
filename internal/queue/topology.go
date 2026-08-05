package queue

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Topology names the durable queues and dead-letter exchange used by a run.
// Tests use unique names while production uses DefaultTopology.
type Topology struct {
	JobsQueue          string
	DeadLetterExchange string
	DeadLetterQueue    string
}

var DefaultTopology = Topology{
	JobsQueue:          "transient-jobs",
	DeadLetterExchange: "transient-jobs.dead",
	DeadLetterQueue:    "transient-jobs.dead",
}

// DeclareTopology idempotently creates every broker definition needed by the
// publisher and consumers. It is called on every connection.
func DeclareTopology(ch *amqp.Channel, topology Topology) error {
	if err := ch.ExchangeDeclare(topology.DeadLetterExchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring dead-letter exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(topology.DeadLetterQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring dead-letter queue: %w", err)
	}
	if err := ch.QueueBind(topology.DeadLetterQueue, "", topology.DeadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("binding dead-letter queue: %w", err)
	}
	args := amqp.Table{"x-dead-letter-exchange": topology.DeadLetterExchange}
	if _, err := ch.QueueDeclare(topology.JobsQueue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declaring jobs queue: %w", err)
	}
	return nil
}
