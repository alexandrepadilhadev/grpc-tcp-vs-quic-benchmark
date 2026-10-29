// Package broker is the AMQP path between the Core and the Risk service.
package broker

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

const (
	EventsQueue  = "payment.events"
	ResultsQueue = "risk.results"
)

// Broker is one AMQP connection with a publish and a consume channel.
type Broker struct {
	conn     *amqp.Connection
	pub, sub *amqp.Channel
}

// Dial connects and declares both queues. They are durable because RabbitMQ 4
// deprecates transient non-exclusive queues; messages stay transient (no disk).
func Dial(url string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	b := &Broker{conn: conn}
	if b.pub, err = conn.Channel(); err == nil {
		b.sub, err = conn.Channel()
	}
	for _, q := range []string{EventsQueue, ResultsQueue} {
		if err == nil {
			_, err = b.pub.QueueDeclare(q, true, false, false, false, nil)
		}
	}
	if err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return b, nil
}

// Publish sends m as a transient protobuf message to queue.
func (b *Broker) Publish(ctx context.Context, queue string, m proto.Message) error {
	body, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return b.pub.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType: "application/x-protobuf", DeliveryMode: amqp.Transient, Body: body})
}

// Consume delivers message bodies from queue with auto-ack; the channel is
// closed when the AMQP channel or connection drops.
func (b *Broker) Consume(queue string, prefetch int) (<-chan []byte, error) {
	if err := b.sub.Qos(prefetch, 0, false); err != nil {
		return nil, err
	}
	d, err := b.sub.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	out := make(chan []byte, prefetch)
	go func() {
		defer close(out)
		for m := range d {
			out <- m.Body
		}
	}()
	return out, nil
}

func (b *Broker) Close() error { return b.conn.Close() }
