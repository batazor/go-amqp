package amqp

import (
	"github.com/streadway/amqp"
	"time"
)

func (c *Consumer) Publish(message []byte) error {
	err := c.channel.Publish(
		c.queueName,
		c.queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Transient,
			Body:         message,
			Timestamp:    time.Now(),
		},
	)

	return err
}
