package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	QueueDurable SimpleQueueType = iota
	QueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	var durable, autoDelete, exclusive bool

	switch queueType {
	case QueueDurable:
		durable = true
		autoDelete = false
		exclusive = false
	case QueueTransient:
		durable = false
		autoDelete = true
		exclusive = true
	}

	q, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)

	if err := ch.QueueBind(q.Name, key, exchange, false, nil); err != nil {
		return ch, q, err
	}

	return ch, q, nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("could not marshal JSON value: %v", err)
	}

	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
}
