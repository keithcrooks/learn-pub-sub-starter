package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	QueueDurable SimpleQueueType = iota
	QueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveries {
			var body T
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				log.Printf("could not unmarshal message body: %v", err)
				continue
			}

			ackType := handler(body)

			switch ackType {
			case Ack:
				if err := msg.Ack(false); err != nil {
					log.Printf("could not acknowledge message: %v", err)
				}
				log.Println("message acknowledged")
			case NackRequeue:
				if err := msg.Nack(false, true); err != nil {
					log.Printf("could not requeue message: %v", err)
				}
				log.Println("message requeued")
			case NackDiscard:
				if err := msg.Nack(false, false); err != nil {
					log.Printf("could not discard message: %v", err)
				}
				log.Println("message discarded")
			}
		}
	}()

	return nil
}
