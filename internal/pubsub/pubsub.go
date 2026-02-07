package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
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

	args := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
	q, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, args)

	if err := ch.QueueBind(q.Name, key, exchange, false, nil); err != nil {
		return ch, q, err
	}

	return ch, q, nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(val); err != nil {
		return fmt.Errorf("could not encode gob: %v", err)
	}

	msg := amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
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
	jsonUnmarshaler := func(b []byte) (T, error) {
		var body T
		if err := json.Unmarshal(b, &body); err != nil {
			return body, err
		}

		return body, nil
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, jsonUnmarshaler)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	gobUnmarshaler := func(b []byte) (T, error) {
		buffer := bytes.NewBuffer(b)
		dec := gob.NewDecoder(buffer)
		var body T
		if err := dec.Decode(&body); err != nil {
			return body, err
		}

		return body, nil
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, gobUnmarshaler)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaler func([]byte) (T, error),
) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	if err := ch.Qos(10, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveries {
			body, err := unmarshaler(msg.Body)
			if err != nil {
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
