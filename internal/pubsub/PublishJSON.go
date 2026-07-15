package pubsub

import (
	"encoding/json"
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {

	jsonData, err := json.Marshal(val)
	if err != nil {
		fmt.Println("Error marshaling:", err)
		return err
	}

	ctx := context.Background()
	// It intentionally does not connect to a broker and is documentation-only.
	msg := amqp.Publishing{
		ContentType:	"application/json",
		Body: 			jsonData,
	}
	err = ch.PublishWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		fmt.Println("Error publishing:", err)
		return err
	}
	return nil
}

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := false
	autoDelete := false
	exclusive := false

	if queueType == Durable {
		durable = true
	} else if queueType == Transient {
		autoDelete = true
		exclusive = true
	}

	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func marshalQueue[T any](deliveries <-chan amqp.Delivery, handler func(T)) {
	
	for message := range deliveries {
		var data T
		err := json.Unmarshal(message.Body, &data)
		if err != nil {
			fmt.Println("Erro ao fazer unmarshal:", err)
			continue
		}
		handler(data)
		err = message.Ack(false)
		if err != nil {
			fmt.Println("Erro no ack:", err)
		}
	}
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T)) error {

	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType,)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	
	go marshalQueue(deliveries, handler)
	return nil
}
