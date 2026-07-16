package pubsub

import (
	"encoding/json"
	"encoding/gob"
	"context"
	"fmt"
	"bytes"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {

	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	ctx := context.Background()
	msg := amqp.Publishing{
		ContentType: "application/gob",
		Body:        data.Bytes(),
	}

	err = ch.PublishWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		fmt.Println("Error publishing:", err)
		return err
	}
	return nil
}


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

	headers:= amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, headers)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func marshalQueue[T any](deliveries <-chan amqp.Delivery, handler func(T) Acktype) {
	
	for message := range deliveries {
		
		var data T
		err := json.Unmarshal(message.Body, &data)
		if err != nil {
			fmt.Println("Erro ao fazer unmarshal:", err)
			continue
		}
		//call the handler action
		result := handler(data)
		switch result {

			case Ack:
			    err = message.Ack(false)
				fmt.Println("Ack action occurred")
			
			case NackRequeue:
			    err = message.Nack(false, true)
				fmt.Println("Nack requeue action occurred")
			
			case NackDiscard:
			    err = message.Nack(false, false)
				fmt.Println("Nack delete action occurred")
		}
		if err != nil {
			fmt.Println("Erro no ack:", err)
		}
	}
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype) error {

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


