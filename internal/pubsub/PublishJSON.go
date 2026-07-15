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
