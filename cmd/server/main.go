package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
)

func main() {
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ")

	ch1, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ch1.Close()

	gamelogic.PrintServerHelp()
	ch, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, "game_logs", "game_logs.*", pubsub.Durable)
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	fmt.Println("Queue declared:", queue.Name)
	
	for {
		cmd := gamelogic.GetInput()

		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {

		case "pause":
			fmt.Println("Sending pause message")
			val := routing.PlayingState{
				IsPaused: true,
			}
			pubsub.PublishJSON(ch1, routing.ExchangePerilDirect, routing.PauseKey, val)

		case "resume":
			fmt.Println("Sending resume message")
			val := routing.PlayingState{
				IsPaused: false,
			}
			pubsub.PublishJSON(ch1, routing.ExchangePerilDirect, routing.PauseKey, val)

		case "quit":
			fmt.Println("Exiting...")
			break

		default:
			fmt.Println("I don't understand that command")
		}
	}

	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("Shutting down...")
}
