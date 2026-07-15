package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"strings"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")
	
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}
	elements := []string{routing.PauseKey, username}
	queueName := strings.Join(elements, ".")
	ch, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)

	if err != nil {
		panic(err)
	}
	defer ch.Close()

	fmt.Println("Queue declared:", queue.Name)


	// Criar canal para sinais do sistema operativo
	sigCh := make(chan os.Signal, 1)

	// Escutar Ctrl+C e SIGTERM
	signal.Notify(
		sigCh,
		os.Interrupt,
		syscall.SIGTERM,
	)

	// Bloqueia até receber sinal
	<-sigCh

	fmt.Println("Shutting down...")
}

