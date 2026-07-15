package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

	val := routing.PlayingState{
		IsPaused:	true,
	}
	pubsub.PublishJSON(ch1, routing.ExchangePerilDirect, routing.PauseKey, val)
	// Canal para receber sinais do sistema operativo
	sigCh := make(chan os.Signal, 1)
	// Escuta Ctrl+C (SIGINT) e SIGTERM
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	// Espera até receber um sinal
	<-sigCh

	fmt.Println("Shutting down...")
}
