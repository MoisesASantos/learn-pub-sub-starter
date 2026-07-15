package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	
	fmt.Println("Connected to RabbitMQ")

	// Canal para receber sinais do sistema operativo
	sigCh := make(chan os.Signal, 1)
	// Escuta Ctrl+C (SIGINT) e SIGTERM
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	// Espera até receber um sinal
	<-sigCh

	fmt.Println("Shutting down...")
}
