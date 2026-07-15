package main

import (
	"fmt"
	"strings"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)


func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(state routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(state)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}
}

func main() {
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ")
	
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ch.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}
	gamestate := gamelogic.NewGameState(username)

	elements := []string{routing.PauseKey, username}
	queueName := strings.Join(elements, ".")
	pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient, handlerPause(gamestate))

	elements1 := []string{routing.ArmyMovesPrefix, username}
	queueName1 := strings.Join(elements1, ".")
	pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, queueName1, "army_moves.*", pubsub.Transient, handlerMove(gamestate))
	
	running := true
	for running  {
		cmd := gamelogic.GetInput()

		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {

		case "spawn":
			_ = gamestate.CommandSpawn(cmd)

		case "move":
			move, err := gamestate.CommandMove(cmd)
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, move)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("the move was published successfully")
		
		case "status":
			gamestate.CommandStatus() 
		
		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			running = false

		default:
			fmt.Println("I don't understand that command")
		}
	}
}

