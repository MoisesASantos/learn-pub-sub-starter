package main

import (
	"fmt"
	"log"
	"strings"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)


func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(state routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")

		gs.HandlePause(state)

		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel, username string) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		
		outCome := gs.HandleMove(move)
		switch outCome {

			case gamelogic.MoveOutComeSafe:
				return pubsub.Ack
			    
			case gamelogic.MoveOutcomeMakeWar:
				data := gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				}
				err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+username, data)
				if err != nil {
					return pubsub.NackRequeue
				}
				return pubsub.NackRequeue

			case gamelogic.MoveOutcomeSamePlayer:
			    return pubsub.NackDiscard
			
			default:
				return pubsub.NackDiscard
		}
	}
}

func handlerWarMessage(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		
		defer fmt.Print("> ")
		
		outCome, _, _ := gs.HandleWar(rw)

		switch outCome {
			case gamelogic.WarOutcomeNotInvolved:
				return pubsub.NackRequeue
			
			case gamelogic.WarOutcomeNoUnits:
			    return pubsub.NackDiscard

			case gamelogic.WarOutcomeOpponentWon:
			    return pubsub.Ack
			
			case gamelogic.WarOutcomeYouWon:
			    return pubsub.Ack
			
			case gamelogic.WarOutcomeDraw:
			    return pubsub.Ack
			default:
				fmt.Println("Error occurred")
				return pubsub.NackDiscard
		}
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
	pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, queueName1, "army_moves.*", pubsub.Transient, handlerMove(gamestate, ch, username))
	
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "war", "war.*", pubsub.Durable, handlerWarMessage(gamestate))
	if err != nil {
		log.Fatal(err)
	}

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

