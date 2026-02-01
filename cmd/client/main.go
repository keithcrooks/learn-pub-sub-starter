package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	conn := connectToExchange()

	username := promptUserForName()

	createUserSubscription(username, conn)

	gs := gamelogic.NewGameState(username)

	executeCommandLoop(gs)
}

func executeCommandLoop(gs *gamelogic.GameState) {
	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		cmd := words[0]
		switch cmd {
		case "spawn":
			if err := gs.CommandSpawn(words); err != nil {
				fmt.Printf("could not spawn: %v", err)
				continue
			}
		case "move":
			_, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("could not move: %v", err)
				continue
			}

			fmt.Println("move successful")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			fmt.Println("Closing Peril server...")
			return
		default:
			fmt.Printf("Don't understand command '%s'\n", cmd)
		}
	}
}

func connectToExchange() *amqp.Connection {
	const url = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connection was successful.")
	return conn
}

func createUserSubscription(username string, conn *amqp.Connection) {
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	if _, _, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.QueueTransient,
	); err != nil {
		log.Fatalf("could not declare and bind: %v", err)
	}
}

func promptUserForName() string {
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Printf("error getting user name: %v", err)
	}
	return username
}
