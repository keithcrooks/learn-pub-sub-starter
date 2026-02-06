package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	conn := connectToExchange()
	defer conn.Close()

	username := promptUserForName()

	gs := gamelogic.NewGameState(username)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	subscribeToPause(username, conn, gs)
	subscribeToMoves(username, conn, gs, ch)
	subscribeToRecognitionOfWar(conn, gs, ch)

	executeCommandLoop(gs, ch)
}

func subscribeToPause(username string, conn *amqp.Connection, gs *gamelogic.GameState) {
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	if err := pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.QueueTransient,
		handlerPause(gs),
	); err != nil {
		log.Fatalf("unable to subscribe: %v", err)
	}
}

func subscribeToMoves(username string, conn *amqp.Connection, gs *gamelogic.GameState, ch *amqp.Channel) {
	key := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	queueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	if err := pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		queueName,
		key,
		pubsub.QueueTransient,
		handlerMove(gs, ch),
	); err != nil {
		log.Fatalf("unable to subscribe: %v", err)
	}
}

func subscribeToRecognitionOfWar(conn *amqp.Connection, gs *gamelogic.GameState, ch *amqp.Channel) {
	key := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	if err := pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war",
		key,
		pubsub.QueueDurable,
		handlerWar(gs, ch),
	); err != nil {
		log.Fatalf("unable to subscribe: %v", err)
	}
}

func executeCommandLoop(gs *gamelogic.GameState, ch *amqp.Channel) {
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
			am, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("could not move: %v", err)
				continue
			}

			routingKey := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, am.Player.Username)

			if err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routingKey,
				am,
			); err != nil {
				log.Printf("could not send move message: %v", err)
			}

			fmt.Println("move successful")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Println("usage: spawn n")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println("invalid number:", words[1])
				continue
			}

			for i := 0; i < n; i++ {
				msg := gamelogic.GetMaliciousLog()
				if err := publishGameLog(ch, msg, gs.GetUsername()); err != nil {
					fmt.Printf("error: %s\n", err)
					continue
				}
			}
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

	fmt.Println("Connection was successful.")
	return conn
}

func promptUserForName() string {
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Printf("error getting user name: %v", err)
	}
	return username
}
