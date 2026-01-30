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
	fmt.Println("Starting Peril server...")

	const url = "amqp://guest:guest@localhost:5672/"

	con, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer con.Close()

	fmt.Println("Connection was successful.")

	ch, err := con.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()

		if len(input) == 0 {
			continue
		}

		cmd := input[0]
		switch cmd {
		case "pause":
			log.Println("Sending a pause message...")
			val := routing.PlayingState{IsPaused: true}
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, val); err != nil {
				log.Printf("could not send pause message: %v", err)
			}
			fmt.Println("Pause message sent!")
		case "resume":
			log.Panicln("Sending a resume message...")
			val := routing.PlayingState{IsPaused: false}
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, val); err != nil {
				log.Printf("could not send resume message: %v", err)
			}
			fmt.Println("Resume message sent!")
		case "quit":
			fmt.Println("Closing Peril server...")
			return
		default:
			fmt.Printf("Done understand command '%s'\n", cmd)
		}
	}
}
