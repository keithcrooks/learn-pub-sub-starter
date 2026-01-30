package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	val := routing.PlayingState{IsPaused: true}
	if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, val); err != nil {
		log.Printf("could not publish JSON: %v", err)
	}
	fmt.Println("Pause message sent!")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Closing Peril server...")
}
