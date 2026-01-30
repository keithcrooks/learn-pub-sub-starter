package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const url = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connection was successful.")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Printf("error getting user name: %v", err)
	}

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

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Closing Peril client...")
}
