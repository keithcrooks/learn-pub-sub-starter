package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Closing Peril server...")
}
