package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(rw)

		winnerMessage := fmt.Sprintf("%s won a war against %s", winner, loser)
		drawMessage := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return publishGameLog(ch, winnerMessage, loser)
		case gamelogic.WarOutcomeYouWon:
			return publishGameLog(ch, winnerMessage, winner)
		case gamelogic.WarOutcomeDraw:
			return publishGameLog(ch, drawMessage, winner)
		default:
			log.Printf("unknown recognition of war outcome: %d", outcome)
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp.Channel, msg, username string) pubsub.AckType {
	gl := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    username,
	}

	key := fmt.Sprintf("%s.%s", routing.GameLogSlug, username)
	if err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, key, gl); err != nil {
		return pubsub.NackRequeue
	}

	return pubsub.Ack
}
