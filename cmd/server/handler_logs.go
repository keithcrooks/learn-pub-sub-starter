package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLogs() func(routing.GameLog) pubsub.AckType {
	return func(l routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")

		if err := gamelogic.WriteLog(l); err != nil {
			return pubsub.NackDiscard
		}

		return pubsub.Ack
	}
}
