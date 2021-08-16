package main

import (
	"fmt"
	"pubsub/models"
	"time"
)

func main() {
	topicId := "topic1"
	subscriptionId := "subscription1"

	pubSub := models.InitializePubSub()
	pubSub.CreateTopic(topicId)
	pubSub.DeleteTopic(topicId)

	pubSub.CreateTopic(topicId)
	pubSub.AddSubscription(topicId, subscriptionId)
	pubSub.Subscribe(subscriptionId, func(message string) {
		fmt.Printf("message: %v\n", message)
	})

	messageId := pubSub.Publish(topicId, "Message")

	fmt.Println("Waiting for retry to come into action")
	time.Sleep(30 * time.Second)

	fmt.Println("Acknowledged. Retry should stop")
	pubSub.Ack(subscriptionId, messageId)

	time.Sleep(30 * time.Second)
}
