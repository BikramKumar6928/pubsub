package main

import (
	"fmt"
	"pubsub/models"
	"sync"
	"time"

	"github.com/google/uuid"
)

type PubSub struct {
	topicMap                   map[string]models.Topic
	subscriptionMap            map[string]models.Subscription
	messageMap                 map[string]models.Message
	currentlyProcessedMessages map[string][]string
	lock                       sync.Mutex
}

func (pubsub *PubSub) deleteSubscriptionFromCurrentlyProcessed(subscriptionIdToDelete string, messageId string) {
	indexToDelete := -1
	listToDeleteFrom := pubsub.currentlyProcessedMessages[messageId]

	for index, subscriptionId := range listToDeleteFrom {
		if subscriptionId == subscriptionIdToDelete {
			indexToDelete = index
			break
		}
	}
	if indexToDelete == -1 {
		return
	}
	listToDeleteFrom[indexToDelete] = listToDeleteFrom[len(listToDeleteFrom)-1]
	pubsub.currentlyProcessedMessages[messageId] = listToDeleteFrom[:len(listToDeleteFrom)-1]
}

func (pubsub *PubSub) Initialize() PubSub {
	pubsub.topicMap = make(map[string]models.Topic)
	pubsub.subscriptionMap = make(map[string]models.Subscription)
	pubsub.messageMap = make(map[string]models.Message)
	pubsub.currentlyProcessedMessages = make(map[string][]string)
	return *pubsub
}

func (pubSub *PubSub) CreateTopic(topicId string) {
	topic := models.Topic{
		TopicId: topicId,
	}
	pubSub.topicMap[topicId] = topic
	fmt.Printf("Added %v topic to system\n", topicId)
}

func (pubSub *PubSub) DeleteTopic(topicId string) {
	delete(pubSub.topicMap, topicId)
	fmt.Printf("Removed %v topic from system\n", topicId)
}

// Under work
func (pubSub *PubSub) AddSubscription(topicId string, subscriptionId string) {
	newSubscription := models.Subscription{
		SubscriptionId: subscriptionId,
		TopicId:        topicId,
	}

	topicStruct := pubSub.topicMap[topicId]
	topicStruct.SubscriptionIdList = append(topicStruct.SubscriptionIdList, subscriptionId)
	pubSub.topicMap[topicId] = topicStruct

	pubSub.subscriptionMap[subscriptionId] = newSubscription

	fmt.Printf("newSubscription: %v\n", newSubscription)
	fmt.Printf("Added %v subscription to system \n", subscriptionId)
}

func (pubSub *PubSub) DeleteSubscription(subscriptionId string) {
	subscriptionMap := pubSub.subscriptionMap
	topicId := subscriptionMap[subscriptionId].TopicId
	topic := pubSub.topicMap[topicId]
	topic.DeleteSubscription(subscriptionId)
	delete(subscriptionMap, subscriptionId)
}

// Under work
func (pubSub *PubSub) Publish(topicId string, message string) {
	topic := pubSub.topicMap[topicId]
	messageId := genUUID()
	messageStruct := models.Message{
		MessageId:      messageId,
		MessageContent: message,
		TopicId:        topicId,
	}
	pubSub.messageMap[messageId] = messageStruct
	pubSub.currentlyProcessedMessages[messageId] = make([]string, 0)

	for _, subscriptionId := range topic.SubscriptionIdList {
		subscription := pubSub.subscriptionMap[subscriptionId]
		pubSub.lock.Lock()
		defer pubSub.lock.Unlock()
		pubSub.currentlyProcessedMessages[messageId] = append(pubSub.currentlyProcessedMessages[messageId], subscriptionId)
		go subscription.SubscriptionFunction(message)
	}
}

func (pubSub *PubSub) Retry() {
	for {
		pubSub.lock.Lock()
		for messageId, subscriptionIdList := range pubSub.currentlyProcessedMessages {
			for _, subscriptionId := range subscriptionIdList {
				subscription := pubSub.subscriptionMap[subscriptionId]
				message := pubSub.messageMap[messageId]
				go subscription.SubscriptionFunction(message.MessageContent)
			}
		}
		pubSub.lock.Unlock()
		time.Sleep(10 * time.Second)
	}
}

func genUUID() string {
	return uuid.NewString()
}

// Under work
func (pubSub *PubSub) Subscribe(subscriptionId string, subscriberFunc func(message string)) {
	subscriber := pubSub.subscriptionMap[subscriptionId]
	subscriber.SubscriptionFunction = subscriberFunc
	pubSub.subscriptionMap[subscriptionId] = subscriber
}

// Under work
func (pubSub *PubSub) UnSubscribe(subscriptionId string) {
	pubSub.DeleteSubscription(subscriptionId)
}

func (pubSub *PubSub) Ack(subscriptionId string, messageId string) {
	pubSub.lock.Lock()
	defer pubSub.lock.Unlock()
	pubSub.deleteSubscriptionFromCurrentlyProcessed(subscriptionId, messageId)
}

// *********************  Pubsub end  *********************

// *********************  Main ****************************

func main() {
	pubSub := PubSub{}
	pubSub.Initialize()
	pubSub.CreateTopic("topic1")
	pubSub.DeleteTopic("topic1")

	pubSub.CreateTopic("topic1")
	pubSub.AddSubscription("topic1", "subscription1")

	pubSub.Subscribe("subscription1", func(message string) {
		fmt.Printf("message: %v\n", message)
	})

	pubSub.Publish("topic1", "Message")
	go pubSub.Retry()

	time.Sleep(30 * time.Second)
}
