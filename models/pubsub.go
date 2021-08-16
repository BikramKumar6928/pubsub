package models

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type PubSub struct {
	topicMap                   map[string]Topic
	subscriptionMap            map[string]Subscription
	messageMap                 map[string]Message
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

func InitializePubSub() *PubSub {
	pubsub := PubSub{}
	pubsub.topicMap = make(map[string]Topic)
	pubsub.subscriptionMap = make(map[string]Subscription)
	pubsub.messageMap = make(map[string]Message)
	pubsub.currentlyProcessedMessages = make(map[string][]string)
	go pubsub.Retry()
	return &pubsub
}

func (pubSub *PubSub) CreateTopic(topicId string) {
	topic := Topic{
		TopicId: topicId,
	}
	pubSub.topicMap[topicId] = topic
	fmt.Printf("Added %v topic to system\n", topicId)
}

func (pubSub *PubSub) DeleteTopic(topicId string) {
	delete(pubSub.topicMap, topicId)
	fmt.Printf("Removed %v topic from system\n", topicId)
}

func (pubSub *PubSub) AddSubscription(topicId string, subscriptionId string) {
	newSubscription := Subscription{
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

func (pubSub *PubSub) Publish(topicId string, message string) string {
	topic := pubSub.topicMap[topicId]
	messageId := genUUID()
	messageStruct := Message{
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
	return messageId
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

func (pubSub *PubSub) Subscribe(subscriptionId string, subscriberFunc func(message string)) {
	subscriber := pubSub.subscriptionMap[subscriptionId]
	subscriber.SubscriptionFunction = subscriberFunc
	pubSub.subscriptionMap[subscriptionId] = subscriber
}

func (pubSub *PubSub) UnSubscribe(subscriptionId string) {
	pubSub.DeleteSubscription(subscriptionId)
}

func (pubSub *PubSub) Ack(subscriptionId string, messageId string) {
	pubSub.lock.Lock()
	defer pubSub.lock.Unlock()
	pubSub.deleteSubscriptionFromCurrentlyProcessed(subscriptionId, messageId)
}

// *********************  Pubsub end  *********************
