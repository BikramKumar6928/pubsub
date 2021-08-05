package main

import (
	"fmt"
	"sync"
)

// *********************  Models begin  *********************

type Topic struct {
	topicId            string
	subscriptionIdList []string
}

func (topic *Topic) deleteSubscription(subscriptionIdToDelete string) {
	indexToDelete := -1
	subscriptionIdList := topic.subscriptionIdList

	for index, subscriptionId := range subscriptionIdList {
		if subscriptionId == subscriptionIdToDelete {
			indexToDelete = index
			break
		}
	}
	if indexToDelete == -1 {
		return
	}
	subscriptionIdList[indexToDelete] = subscriptionIdList[len(subscriptionIdList)-1]
	topic.subscriptionIdList = subscriptionIdList[:len(subscriptionIdList)-1]
}

type Subscription struct {
	subscriptionId       string
	topicId              string
	subscriptionFunction func(message string)
}

type Message struct {
	messageId      string
	messageContent string
	topicId        string
}

// *********************  Models end  *********************

// *********************  Pubsub begin  *********************

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

func (pubsub *PubSub) Initialize() PubSub {
	pubsub.topicMap = make(map[string]Topic)
	pubsub.subscriptionMap = make(map[string]Subscription)
	pubsub.messageMap = make(map[string]Message)
	pubsub.currentlyProcessedMessages = make(map[string][]string)
	return *pubsub
}

func (pubSub *PubSub) CreateTopic(topicId string) {
	topic := Topic{
		topicId: topicId,
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
	newSubscription := Subscription{
		subscriptionId: subscriptionId,
		topicId:        topicId,
	}

	topicStruct := pubSub.topicMap[topicId]
	topicStruct.subscriptionIdList = append(topicStruct.subscriptionIdList, subscriptionId)
	pubSub.subscriptionMap[subscriptionId] = newSubscription

	fmt.Printf("newSubscription: %v\n", newSubscription)
	fmt.Printf("Added %v subscription to system", subscriptionId)
}

func (pubSub *PubSub) DeleteSubscription(subscriptionId string) {
	subscriptionMap := pubSub.subscriptionMap
	topicId := subscriptionMap[subscriptionId].topicId
	topic := pubSub.topicMap[topicId]
	topic.deleteSubscription(subscriptionId)
	delete(subscriptionMap, subscriptionId)
}

// Under work
func (pubSub *PubSub) Publish(topicId string, message string) {
	topic := pubSub.topicMap[topicId]
	messageId := genUUID()
	messageStruct := Message{
		messageId:      messageId,
		messageContent: message,
		topicId:        topicId,
	}
	pubSub.messageMap[messageId] = messageStruct
	pubSub.currentlyProcessedMessages[messageId] = make([]string, 100)

	for _, subscriptionId := range topic.subscriptionIdList {
		subscription := pubSub.subscriptionMap[subscriptionId]
		pubSub.lock.Lock()
		defer pubSub.lock.Unlock()
		pubSub.currentlyProcessedMessages[messageId] = append(pubSub.currentlyProcessedMessages[messageId], subscriptionId)
		go subscription.subscriptionFunction(message)
	}
}

func (pubSub *PubSub) Retry() {
	pubSub.lock.Lock()
	defer pubSub.lock.Unlock()
	for messageId, subscriptionIdList := range pubSub.currentlyProcessedMessages {
		for _, subscriptionId := range subscriptionIdList {
			subscription := pubSub.subscriptionMap[subscriptionId]
			message := pubSub.messageMap[messageId]
			go subscription.subscriptionFunction(message.messageContent)
		}
	}
	// sleep for some time
}

func genUUID() string {
	return "random-uuid"
}

// Under work
func (pubSub *PubSub) Subscribe(subscriptionId string, subscriberFunc func(message string)) {
	subscriber := pubSub.subscriptionMap[subscriptionId]
	subscriber.subscriptionFunction = subscriberFunc
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
	fmt.Println(pubSub)
	pubSub.DeleteTopic("topic1")
	fmt.Println(pubSub)

	pubSub.CreateTopic("topic1")
	fmt.Println(pubSub)
	pubSub.AddSubscription("topic1", "subscription1")
	fmt.Println(pubSub)

	pubSub.Subscribe("subscription1", func(message string) {
		fmt.Printf("message: %v\n", message)
	})

	pubSub.Publish("topic1", "Message")
}
