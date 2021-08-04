package main

import (
	"fmt"
	"sync"

	"github.com/emirpasic/gods/sets/hashset"
)

// *********************  Models begin  *********************

type Subscription struct {
	subscriptionId       string
	topicId              string
	subscriptionFunction func(message string)
}

func (subscription Subscription) NewWithoutFunction(subscriptionId string, topicId string) Subscription {
	return Subscription{
		subscriptionId: subscriptionId,
		topicId:        topicId,
	}
}

// **********************************************************

type Message struct {
	messageId      string
	messageContent string
	subscriptionId string
}

func (message Message) New(messageId string, messageContent string, subscriptionId string) Message {
	return Message{messageId, messageContent, subscriptionId}
}

// *********************  Models end  *********************

// *********************  Pubsub begin  *********************

type PubSub struct {
	topicIdSet                 hashset.Set
	subscriptionSetMap         map[string]hashset.Set
	messageMapList             map[string][]Message
	currentlyProcessedMessages hashset.Set
	lock                       sync.Mutex
}

func (pubSub *PubSub) Initialize() {
	pubSub.topicIdSet = *hashset.New()
}

func (pubSub *PubSub) CreateTopic(topicId string) {
	pubSub.topicIdSet.Add(topicId)
	fmt.Printf("Added %v topic to system\n", topicId)
}

func (pubSub *PubSub) DeleteTopic(topicId string) {
	pubSub.topicIdSet.Remove(topicId)
	fmt.Printf("Removed %v topic from system\n", topicId)
}

// Under work
func (pubSub *PubSub) AddSubscription(topicId string, subscriptionId string) {
	newSubscription := Subscription{
		subscriptionId: subscriptionId,
		topicId:        topicId,
	}
	subscriptionSet := pubSub.subscriptionSetMap[topicId]
	if subscriptionSet.Empty() {
		subscriptionSet = *hashset.New()
		pubSub.subscriptionSetMap[topicId] = subscriptionSet
	}
	// TODO: having issue in adding objects to map
	// subscriptionSet.Add(newSubscription)
	fmt.Printf("newSubscription: %v\n", newSubscription)
	pubSub.subscriptionSetMap[topicId] = subscriptionSet
	fmt.Printf("Added %v subscription to system", subscriptionId)
}

func (pubSub *PubSub) DeleteSubscription(subscriptionId string) {
	for _, subscriptionSet := range pubSub.subscriptionSetMap {
		subscriptionSet.Remove(subscriptionId)
	}
}

// Under work
func (pubSub *PubSub) Publish(topicId string, message string) {
	subscriptionSet := pubSub.subscriptionSetMap[topicId]
	for _, subscription := range subscriptionSet.Values() {
		subscription = subscription.(Subscription)
		pubSub.lock.Lock()
		pubSub.currentlyProcessedMessages.Add(Message{
			genUUID(),
			message,
			topicId,
		})
		defer pubSub.lock.Unlock()
		// go subscription.subscriberFunc()
		fmt.Printf("subscription: %v\n", subscription)
	}
}

func (pubSub *PubSub) Retry() {
	pubSub.lock.Lock()
	defer pubSub.lock.Unlock()
	for _, message := range pubSub.currentlyProcessedMessages.Values() {
		message = message.(Message)
		// subscriptionId := message.subscriptionId
		// subscriberFunc := getSubscriptionFromSubscriptionId().subscriberFunc Create the function
		// go subscriberFunc()
	}
}

func genUUID() string {
	return "random-uuid"
}

// Under work
func (pubSub *PubSub) Subscribe(subscriptionId string, subscriberFunc func(message string)) {
	for _, subscriptionSet := range pubSub.subscriptionSetMap {
		for _, subscription := range subscriptionSet.Values() {
			subscription = subscription.(Subscription)
			// if subscription.subscriptionId == subscriptionId {
			// 	subscription.subscriberFunc = subscriberFunc
			// }
			fmt.Printf("subscription: %v\n", subscription)
		}
	}
}

// Under work
func (pubSub *PubSub) UnSubscribe(subscriptionId string) {
	for _, subscriptionSet := range pubSub.subscriptionSetMap {
		for _, subscription := range subscriptionSet.Values() {
			subscription = subscription.(Subscription)
			// if subscription.subscriptionId == subscriptionId {
			// subscriptionSet.Remove(subscription)
			// }
			fmt.Printf("subscription: %v\n", subscription)
		}
	}

}

func (pubSub *PubSub) Ack(subscriptionId string, messageId string) {
	pubSub.lock.Lock()
	pubSub.currentlyProcessedMessages.Remove(messageId)
	defer pubSub.lock.Unlock()
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
}
