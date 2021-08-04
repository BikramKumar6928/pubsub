package main

import (
	"github.com/emirpasic/gods/sets/hashset"
)

// *********************  Models begin  *********************

type Topic struct {
	topicId   string
	topicName string
}

func (topic Topic) New(topicId string, topicName string) Topic {
	return Topic{topicId, topicName}
}

// **********************************************************

type Subsciption struct {
	subsciptionId   string
	subsciptionName string
}

func (subsciption Subsciption) New(subsciptionId string, subsciptionName string) Subsciption {
	return Subsciption{subsciptionId, subsciptionName}
}

// **********************************************************

type Message struct {
	messageId      string
	messageContent string
}

func (message Message) New(messageId string, messageContent string) Message {
	return Message{messageId, messageContent}
}

// *********************  Models end  *********************

// *********************  Pubsub begin  *********************

type PubSub struct {
	topicIdMap                 map[string]Topic
	subsciptionMapList         map[string][]Subsciption
	messageMapList             map[string][]Message
	currentlyProcessedMessages hashset.Set
}

func (pubSub PubSub) CreateTopic() {

}

func (pubSub PubSub) DeleteTopic() {

}

func (pubSub PubSub) AddSubscription() {

}

func (pubSub PubSub) DeleteSubscription() {

}

func (pubSub PubSub) Publish() {

}

func (pubSub PubSub) Subscribe() {

}

func (pubSub PubSub) UnSubscribe() {

}

func (pubSub PubSub) Ack() {

}

// *********************  Pubsub end  *********************

// *********************  Main  *********************

func main() {
	pubSub := PubSub{}
	pubSub.CreateTopic()
}
