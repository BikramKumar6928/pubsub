package models

// *********************  Models begin  *********************

type Topic struct {
	TopicId            string
	SubscriptionIdList []string
}

func (topic *Topic) DeleteSubscription(subscriptionIdToDelete string) {
	indexToDelete := -1
	subscriptionIdList := topic.SubscriptionIdList

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
	topic.SubscriptionIdList = subscriptionIdList[:len(subscriptionIdList)-1]
}

type Subscription struct {
	SubscriptionId       string
	TopicId              string
	SubscriptionFunction func(message string)
}

type Message struct {
	MessageId      string
	MessageContent string
	TopicId        string
}

// *********************  Models end  *********************

// *********************  Pubsub begin  *********************
