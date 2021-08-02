# LLD

This document provides details to the design and implementation of the PubSub application.

# Data Models

## Topic

* topicId:- ID of the topic
* topicName:- Name of the topic

## Subscription

* subscriptionID:- ID of the subscription made 
* subscriptionFunction:- Function to be called for processing message

## Message

* messageID:- ID of message
* messageContent:- Contents of the message

# Data to save

These data needs to be saved in order to perform various activities.

* Map of topicID and Topic
* Map of topicID and list of Subscription
* Map of topicID and list of Message
* Set of messageID that have not been acknowledged

# Design

The following will deal with the design

# CreateTopic

This will add a topic to structure 1.

# DeleteTopic

This will remove the topics from structure 1 and 2. There is an open question as to the behavior with structure 3.

# AddSubscription

This will add a Subscription to structure 2.

# DeleteSubscription

This will add a Subscription from structure 2. Similar open question as in case of DeleteTopic.

# Publish

This will add a message to structure 3. This will also trigger in parallel all the subscription functions. This will also add an entry to structure 4 in order to indicate that the message has not been acknowledged till now.

# Subscribe

This will update structure 2 to add the subscription function.

# UnSubscribe

This will update structure 2 to remove the subscription function.

# Ack
This will check and see if there is a message which has not been acknowledged in structure 4. If it finds something, it will remove the said message from there.

# Retry Mechanism

After a particular time interval in the parallel thread, this will check if the structure 4 contains the specific message or not. If found, it will retrigger the function again for a specific number of times till the message has not been acknowledged correctly.

# Open questions

1. What should be done if there are messages in the queue that have not been processed and there is a request to delete the topic?