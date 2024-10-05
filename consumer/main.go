package main

import (
	"fmt"
	"log"

	"github.com/mehedimayall/kafka-go/kafkaClient"
)

func print[T any](values T) {
	fmt.Println(values)
}

func main() {
	topic := []string{"coffee_orders"}

	consumer, err := kafkaClient.CreateConsumer("order")

	if err != nil {
		log.Println(err)
		return
	}

	print("Consumer started")

	err = consumer.SubscribeTopics(topic, nil)

	msgCount := 0
	run := true

	for run == true {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			print(err)
		}
		print(string(msg.Value))
	}

	// 4. Close the consumer on exit.
	print("Processed " + string(msgCount) + " messages")
	if err := consumer.Close(); err != nil {
		log.Println(err)
	}
}
