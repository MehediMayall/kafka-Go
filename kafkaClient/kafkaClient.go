package kafkaClient

import (
	"fmt"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

func connectProducer() (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers": KafkaServerAddress,
			"acks":              "all",
		},
	)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func CreateConsumer(groupId string) (*kafka.Consumer, error) {
	consumer, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": KafkaServerAddress,
			"group.id":          groupId,
			"auto.offset.reset": "smallest",
		},
	)

	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func PushIntoTheQueue(topic string, message []byte) error {

	// Create connection
	producer, err := connectProducer()
	if err != nil {
		return err
	}

	defer producer.Close()

	deliveryCh := make(chan kafka.Event, 1000)

	// Send message
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: message,
	}, deliveryCh)

	if err != nil {
		return err
	}

	e := <-deliveryCh
	fmt.Printf("%v", e)
	return nil
}
