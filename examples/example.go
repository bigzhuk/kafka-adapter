package main

import (
	"log"

	queue "gitlab.goodsteam.tech/moklyakov/kafka-adapter"
)

func main() {
	example()
}

func example() {
	topic := "my_topic"
	consumerGroup := "my_group"
	broker := "kafka-01-croc.test.lan:9092"
	messageToSend := []byte("some message")

	cfg := queue.KafkaCfg{
		Concurrency:       100,
		QueueToReadNames:  []string{topic},
		QueueToWriteNames: []string{topic},
		Brokers:           []string{broker},
		ConsumerGroupID:   consumerGroup,
		DefaultTopicConfig: struct {
			NumPartitions     int
			ReplicationFactor int
		}{NumPartitions: 1, ReplicationFactor: 1},
	}

	log.Printf("starting adapter")
	q, err := queue.FromStruct(cfg, queue.DefaultLogger)
	if err != nil {
		log.Fatalf("cant init kafka adapter: %v", err)
	}
	defer q.Close()
	log.Printf("adapter started")

	log.Printf("putting message")
	err = q.Put(topic, messageToSend)
	if err != nil {
		log.Fatalf("cant put message in topic: %v", err)
	}
	log.Printf("message put")

	log.Printf("getting message")
	msg, err := q.Get(topic)
	if err != nil {
		log.Fatalf("cant get message from topic: %v", err)
	}
	message := string(msg.Data())
	log.Printf("message got: %v", message)

	log.Printf("acking message")
	err = msg.Ack()
	if err != nil {
		log.Fatalf("cant ack message: %v", err)
	}
	log.Printf("message acked")
	q.Close()
	log.Printf("adapter closed")
}
