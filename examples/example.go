package main

import (
	"log"

	queue "gitlab.goodsteam.tech/moklyakov/kafka-adapter"
)

func main() {
	example()
}

func example() {
	//topic := "malibu-api.ml-service.ml-moderation-images"
	topic := "ml-service.malibu-api-pim-import-result-consumer.ml-moderation-images-result"
	consumerGroup := "malibu-api.ml-service.ml-moderation-images"
	broker := []string{"automoderation-kafka-01.test.cloud.sber-msk-az1.goods.local:9092", "automoderation-kafka-02.test.cloud.sber-msk-az1.goods.local:9092", "automoderation-kafka-03.test.cloud.sber-msk-az1.goods.local:9092"}
	messageToSend := []byte("some message")
	defaultTopicConfig := queue.TopicConfig{NumPartitions: 1, ReplicationFactor: 1}
	authSASLConfig := queue.AuthSASLConfig{User: "admin", Password: "Qmm3Dbrg6Kq6FPTYShgxKi2n"}
	cfg := queue.KafkaCfg{
		Concurrency:        100,
		QueueToReadNames:   []string{topic},
		QueueToWriteNames:  []string{topic},
		Brokers:            broker,
		ConsumerGroupID:    consumerGroup,
		DefaultTopicConfig: defaultTopicConfig,
		AuthSASLConfig:     authSASLConfig,
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
