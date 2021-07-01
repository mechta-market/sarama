package sarama

import "time"

type Interface interface {
	NewConsumerGroup(
		clientId string,
		group string,
		topics []string,
		msgHandler func(topic string, offset int64, msg []byte) bool,
		commitHandler func(topic string) bool,
		commitDuration time.Duration,
		skipUnread bool,
	) (ConsumerGroupInterface, error)

	NewSyncProducer(
		clientId string,
		topic string,
		ack AckType, // '0': not wait, '1': wait for write to disk, '-1': wait for saving on all replicas
	) (SyncProducerInterface, error)

	Wait()
}

type ConsumerGroupInterface interface {
	Stop()
}

type SyncProducerInterface interface {
	Send(key string, value []byte) error
	SendTopic(topic string, key string, value []byte) error
	Stop()
}
