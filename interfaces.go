package sarama

type Interface interface {
	NewConsumerGroup(
		clientId string,
		group string,
		topics []string,
		handler func(topic string, offset int64, msg []byte) bool,
		skipUnread bool,
	) (ConsumerGroupInterface, error)

	NewSyncProducer(
		clientId string,
		topic string,
		ack int, // '0': not wait, '1': wait for write to disk, '-1': wait for saving on all replicas
	) (SyncProducerInterface, error)

	Wait()
}

type ConsumerGroupInterface interface {
	SetOffset(offset int64)
	Stop()
}

type SyncProducerInterface interface {
	Send(key string, value []byte) error
	SendTopic(topic string, key string, value []byte) error
}
