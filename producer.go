package sarama

import (
	"errors"

	"github.com/Shopify/sarama"
)

type SyncProducer struct {
	sarama *Sarama

	topic string

	producer sarama.SyncProducer
}

func (s *Sarama) NewSyncProducer(
	clientId string,
	topic string,
	ack AckType,
) (SyncProducerInterface, error) {
	result := &SyncProducer{
		sarama: s,
		topic:  topic,
	}

	cfg, err := s.getCommonConfig(clientId)
	if err != nil {
		return nil, err
	}

	cfg.Producer.Return.Successes = true

	cfg.Producer.RequiredAcks = sarama.RequiredAcks(ack)

	result.producer, err = sarama.NewSyncProducer(s.config.Brokers, cfg)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (o *SyncProducer) Send(key string, value []byte) error {
	if o.topic == "" {
		return errors.New("topic_has_not_set")
	}

	return o.SendTopic(o.topic, key, value)
}

func (o *SyncProducer) SendTopic(topic string, key string, value []byte) error {
	_, _, err := o.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	})

	return err
}

func (o *SyncProducer) Stop() {
	_ = o.producer.Close()
}
