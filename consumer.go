package sarama

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

type ConsumerGroup struct {
	sarama *Sarama

	ctx    context.Context
	cancel context.CancelFunc

	handler func(topic string, offset int64, msg []byte) bool

	offsetCh chan int64
}

func (s *Sarama) NewConsumerGroup(
	clientId string,
	group string,
	topics []string,
	handler func(topic string, offset int64, msg []byte) bool,
	skipUnread bool,
) (*ConsumerGroup, error) {
	result := &ConsumerGroup{
		sarama: s,
	}

	cfg, err := s.getCommonConfig(clientId)
	if err != nil {
		return nil, err
	}

	cfg.Consumer.Return.Errors = true

	if skipUnread {
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	client, err := sarama.NewConsumerGroup(s.config.Brokers, group, cfg)
	if err != nil {
		return nil, err
	}

	result.handler = handler

	result.offsetCh = make(chan int64, 1)

	result.ctx, result.cancel = context.WithCancel(context.Background())

	result.sarama.wg.Add(1)

	go func() {
		defer result.sarama.wg.Done()
		defer client.Close()

		for {
			err = client.Consume(result.ctx, topics, result)
			if err != nil {
				fmt.Println("Error occurred on consume:", err)
			}

			if result.ctx.Err() != nil {
				return
			}
		}
	}()

	return result, nil
}

func (o *ConsumerGroup) SetOffset(offset int64) {
	o.offsetCh <- offset
}

func (o *ConsumerGroup) Stop() {
	o.cancel()
	close(o.offsetCh)
}

// PRIVATE METHODS FOR INTERFACE:

func (o *ConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (o *ConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (o *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var msg *sarama.ConsumerMessage
	var offset int64
	var ok bool

	for {
		select {
		case msg, ok = <-claim.Messages():
			if !ok {
				return nil
			}

			if msg != nil {
				if o.handler(msg.Topic, msg.Offset, msg.Value) {
					session.MarkMessage(msg, "")
				}
			}
		case offset, ok = <-o.offsetCh:
			if !ok {
				return nil
			}

			if offset < claim.HighWaterMarkOffset() {
				session.ResetOffset(claim.Topic(), claim.Partition(), offset, "")
				return nil
			} else {
				session.MarkOffset(claim.Topic(), claim.Partition(), offset, "")
			}
		}
	}
}
