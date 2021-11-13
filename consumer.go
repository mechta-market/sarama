package sarama

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type ConsumerGroup struct {
	stopCh chan int
	cancel context.CancelFunc

	msgHandler func(topic string, offset int64, msg []byte) bool
}

func (s *Sarama) NewConsumerGroup(
	clientId string,
	group string,
	topics []string,
	msgHandler func(topic string, offset int64, msg []byte) bool,
	skipUnread bool,
	retryInterval time.Duration,
) (ConsumerGroupInterface, error) {
	cGroup := &ConsumerGroup{
		msgHandler: msgHandler,
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

	cGroup.stopCh = make(chan int, 1)

	var ctx context.Context

	ctx, cGroup.cancel = context.WithCancel(context.Background())

	s.wg.Add(1)

	go func() {
		defer client.Close()
		defer s.wg.Done()

		for {
			err = client.Consume(ctx, topics, cGroup)
			if err != nil {
				fmt.Println("Error occurred on consume:", err)
			}
			if ctx.Err() != nil {
				return
			}

			time.Sleep(retryInterval)
		}
	}()

	return cGroup, nil
}

func (o *ConsumerGroup) Stop() {
	o.cancel()
	close(o.stopCh)
}

// PRIVATE METHODS FOR INTERFACE:

func (o *ConsumerGroup) Setup(ses sarama.ConsumerGroupSession) error {
	return nil
}

func (o *ConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (o *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var ok bool
	var msg *sarama.ConsumerMessage

	for {
		select {
		case <-o.stopCh:
			return nil
		case msg, ok = <-claim.Messages():
			if !ok {
				return nil
			}
			if msg != nil {
				if o.msgHandler(msg.Topic, msg.Offset, msg.Value) {
					session.MarkMessage(msg, "")
				} else {
					return errors.New("fail_to_handle_message")
				}
			}
		}
	}
}
