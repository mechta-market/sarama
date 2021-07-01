package sarama

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type ConsumerGroup struct {
	sarama *Sarama

	stopCh chan int
	ctx    context.Context
	cancel context.CancelFunc

	msgHandler     func(topic string, offset int64, msg []byte) bool
	commitHandler  func(topic string) bool
	commitDuration time.Duration
}

func (s *Sarama) NewConsumerGroup(
	clientId string,
	group string,
	topics []string,
	msgHandler func(topic string, offset int64, msg []byte) bool,
	commitHandler func(topic string) bool,
	commitDuration time.Duration,
	skipUnread bool,
) (ConsumerGroupInterface, error) {
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

	result.msgHandler = msgHandler
	result.commitHandler = commitHandler
	result.commitDuration = commitDuration

	result.stopCh = make(chan int, 1)

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
	var lastMsgCommitted = true
	var commitTimer *time.Timer

	useTimer := o.commitHandler != nil && o.commitDuration > 0

	if useTimer {
		commitTimer = time.NewTimer(o.commitDuration)
	} else {
		commitTimer = time.NewTimer(time.Minute)
	}

	commitTimer.Stop() // initial state

	stopAndDrainCommitTimer := func() {
		commitTimer.Stop()

		for {
			select {
			case <-commitTimer.C:
			default:
				return
			}
		}
	}

	handleCommitTimout := func() {
		stopAndDrainCommitTimer()

		if !lastMsgCommitted {
			if o.commitHandler(msg.Topic) {
				session.MarkMessage(msg, "")
				lastMsgCommitted = true
			} else {
				commitTimer.Reset(o.commitDuration)
			}
		}
	}

	handleMsg := func() {
		if o.msgHandler(msg.Topic, msg.Offset, msg.Value) {
			session.MarkMessage(msg, "")
			lastMsgCommitted = true
		} else {
			lastMsgCommitted = false

			if useTimer {
				stopAndDrainCommitTimer()
				commitTimer.Reset(o.commitDuration)
			}
		}
	}

	for {
		select {
		case <-o.stopCh:
			return nil
		default:
		}

		select {
		case <-commitTimer.C:
			handleCommitTimout()
		default:
		}

		select {
		case <-o.stopCh:
			return nil
		case <-commitTimer.C:
			handleCommitTimout()
		case msg, ok = <-claim.Messages():
			if !ok {
				return nil
			}
			if msg != nil {
				handleMsg()
			}
		}
	}
}
