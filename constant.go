package sarama

import (
	"github.com/Shopify/sarama"
)

type AckType int16

const (
	AckTypeNoResponse   = AckType(sarama.NoResponse)   // not wait
	AckTypeWaitForLocal = AckType(sarama.WaitForLocal) // wait for write to disk
	AckTypeWaitForAll   = AckType(sarama.WaitForAll)   // wait for saving on all replicas
)
