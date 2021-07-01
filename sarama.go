package sarama

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/Shopify/sarama"
)

type Sarama struct {
	config Config

	wg sync.WaitGroup
}

type Config struct {
	Brokers []string

	SaslUsername string
	SaslPassword string

	ScramMechanism string // SHA512 | SHA256

	TLS           bool
	TLSRootCAPath string
}

func NewSarama(config Config) *Sarama {
	fmt.Println("NewSarama config:", config)

	return &Sarama{
		config: config,
	}
}

func (s *Sarama) Wait() {
	s.wg.Wait()
}

func (s *Sarama) getCommonConfig(clientId string) (*sarama.Config, error) {
	result := sarama.NewConfig()
	result.Version = sarama.V0_10_2_0
	result.ClientID = clientId
	result.Metadata.Full = true

	if s.config.SaslUsername != "" {
		result.Net.SASL.Enable = true
		result.Net.SASL.Handshake = true
		result.Net.SASL.User = s.config.SaslUsername
		result.Net.SASL.Password = s.config.SaslPassword
	}

	switch s.config.ScramMechanism {
	case "SHA512":
		result.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		result.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	case "SHA256":
		result.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		result.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case "":
	default:
		fmt.Println("Undefined scram mechanism, must be `SHA512` or `SHA256`")
	}

	if s.config.TLS {
		result.Net.TLS.Enable = true
		result.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
		}

		if s.config.TLSRootCAPath != "" {
			certs := x509.NewCertPool()

			pemData, err := ioutil.ReadFile(s.config.TLSRootCAPath)
			if err != nil {
				return nil, err
			}

			certs.AppendCertsFromPEM(pemData)
			result.Net.TLS.Config.RootCAs = certs
		}
	}

	return result, nil
}
