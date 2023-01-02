package rabbitmq

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func init() {
	err := service.RegisterBatchOutput(
		"rabbitmqstreams",
		commonInputConfig("Write messages to a RabbitMQ stream.").
			Field(service.NewBatchPolicyField("batching")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			w, err := newStreamOutputFromConfig(conf, mgr.Logger())
			if err != nil {
				return w, service.BatchPolicy{}, 1, err
			}
			batchPolicy, err := conf.FieldBatchPolicy("batching")
			// max_in_flight == 1, client locks a mutex every time BatchSend is called
			return w, batchPolicy, 1, err
		})
	if err != nil {
		panic(err)
	}
}

type streamOutput struct {
	producer *ha.ReliableProducer

	params commonParams
	log    *service.Logger
}

func newStreamOutputFromConfig(conf *service.ParsedConfig, log *service.Logger) (*streamOutput, error) {
	params, err := commonParamsFromConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing RabbitMQ streams config: %w", err)
	}
	bs := streamOutput{
		log:    log,
		params: params,
	}
	return &bs, nil
}

func (bs *streamOutput) Connect(ctx context.Context) error {
	env, err := prepareStreamEnvironment(bs.params)
	if err != nil {
		return err
	}
	bs.producer, err = ha.NewHAProducer(env, bs.params.streamName, nil, func(messageConfirm []*stream.ConfirmationStatus) {
		// TODO: Ensure, that in case of BatchSend confirmations don't need to be tracked
	})

	if err != nil {
		return nil
	}
	return nil
}

func (bs *streamOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	bs.log.Infof("[out] Writing batch size: %d", len(batch))
	rabbitmMQBatch := []message.StreamMessage{}
	for _, msg := range batch {
		messageBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}
		rabbitmMQBatch = append(rabbitmMQBatch, amqp.NewMessage(messageBytes))
	}
	err := bs.producer.BatchSend(rabbitmMQBatch)
	if err != nil {
		return err
	}
	// if !BG().Bool() {
	// 	return fmt.Errorf("Mock error!")
	// }
	return nil
}

func (bs *streamOutput) Close(context.Context) error {
	return bs.producer.Close()
}

type boolgen struct {
	src       rand.Source
	cache     int64
	remaining int
}

func (b *boolgen) Bool() bool {
	if b.remaining == 0 {
		b.cache, b.remaining = b.src.Int63(), 63
	}

	result := b.cache&0x01 == 1
	b.cache >>= 1
	b.remaining--

	return result
}
func BG() *boolgen {
	return &boolgen{src: rand.NewSource(time.Now().UnixNano())}
}
