package rabbitmq

import (
	"context"
	"fmt"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterInput(
		"rabbitmqstreams", commonInputConfig("Reads messages from a RabbitMQ stream protocol."),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newRabbitMQStreamsReaderFromConfig(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

type rabbitMQStreamsReader struct {
	env      *stream.Environment
	consumer *stream.Consumer

	params commonParams
	log    *service.Logger

	messages chan *amqp.Message
}

func newRabbitMQStreamsReaderFromConfig(conf *service.ParsedConfig, log *service.Logger) (*rabbitMQStreamsReader, error) {
	params, err := commonParamsFromConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing RabbitMQ streams config: %w", err)
	}
	bs := rabbitMQStreamsReader{
		log:    log,
		params: params,

		messages: make(chan *amqp.Message),
	}
	return &bs, nil
}

func (bs *rabbitMQStreamsReader) Connect(ctx context.Context) error {
	opts := stream.NewEnvironmentOptions()
	if err := configureConnectionParameters(opts, bs.params.dsns); err != nil {
		return err
	}
	env, err := stream.NewEnvironment(opts)
	if err != nil {
		return err
	}

	if bs.params.declareEnabled {
		opts := &stream.StreamOptions{}
		if bs.params.declareCapacityGb != 0 {
			opts.SetMaxLengthBytes(stream.ByteCapacity{}.GB(int64(bs.params.declareCapacityGb)))
		} else if bs.params.declareCapacityMb != 0 {
			opts.SetMaxLengthBytes(stream.ByteCapacity{}.MB(int64(bs.params.declareCapacityMb)))
		}
		if bs.params.declareMaxAge > 0 {
			opts.SetMaxAge(bs.params.declareMaxAge)
		}
		err = env.DeclareStream(
			bs.params.streamName,
			opts,
		)
		if err != nil {
			return fmt.Errorf("Failed to declare RabbitMQ Stream: %w", err)
		}
	}

	bs.env = env

	bs.consumer, err = env.NewConsumer(bs.params.streamName, func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		bs.messages <- message
	}, nil)

	if err != nil {
		return nil
	}
	return nil
}

func (bs *rabbitMQStreamsReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	msg := service.NewMessage((<-bs.messages).GetData())
	return msg, func(ctx context.Context, res error) error {
		return bs.consumer.StoreOffset()
	}, nil
}

func (bs *rabbitMQStreamsReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {

	return nil, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (bs *rabbitMQStreamsReader) Close(ctx context.Context) (err error) {
	close(bs.messages)
	return bs.env.Close()
}
