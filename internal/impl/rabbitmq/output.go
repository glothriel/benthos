package rabbitmq

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	stream_message "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func rabbitMQStreamsOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.7.0").
		Summary("Write messages to a RabbitMQ stream.").
		Field(service.NewStringListField("dsns")).Description("A list of uris (can be one)").Example("", "", "").
		Field(service.NewObjectField(
			"stream",
			service.NewStringField("name"),
			service.NewObjectField(
				"declare",
				service.NewBoolField("enabled"),
				service.NewObjectField(
					"max_age",
					service.NewIntField("seconds").Default(0),
					service.NewIntField("hours").Default(0),
					service.NewIntField("days").Default(0),
				),
				service.NewObjectField(
					"capacity",
					service.NewIntField("megabytes").Default(0),
					service.NewIntField("gigabytes").Default(0),
				),
			),
		)).Description("A list of hosts (can be one)").Example("", "", "")
	// Field(service.NewStringField("address").
	// 	Description("An address to connect to.").
	// 	Example("127.0.0.1:11300")).
	// Field(service.NewIntField("max_in_flight").
	// 	Description("The maximum number of messages to have in flight at a given time. Increase to improve throughput.").
	// 	Default(64))
}

func init() {
	err := service.RegisterOutput(
		"rabbitmqstreams", rabbitMQStreamsOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newRabbitMQStreamsWriterFromConfig(conf, mgr.Logger())
			return w, 1000, err
		})
	if err != nil {
		panic(err)
	}
}

type rabbitMQStreamsWriter struct {
	env      *stream.Environment
	producer *ha.ReliableProducer

	params commonParams
	log    *service.Logger
}

func newRabbitMQStreamsWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*rabbitMQStreamsWriter, error) {
	params, err := commonParamsFromConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing RabbitMQ streams config: %w", err)
	}
	bs := rabbitMQStreamsWriter{
		log:    log,
		params: params,
	}
	return &bs, nil
}

func (bs *rabbitMQStreamsWriter) Connect(ctx context.Context) error {
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
		err = env.DeclareStream(bs.params.streamName,
			opts,
		)
		if err != nil {
			return fmt.Errorf("Failed to declare RabbitMQ Stream: %w", err)
		}
	}

	bs.env = env

	bs.producer, err = ha.NewHAProducer(env, bs.params.streamName, nil, func(messageConfirm []*stream.ConfirmationStatus) {
		for _, m := range messageConfirm {
			bs.log.Info(fmt.Sprintf("%v", m.IsConfirmed()))
		}
	})

	if err != nil {
		return nil
	}
	return nil
}

func (bs *rabbitMQStreamsWriter) Write(ctx context.Context, msg *service.Message) error {
	defer func() { bs.log.Warn("ueue") }()
	messageBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	return bs.producer.BatchSend([]stream_message.StreamMessage{amqp.NewMessage(messageBytes)})
}

func (bs *rabbitMQStreamsWriter) Close(context.Context) error {
	return bs.env.Close()
}
