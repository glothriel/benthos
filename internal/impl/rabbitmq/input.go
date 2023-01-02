package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

	"github.com/benthosdev/benthos/v4/internal/checkpoint"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterInput(
		"rabbitmqstreams", consumerInputConfig(commonInputConfig("Write messages to a RabbitMQ stream.")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			input, err := streamInputFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacks(input), nil
		})
	if err != nil {
		panic(err)
	}
}

type streamsReader struct {
	commonParams   commonParams
	consumerParams consumerParams

	consumer *pausableConsumer
	log      *service.Logger

	checkpoint         *checkpoint.Type
	messages           chan *inFlightMessage
	backgroundQuitChan chan bool
}

func streamInputFromConfig(conf *service.ParsedConfig, log *service.Logger) (*streamsReader, error) {
	commonParams, err := commonParamsFromConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing RabbitMQ streams config: %w", err)
	}
	consumerParams, err := consumerParamsFromConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("Failed parsing RabbitMQ streams config: %w", err)
	}

	bs := &streamsReader{
		log:            log,
		commonParams:   commonParams,
		consumerParams: consumerParams,
		messages:       make(chan *inFlightMessage),
		checkpoint:     checkpoint.New(),
	}

	return bs, nil
}

func (bs *streamsReader) Connect(ctx context.Context) error {
	var err error
	bs.consumer, err = newPausableConsumerFromConfig(
		bs.commonParams,
		bs.consumerParams,
		func(offset int64, message *amqp.Message) {
			onAck := bs.checkpoint.Track(offset, 1)
			if bs.tooManyInFlightMessages() {
				err := bs.consumer.pause()
				if err != nil && err != errConsumerPaused {
					bs.log.Warnf("Failed to pause consumer: %", err)
				} else if err == nil {
					bs.log.Info("Paused the consumer")
				}
			}

			bs.messages <- &inFlightMessage{
				amqpMsg: message,
				onAck:   onAck,
			}
		}, func() int64 {
			highest := bs.checkpoint.Highest()
			highestConverted, ok := highest.(int64)
			if ok {
				return highestConverted + 1
			}
			return 0
		},
		bs.log,
	)
	if err != nil {
		return err
	}

	go bs.startBackgroundTasks()

	return nil
}

func (bs *streamsReader) startBackgroundTasks() {
	bs.backgroundQuitChan = make(chan bool)
	offsetFlushTicker := time.NewTicker(bs.consumerParams.offsetFlushInterval)
	wakeConsumerTicker := time.NewTicker(1 * time.Millisecond)

	for {
		select {
		case <-offsetFlushTicker.C:
			highest := bs.checkpoint.Highest()
			highestConverted, ok := highest.(int64)

			if ok && highestConverted > 0 {
				err := bs.consumer.setOffset(highestConverted + 1)
				if err != nil && err != errConsumerPaused {
					bs.log.Warnf("Failed to store offset: %s", err)
				}
			}
		case <-wakeConsumerTicker.C:
			if !bs.tooManyInFlightMessages() {
				err := bs.consumer.start()
				if err != nil && err != errConsumerAlreadyRunning {
					bs.log.Warnf("Failed to start consumer: %s", err)
				} else if err == nil {
					bs.log.Info("Started the conusmer")
				}
			}
		case <-bs.backgroundQuitChan:
			return
		}
	}
}

func (bs *streamsReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case inFlightMsg := <-bs.messages:
		msg := service.NewMessage(inFlightMsg.amqpMsg.GetData())
		return msg, func(ctx context.Context, outputErr error) error {
			if outputErr == nil {
				inFlightMsg.onAck()
			}
			return outputErr
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (bs *streamsReader) Close(ctx context.Context) (err error) {
	close(bs.messages)
	bs.backgroundQuitChan <- true
	bs.backgroundQuitChan = nil
	return bs.consumer.close()
}

func (bs *streamsReader) tooManyInFlightMessages() bool {
	return bs.checkpoint.Pending() > int64(bs.consumerParams.checkpointLimit)
}

type inFlightMessage struct {
	amqpMsg *amqp.Message
	onAck   func() any
}
