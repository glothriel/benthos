package rabbitmq

import (
	"errors"
	"fmt"
	"sync"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type onMessageFunc func(offset int64, message *amqp.Message)
type lastProcessedOffsetFunc func() int64

var errConsumerPaused = errors.New("consumer is paused")
var errConsumerAlreadyRunning = errors.New("consumer already running")

func newPausableConsumerFromConfig(
	commonParams commonParams,
	consumerParams consumerParams,
	handler onMessageFunc,
	offsetFunc lastProcessedOffsetFunc,
	log *service.Logger,
) (*pausableConsumer, error) {
	startFunc := func(offset int64) (*stream.Consumer, error) {

		env, err := prepareStreamEnvironment(commonParams)
		if err != nil {
			return nil, err
		}

		opts := stream.NewConsumerOptions().
			SetConsumerName(consumerParams.consumerName).
			SetManualCommit()

		if offset > 0 {
			// If offset is passed explicitly (for example some messages were processed while the consumer was paused
			// and we don't need to reprocess them), use it.
			opts = opts.
				SetOffset(stream.OffsetSpecification{}.Offset(offset))
		} else {
			// Otherwise use the offset stored per-consumer on the server.
			opts = opts.
				SetOffset(stream.OffsetSpecification{}.LastConsumed())
		}

		streamsConsumer, err := env.NewConsumer(
			commonParams.streamName,
			func(consumerContext stream.ConsumerContext, message *amqp.Message) {
				handler(consumerContext.Consumer.GetOffset(), message)
			},
			opts,
		)
		if err != nil {
			return nil, err
		}
		return streamsConsumer, nil
	}

	consumer := &pausableConsumer{
		startFunc:  startFunc,
		lock:       &sync.Mutex{},
		offsetFunc: offsetFunc,
		log:        log,
	}
	return consumer, consumer.start()
}

type pausableConsumer struct {
	lock       *sync.Mutex
	consumer   *stream.Consumer
	startFunc  func(int64) (*stream.Consumer, error)
	offsetFunc lastProcessedOffsetFunc
	log        *service.Logger
}

func (consumer *pausableConsumer) pause() error {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()
	if consumer.consumer == nil {
		return errConsumerPaused
	}
	currentConsumer := consumer.consumer
	consumer.consumer = nil

	offset := consumer.offsetFunc()
	if offset != 0 {
		if err := currentConsumer.StoreCustomOffset(offset); err != nil {
			consumer.log.Errorf("Failed to store offset before pausing the consumer: %s", err.Error())
		}
	}
	if err := currentConsumer.Close(); err != nil {
		return fmt.Errorf("Failed to pause the consumer: %w", err)
	}
	return nil
}

func (consumer *pausableConsumer) start() error {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()
	if consumer.consumer != nil {
		return errConsumerAlreadyRunning
	}
	var err error
	consumer.consumer, err = consumer.startFunc(consumer.offsetFunc())
	return err
}

func (consumer *pausableConsumer) setOffset(o int64) error {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()
	if consumer.consumer == nil {
		return errConsumerPaused
	}
	return consumer.consumer.StoreCustomOffset(o)
}

func (consumer *pausableConsumer) close() error {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()
	if consumer.consumer == nil {
		return errConsumerPaused
	}
	return consumer.consumer.Close()
}
