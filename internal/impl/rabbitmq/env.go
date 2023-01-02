package rabbitmq

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func configureConnectionParameters(
	envOptions *stream.EnvironmentOptions, addresses []string,
) error {
	if len(addresses) > 1 {
		envOptions.SetUris(addresses)
		return nil
	} else if len(addresses) == 1 {
		u, err := url.Parse(addresses[0])
		if err != nil {
			return fmt.Errorf("Failed to parse DSN as valid url: %w", err)
		}
		if u.Host == "" || u.Port() == "" || u.User.Username() == "" {
			return fmt.Errorf("DSN is missing information, please make sure to set hostname, port and username")
		}
		thePort, err := strconv.Atoi(u.Port())
		if err != nil {
			return fmt.Errorf("Cannot convert port value `%s` to integer: %w", u.Port(), err)
		}

		envOptions.SetHost(u.Hostname()).SetPort(thePort).SetUser(u.User.Username())
		pass, passSet := u.User.Password()
		if passSet {
			envOptions.SetPassword(pass)
		}
		return nil
	}
	return errors.New("You need to configure at least one RabbitMQ stream address")
}

func prepareStreamEnvironment(params commonParams) (*stream.Environment, error) {
	opts := stream.NewEnvironmentOptions()
	if err := configureConnectionParameters(opts, params.dsns); err != nil {
		return nil, err
	}

	env, err := stream.NewEnvironment(opts)
	if err != nil {
		return nil, err
	}

	if params.declareEnabled {
		opts := &stream.StreamOptions{}
		if params.declareCapacityGb != 0 {
			opts.SetMaxLengthBytes(stream.ByteCapacity{}.GB(int64(params.declareCapacityGb)))
		} else if params.declareCapacityMb != 0 {
			opts.SetMaxLengthBytes(stream.ByteCapacity{}.MB(int64(params.declareCapacityMb)))
		}
		if params.declareMaxAge > 0 {
			opts.SetMaxAge(params.declareMaxAge)
		}
		err = env.DeclareStream(
			params.streamName,
			opts,
		)
		if err != nil {
			return nil, fmt.Errorf("Failed to declare RabbitMQ Stream: %w", err)
		}
	}

	return env, nil
}
