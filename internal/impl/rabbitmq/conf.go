package rabbitmq

import (
	"errors"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
)

func commonInputConfig(summary string) *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.7.0").
		Summary(summary).
		Field(
			service.NewStringListField("dsns"),
		).Description("A list of uris (can be one)").Example("", "", "").
		Field(service.NewObjectField(
			"stream",
			service.NewStringField("name"),
			service.NewObjectField(
				"declare",
				service.NewBoolField("enabled").Default(false),
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
}

type commonParams struct {
	dsns              []string
	streamName        string
	declareEnabled    bool
	declareMaxAge     time.Duration
	declareCapacityGb int
	declareCapacityMb int
}

func commonParamsFromConfig(parsedConfig *service.ParsedConfig) (commonParams, error) {
	params := &commonParams{}
	var err error

	params.dsns, err = parsedConfig.FieldStringList("dsns")
	if err != nil {
		return *params, err
	}
	params.streamName, err = parsedConfig.FieldString("stream", "name")
	if err != nil {
		return *params, err
	}
	params.declareEnabled, err = parsedConfig.FieldBool("stream", "declare", "enabled")
	if err != nil {
		return *params, err
	}

	maxAgeSeconds, err := parsedConfig.FieldInt("stream", "declare", "max_age", "seconds")
	if err != nil {
		return *params, err
	}
	if maxAgeSeconds != 0 {
		params.declareMaxAge = time.Duration(maxAgeSeconds) * time.Second
	}
	maxAgeHours, err := parsedConfig.FieldInt("stream", "declare", "max_age", "hours")
	if err != nil {
		return *params, err
	}
	if maxAgeHours != 0 {
		params.declareMaxAge = time.Duration(maxAgeHours) * time.Hour
	}
	maxAgeDays, err := parsedConfig.FieldInt("stream", "declare", "max_age", "days")
	if err != nil {
		return *params, err
	}
	if maxAgeDays != 0 {
		params.declareMaxAge = time.Duration(maxAgeDays) * time.Hour * 24
	}

	if !validateAtMostOneBiggerThanZero([]int{maxAgeDays, maxAgeHours, maxAgeSeconds}) {
		return *params, errors.New("You can set only one age constraint, please choose between days, hours and seconds")
	}

	params.declareCapacityMb, err = parsedConfig.FieldInt("stream", "declare", "capacity", "megabytes")
	if err != nil {
		return *params, err
	}
	params.declareCapacityGb, err = parsedConfig.FieldInt("stream", "declare", "capacity", "gigabytes")
	if err != nil {
		return *params, err
	}
	if !validateAtMostOneBiggerThanZero([]int{params.declareCapacityMb, params.declareCapacityGb}) {
		return *params, errors.New("You can set only one capacity constraint, please choose between megabytes and gigabytes")
	}

	return *params, nil
}

func validateAtMostOneBiggerThanZero(values []int) bool {
	foundBigger := false
	for _, v := range values {
		if v > 0 {
			if foundBigger {
				return false
			}
			foundBigger = true
		}
	}
	return true
}

func consumerInputConfig(spec *service.ConfigSpec) *service.ConfigSpec {
	return spec.Field(
		service.NewDurationField("offset_flush_interval").
			Default("100ms"),
	).Field(
		service.NewIntField("checkpoint_limit").
			Description("Determines how many messages can be processed in parallel before applying back pressure.").
			Default(1024).
			Advanced(),
	).Field(
		service.NewStringField("consumer"),
	)
}

type consumerParams struct {
	consumerName        string
	offsetFlushInterval time.Duration
	checkpointLimit     int
}

func consumerParamsFromConfig(conf *service.ParsedConfig) (consumerParams, error) {
	var params consumerParams
	var err error
	params.consumerName, err = conf.FieldString("consumer")
	if err != nil {
		return params, err
	}
	params.offsetFlushInterval, err = conf.FieldDuration("offset_flush_interval")
	if err != nil {
		return params, err
	}
	params.checkpointLimit, err = conf.FieldInt("checkpoint_limit")
	if err != nil {
		return params, err
	}
	return params, nil
}
