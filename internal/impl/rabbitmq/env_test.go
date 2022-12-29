package rabbitmq

import (
	"testing"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
)

func TestConfigureConnectionParametersMultiHost(t *testing.T) {
	opts := stream.NewEnvironmentOptions()
	assert.Nil(t,
		configureConnectionParameters(opts,
			[]string{
				"rabbitmq-stream://guest:guest@server1:5552",
				"rabbitmq-stream://guest:guest@server2:5552",
				"rabbitmq-stream://guest:guest@server3:5552",
			},
		),
	)

	assert.Equal(t, "rabbitmq-stream://guest:guest@server1:5552", opts.ConnectionParameters[0].Uri)
	assert.Equal(t, "rabbitmq-stream://guest:guest@server2:5552", opts.ConnectionParameters[1].Uri)
	assert.Equal(t, "rabbitmq-stream://guest:guest@server3:5552", opts.ConnectionParameters[2].Uri)
}

func TestConfigureConnectionParametersSingleHost(t *testing.T) {
	opts := stream.NewEnvironmentOptions()
	assert.Nil(t,
		configureConnectionParameters(opts,
			[]string{
				"rabbitmq-stream://theUser:thePass@server1:1337",
			},
		),
	)

	assert.Len(t, opts.ConnectionParameters, 1)
	assert.Equal(t, "theUser", opts.ConnectionParameters[0].User)
	assert.Equal(t, "thePass", opts.ConnectionParameters[0].Password)
	assert.Equal(t, "server1", opts.ConnectionParameters[0].Host)
	assert.Equal(t, "1337", opts.ConnectionParameters[0].Port)
}

func TestConfigureConnectionParametersErrors(t *testing.T) {
	type args struct {
		envOptions *stream.EnvironmentOptions
		addresses  []string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Empty address list",
			args: args{envOptions: stream.NewEnvironmentOptions(), addresses: []string{}},
		},
		{
			name: "Non-url",
			args: args{envOptions: stream.NewEnvironmentOptions(), addresses: []string{"foo:/bar"}},
		},
		{
			name: "Empty string",
			args: args{envOptions: stream.NewEnvironmentOptions(), addresses: []string{""}},
		},
		{
			name: "Non-numeric port",
			args: args{
				envOptions: stream.NewEnvironmentOptions(),
				addresses:  []string{"rabbitmq-stream://guest:guest@localhost:notAPort"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := configureConnectionParameters(tt.args.envOptions, tt.args.addresses); err == nil {
				t.Errorf("configureConnectionParameters() error = %v, wantErr true", err)
			}
		})
	}
}
