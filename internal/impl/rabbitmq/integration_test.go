package rabbitmq

import (
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

const template string = `
output:
  rabbitmqstreams:
    address: localhost:$PORT
    max_in_flight: $MAX_IN_FLIGHT

input:
  rabbitmqstreams:
    address: localhost:$PORT
`

func TestIntegrationRabbitMQStreamsOpenClose(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("library/rabbitmq", "3-management-alpine", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(30)
	require.NoError(t, pool.Retry(func() error {
		return nil
	}))

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("5672/tcp")),
	)
}
