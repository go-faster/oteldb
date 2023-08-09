package tempoe2e_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func TestCH(t *testing.T) {
	t.Parallel()
	if os.Getenv("E2E") == "" {
		t.Skip("Set E2E env to run")
	}
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Name:         "oteldb-tempoe2e-clickhouse",
		Image:        "clickhouse/clickhouse-server:23.4",
		ExposedPorts: []string{"8123/tcp", "9000/tcp"},
	}
	chContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           testcontainers.TestLogger(t),
		Reuse:            true,
	})
	require.NoError(t, err, "container start")

	endpoint, err := chContainer.PortEndpoint(ctx, "9000", "")
	require.NoError(t, err, "container endpoint")

	opts := ch.Options{
		Address:  endpoint,
		Database: "default",
	}
	connectBackoff := backoff.NewExponentialBackOff()
	connectBackoff.InitialInterval = 2 * time.Second
	connectBackoff.MaxElapsedTime = time.Minute
	c, err := backoff.RetryWithData(func() (*chpool.Pool, error) {
		c, err := chpool.Dial(ctx, chpool.Options{
			ClientOptions: opts,
		})
		if err != nil {
			return nil, errors.Wrap(err, "dial")
		}
		return c, nil
	}, connectBackoff)
	if err != nil {
		t.Fatal(err)
	}

	prefix := "traces_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	tables := chstorage.Tables{
		Spans: prefix + "_spans",
		Tags:  prefix + "_tags",
	}
	t.Logf("Test tables prefix: %s", prefix)
	require.NoError(t, tables.Create(ctx, c))

	inserter := chstorage.NewInserter(c, tables)
	querier := chstorage.NewQuerier(c, tables)
	runTest(ctx, t, inserter, querier, nil)
}
