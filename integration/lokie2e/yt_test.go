package lokie2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

	"github.com/go-faster/oteldb/internal/ytstorage"
)

func TestYT(t *testing.T) {
	t.Parallel()
	if os.Getenv("E2E") == "" {
		t.Skip("Set E2E env to run")
	}
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Name:         "oteldb-lokie2e-ytsaurus",
		Image:        "ytsaurus/local:stable",
		ExposedPorts: []string{"80/tcp"},
		Cmd: []string{
			`--fqdn`, `localhost`,
			`--proxy-config`, `{address_resolver={enable_ipv4=%true;enable_ipv6=%false;};coordinator={public_fqdn="localhost:8000"}}`,
			`--rpc-proxy-count`, `0`,
			`--rpc-proxy-port`, `8002`,
		},
		WaitingFor: wait.ForLog("Local YT started"),
	}
	ytContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           testcontainers.TestLogger(t),
		Reuse:            true,
	})
	require.NoError(t, err, "container start")

	endpoint, err := ytContainer.Endpoint(ctx, "")
	require.NoError(t, err, "container endpoint")

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy: endpoint,
	})
	require.NoError(t, err)

	rootPath := ypath.Path("//oteldb-test-" + uuid.NewString()).Child("logs")
	t.Logf("Test tables path: %s", rootPath)
	tables := ytstorage.NewTables(rootPath)
	{
		migrateBackoff := backoff.NewExponentialBackOff()
		migrateBackoff.InitialInterval = 2 * time.Second
		migrateBackoff.MaxElapsedTime = time.Minute

		if err := backoff.Retry(func() error {
			return tables.Migrate(ctx, yc, migrate.OnConflictDrop(ctx, yc))
		}, migrateBackoff); err != nil {
			t.Fatalf("Migrate: %+v", err)
		}
	}

	inserter, err := ytstorage.NewInserter(yc, ytstorage.InserterOptions{Tables: tables})
	require.NoError(t, err)

	querier, err := ytstorage.NewYTQLQuerier(yc, ytstorage.YTQLQuerierOptions{Tables: tables})
	require.NoError(t, err)

	runTest(ctx, t, inserter, querier, querier)
}
