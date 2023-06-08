package tempoe2e_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

	"github.com/go-faster/oteldb/internal/ytstorage"
)

func TestYT(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := os.Getenv("E2E_YT_PROXY")
	if proxy == "" {
		t.Skip("Set E2E_YT_PROXY to run")
	}

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy: proxy,
	})
	require.NoError(t, err)

	rootPath := ypath.Path("//oteldb-test-" + uuid.NewString()).Child("traces")
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

	inserter := ytstorage.NewInserter(yc, tables)
	querier := ytstorage.NewYTQLQuerier(yc, tables)
	runTest(ctx, t, inserter, querier)
}
