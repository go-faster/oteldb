package tempoe2e_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

	"github.com/go-faster/oteldb/integration/tempoe2e"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempohandler"
	"github.com/go-faster/oteldb/internal/tracestorage"
	"github.com/go-faster/oteldb/internal/ytstorage"
)

func readBatchSet(p string) (s tempoe2e.BatchSet, _ error) {
	f, err := os.Open(p)
	if err != nil {
		return s, err
	}
	defer func() {
		_ = f.Close()
	}()
	return tempoe2e.ParseBatchSet(f)
}

func setupDB(
	ctx context.Context,
	t *testing.T,
	set tempoe2e.BatchSet,
	inserter tracestorage.Inserter,
	querier tracestorage.Querier,
) *tempoapi.Client {
	consumer := tracestorage.NewConsumer(inserter)
	for i, b := range set.Batches {
		if err := consumer.ConsumeTraces(ctx, b); err != nil {
			t.Fatalf("Send batch %d: %+v", i, err)
		}
	}

	api := tempohandler.NewTempoAPI(querier)
	tempoh, err := tempoapi.NewServer(api)
	require.NoError(t, err)

	s := httptest.NewServer(tempoh)
	t.Cleanup(s.Close)

	c, err := tempoapi.NewClient(s.URL, tempoapi.WithClient(s.Client()))
	require.NoError(t, err)
	return c
}

func TestYT(t *testing.T) {
	ctx := context.Background()

	proxy := os.Getenv("E2E_YT_PROXY")
	if proxy == "" {
		t.Skip("Set E2E_YT_PROXY to run")
	}

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy: proxy,
	})
	require.NoError(t, err)

	tables := ytstorage.NewTables(ypath.Path("//oteldb-test").Child("traces"))
	require.NoError(t, tables.Migrate(ctx, yc, migrate.OnConflictDrop(ctx, yc)))

	set, err := readBatchSet("_testdata/traces.json")
	require.NoError(t, err)

	inserter := ytstorage.NewInserter(yc, tables)
	querier := ytstorage.NewYTQLQuerier(yc, tables)
	c := setupDB(ctx, t, set, inserter, querier)

	t.Run("SearchTags", func(t *testing.T) {
		a := require.New(t)

		tags, err := c.SearchTags(ctx)
		a.NoError(err)
		a.Len(tags.TagNames, len(set.Tags))
		for _, tagName := range tags.TagNames {
			a.Contains(set.Tags, tagName)
		}
	})
}
