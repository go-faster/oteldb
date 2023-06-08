package tempoe2e_test

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
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

	rootPath := ypath.Path("//oteldb-test-" + uuid.NewString()).Child("traces")
	t.Logf("Test path: %s", rootPath)
	tables := ytstorage.NewTables(rootPath)
	require.NoError(t, tables.Migrate(ctx, yc, migrate.OnConflictDrop(ctx, yc)))

	set, err := readBatchSet("_testdata/traces.json")
	require.NoError(t, err)

	inserter := ytstorage.NewInserter(yc, tables)
	querier := ytstorage.NewYTQLQuerier(yc, tables)
	c := setupDB(ctx, t, set, inserter, querier)

	t.Run("SearchTags", func(t *testing.T) {
		a := require.New(t)

		r, err := c.SearchTags(ctx)
		a.NoError(err)
		a.Len(r.TagNames, len(set.Tags))
		for _, tagName := range r.TagNames {
			a.Contains(set.Tags, tagName)
		}
	})
	t.Run("SearchTagValues", func(t *testing.T) {
		a := require.New(t)

		for tagName, tags := range set.Tags {
			tagValues := map[string]struct{}{}
			for _, t := range tags {
				tagValues[t.Value] = struct{}{}
			}

			r, err := c.SearchTagValuesV2(ctx, tempoapi.SearchTagValuesV2Params{TagName: tagName})
			a.NoError(err)
			a.Len(r.TagValues, len(tagValues))
			for _, val := range r.TagValues {
				a.Contains(tagValues, val.Value)
			}
		}
	})
	t.Run("TraceByID", func(t *testing.T) {
		t.Run("Query", func(t *testing.T) {
			a := require.New(t)

			for traceID, trace := range set.Traces {
				r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: uuid.UUID(traceID)})
				a.NoError(err)
				a.IsType(&tempoapi.TraceByID{}, r)

				data, err := io.ReadAll(r.(*tempoapi.TraceByID))
				a.NoError(err)

				var u ptrace.ProtoUnmarshaler
				resp, err := u.UnmarshalTraces(data)
				a.NoError(err)

				a.Equal(resp.SpanCount(), len(trace.Spanset))
				resSpans := resp.ResourceSpans()
				for i := 0; i < resSpans.Len(); i++ {
					scopeSpans := resSpans.At(i).ScopeSpans()
					for i := 0; i < scopeSpans.Len(); i++ {
						spans := scopeSpans.At(i).Spans()
						for i := 0; i < spans.Len(); i++ {
							span := spans.At(i)
							a.Contains(trace.Spanset, span.SpanID())
						}
					}
				}
			}
		})

		t.Run("NotFound", func(t *testing.T) {
			a := require.New(t)
			r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: uuid.New()})
			a.NoError(err)
			a.IsType(&tempoapi.TraceByIDNotFound{}, r)
		})
	})
}
