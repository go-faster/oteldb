package tempoe2e_test

import (
	"context"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/go-faster/oteldb/integration/tempoe2e"
	"github.com/go-faster/oteldb/internal/tempoapi"
	"github.com/go-faster/oteldb/internal/tempohandler"
	"github.com/go-faster/oteldb/internal/tracestorage"
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

func runTest(
	ctx context.Context,
	t *testing.T,
	inserter tracestorage.Inserter,
	querier tracestorage.Querier,
) {
	set, err := readBatchSet("_testdata/traces.json")
	require.NoError(t, err)
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
				a.Containsf(tagValues, val.Value, "check tag %q", tagName)
			}
		}
	})
	t.Run("TraceByID", func(t *testing.T) {
		t.Run("Query", func(t *testing.T) {
			a := require.New(t)

			for traceID, trace := range set.Traces {
				uid := uuid.UUID(traceID)

				r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: uid})
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
							a.Contains(trace.Spanset, span.SpanID(), "check trace %q", uid)
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
	t.Run("SearchWithLogfmt", func(t *testing.T) {
		validateMetadata := func(a *require.Assertions, metadata tempoapi.TraceSearchMetadata) {
			a.NotEmpty(metadata.RootTraceName)
			a.False(metadata.StartTimeUnixNano.IsZero())

			traceID := uuid.MustParse(metadata.TraceID)
			trace, ok := set.Traces[pcommon.TraceID(traceID)]
			a.Truef(ok, "search trace %q", traceID)

			spans := metadata.SpanSet.Spans
			a.Len(spans, len(trace.Spanset))
			for _, s := range spans {
				r, err := hex.DecodeString(s.SpanID)
				a.NoError(err)
				a.Len(r, 8)
				a.Contains(trace.Spanset, *(*pcommon.SpanID)(r), "check trace %q", traceID)
			}
		}
		t.Run("Search", func(t *testing.T) {
			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Tags:  tempoapi.NewOptString(`http.method=POST http.status_code=200`),
				Limit: tempoapi.NewOptInt(1),
			})
			a.NoError(err)
			a.NotEmpty(r.Traces)

			for _, metadata := range r.Traces {
				validateMetadata(a, metadata)
			}
		})
		t.Run("Limit", func(t *testing.T) {
			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Tags:  tempoapi.NewOptString(`http.method=GET http.status_code=200`),
				Limit: tempoapi.NewOptInt(1),
			})
			a.NoError(err)
			a.Len(r.Traces, 1)
			validateMetadata(a, r.Traces[0])
		})
		t.Run("TagsNotExist", func(t *testing.T) {
			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Tags: tempoapi.NewOptString(`clearly.not.exist=amongus http.method=GET`),
			})
			a.NoError(err)
			a.Empty(r.Traces)
		})
	})
}
