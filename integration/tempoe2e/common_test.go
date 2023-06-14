package tempoe2e_test

import (
	"context"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"os"
	"strings"
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
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Tags)
	require.NotEmpty(t, set.Traces)

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

			r, err := c.SearchTagValues(ctx, tempoapi.SearchTagValuesParams{TagName: tagName})
			a.NoError(err)
			a.Len(r.TagValues, len(tagValues))
			for _, val := range r.TagValues {
				a.Containsf(tagValues, val, "check tag %q", tagName)
			}
		}
	})
	t.Run("SearchTagValuesV2", func(t *testing.T) {
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

				r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: tracestorage.TraceID(traceID).Hex()})
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
							gotSpan := spans.At(i)
							spanID := gotSpan.SpanID()
							a.Contains(trace.Spanset, spanID, "check trace %q", uid)
							expectSpan := trace.Spanset[spanID]
							// Ensure that spans are equal.
							a.Equal(expectSpan.TraceID(), gotSpan.TraceID())
							a.Equal(expectSpan.SpanID(), gotSpan.SpanID())
							a.Equal(expectSpan.TraceState(), gotSpan.TraceState())
							a.Equal(expectSpan.ParentSpanID(), gotSpan.ParentSpanID())
							a.Equal(expectSpan.Name(), gotSpan.Name())
							a.Equal(expectSpan.Kind(), gotSpan.Kind())
							a.Equal(expectSpan.StartTimestamp(), gotSpan.StartTimestamp())
							a.Equal(expectSpan.EndTimestamp(), gotSpan.EndTimestamp())
							a.Equal(expectSpan.Status(), gotSpan.Status())
							a.Equal(expectSpan.Attributes().AsRaw(), gotSpan.Attributes().AsRaw())
						}
					}
				}
			}
		})

		t.Run("NotFound", func(t *testing.T) {
			a := require.New(t)
			r, err := c.TraceByID(ctx, tempoapi.TraceByIDParams{TraceID: strings.Repeat("0", 16*2)})
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

			spans := metadata.SpanSet.Value.Spans
			a.Len(spans, len(trace.Spanset))
			for _, gotSpan := range spans {
				r, err := hex.DecodeString(gotSpan.SpanID)
				a.NoError(err)

				a.Len(r, 8)
				spanID := *(*pcommon.SpanID)(r)
				a.Contains(trace.Spanset, spanID, "check trace %q", traceID)
				expectSpan := trace.Spanset[spanID]

				a.Equal(expectSpan.Name(), gotSpan.Name)
				start := gotSpan.StartTimeUnixNano.UnixNano()
				end := start + gotSpan.DurationNanos
				a.Equal(int64(expectSpan.StartTimestamp()), start)
				a.Equal(int64(expectSpan.EndTimestamp()), end)

				if expectAttrs := expectSpan.Attributes(); expectAttrs.Len() > 0 {
					// TODO(tdakkota): do a full attributes comparison.
					a.NotNil(gotSpan.Attributes)
					gotAttrs := *gotSpan.Attributes
					a.Len(gotAttrs, expectAttrs.Len())
				}
			}
		}
		t.Run("Search", func(t *testing.T) {
			a := require.New(t)
			r, err := c.Search(ctx, tempoapi.SearchParams{
				Tags: tempoapi.NewOptString(`http.method=POST http.status_code=200`),
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
