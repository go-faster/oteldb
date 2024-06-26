package lokie2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"
	"sigs.k8s.io/yaml"

	"github.com/go-faster/oteldb/integration/lokie2e"
	"github.com/go-faster/oteldb/integration/requirex"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokihandler"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func TestMain(m *testing.M) {
	// Explicitly registering flags for golden files.
	gold.Init()

	os.Exit(m.Run())
}

func setupDB(
	ctx context.Context,
	t *testing.T,
	provider trace.TracerProvider,
	set *lokie2e.BatchSet,
	inserter logstorage.Inserter,
	querier logstorage.Querier,
	engineQuerier logqlengine.Querier,
) *lokiapi.Client {
	consumer := logstorage.NewConsumer(inserter)

	logEncoder := plog.JSONMarshaler{}
	var out bytes.Buffer
	for i, b := range set.Batches {
		if err := consumer.ConsumeLogs(ctx, b); err != nil {
			t.Fatalf("Send batch %d: %+v", i, err)
		}
		data, err := logEncoder.MarshalLogs(b)
		require.NoError(t, err)
		outData, err := yaml.JSONToYAML(data)
		require.NoError(t, err)
		out.WriteString("---\n")
		out.Write(outData)
	}

	gold.Str(t, out.String(), "logs.yml")

	var optimizers []logqlengine.Optimizer
	optimizers = append(optimizers, logqlengine.DefaultOptimizers()...)
	optimizers = append(optimizers, &chstorage.ClickhouseOptimizer{})
	engine := logqlengine.NewEngine(engineQuerier, logqlengine.Options{
		ParseOptions:   logql.ParseOptions{AllowDots: true},
		Optimizers:     optimizers,
		TracerProvider: provider,
	})

	api := lokihandler.NewLokiAPI(querier, engine)
	lokih, err := lokiapi.NewServer(api,
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)

	s := httptest.NewServer(lokih)
	t.Cleanup(s.Close)

	c, err := lokiapi.NewClient(s.URL,
		lokiapi.WithClient(s.Client()),
		lokiapi.WithTracerProvider(provider),
	)
	require.NoError(t, err)
	return c
}

func runTest(
	ctx context.Context,
	t *testing.T,
	provider trace.TracerProvider,
	inserter logstorage.Inserter,
	querier logstorage.Querier,
	engineQuerier logqlengine.Querier,
) {
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	set, err := generateLogs(now)
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Labels)
	require.NotEmpty(t, set.Records)
	require.NotZero(t, set.Start)
	require.NotZero(t, set.End)
	require.GreaterOrEqual(t, set.End, set.Start)

	c := setupDB(ctx, t, provider, set, inserter, querier, engineQuerier)

	t.Run("Labels", func(t *testing.T) {
		a := require.New(t)
		r, err := c.Labels(ctx, lokiapi.LabelsParams{
			// Always sending time range because default is current time.
			Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
			End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
		})
		a.NoError(err)

		a.Len(r.Data, len(set.Labels))
		requirex.Unique(t, r.Data)
		requirex.Sorted(t, r.Data)
		for _, label := range r.Data {
			a.Contains(set.Labels, label)
		}
	})
	t.Run("LabelValues", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			for labelName, labels := range set.Labels {
				unique := map[string]struct{}{}
				for _, t := range labels {
					unique[t.Value] = struct{}{}
				}
				values := maps.Keys(unique)
				slices.Sort(values)

				t.Run(labelName, func(t *testing.T) {
					a := require.New(t)

					r, err := c.LabelValues(ctx, lokiapi.LabelValuesParams{
						Name: labelName,
						// Always sending time range because default is current time.
						Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
						End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
					})
					a.NoError(err)

					a.Len(r.Data, len(values))
					requirex.Unique(t, r.Data)
					requirex.Sorted(t, r.Data)
					for _, val := range r.Data {
						a.Containsf(values, val, "check label %q", labelName)
					}
				})
			}
		})
		for _, tt := range []struct {
			name    string
			params  lokiapi.LabelValuesParams
			want    []string
			wantErr bool
		}{
			{
				"OneMatcher",
				lokiapi.LabelValuesParams{
					Name:  "http_method",
					Query: lokiapi.NewOptString(`{http_method="GET"}`),
					Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				},
				[]string{"GET"},
				false,
			},
			{
				"AnotherLabel",
				lokiapi.LabelValuesParams{
					Name:  "http_method",
					Query: lokiapi.NewOptString(`{http_method="HEAD",http_status_code="500"}`),
					Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				},
				[]string{"HEAD"},
				false,
			},
			{
				"UnknownLabel",
				lokiapi.LabelValuesParams{
					Name:  "label_clearly_not_exist",
					Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				},
				nil,
				false,
			},
			{
				"UnknownValue",
				lokiapi.LabelValuesParams{
					Name:  "http_method",
					Query: lokiapi.NewOptString(`{http_method="clearly_not_exist"}`),
					Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				},
				nil,
				false,
			},
			{
				"NoMatch",
				lokiapi.LabelValuesParams{
					Name:  "http_method",
					Query: lokiapi.NewOptString(`{handler=~".+",clearly="not_exist"}`),
					Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				},
				nil,
				false,
			},
			{
				"OutOfRange",
				lokiapi.LabelValuesParams{
					Name:  "http_method",
					Start: lokiapi.NewOptLokiTime(asLokiTime(10)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(20)),
				},
				nil,
				false,
			},
			{
				"OutOfRangeWithQuery",
				lokiapi.LabelValuesParams{
					Name:  "http_method",
					Start: lokiapi.NewOptLokiTime(asLokiTime(10)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(20)),
				},
				nil,
				false,
			},
			{
				"InvalidSelector",
				lokiapi.LabelValuesParams{
					Name:  "http_method",
					Query: lokiapi.NewOptString(`\{\}`),
					Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				},
				nil,
				true,
			},
			{
				"InvalidRange",
				lokiapi.LabelValuesParams{
					Name:  "http_method",
					Query: lokiapi.NewOptString(`\{\}`),
					Start: lokiapi.NewOptLokiTime(asLokiTime(20)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(10)),
				},
				nil,
				true,
			},
		} {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				a := require.New(t)

				r, err := c.LabelValues(ctx, tt.params)
				if tt.wantErr {
					var gotErr *lokiapi.ErrorStatusCode
					a.ErrorAs(err, &gotErr)
					return
				}
				a.NoError(err)

				requirex.Unique(t, r.Data)
				requirex.Sorted(t, r.Data)
				a.ElementsMatch(tt.want, r.Data)
			})
		}
	})
	t.Run("Series", func(t *testing.T) {
		t.Run("All", func(t *testing.T) {
			a := require.New(t)

			r, err := c.Series(ctx, lokiapi.SeriesParams{
				// Always sending time range because default is current time.
				Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
				End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
			})
			a.NoError(err)

			names := make(map[string]struct{}, len(set.Labels))
			for _, series := range r.Data {
				for k := range series {
					names[k] = struct{}{}
				}
			}

			// oteldb does not return these fields as part of series
			// because they have too high cardinality.
			for _, name := range []string{
				logstorage.LabelTraceID,
				logstorage.LabelSpanID,
				logstorage.LabelBody,
			} {
				names[name] = struct{}{}
			}

			a.ElementsMatch(maps.Keys(names), maps.Keys(set.Labels))
		})
		t.Run("OneMatcher", func(t *testing.T) {
			a := require.New(t)

			r, err := c.Series(ctx, lokiapi.SeriesParams{
				// Always sending time range because default is current time.
				Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
				End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				Match: []string{`{http_method="GET"}`},
			})
			a.NoError(err)

			for _, series := range r.Data {
				a.Equal("GET", series["http_method"])
			}
		})
		t.Run("Matchers", func(t *testing.T) {
			a := require.New(t)

			r, err := c.Series(ctx, lokiapi.SeriesParams{
				// Always sending time range because default is current time.
				Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
				End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
				Match: []string{`{http_method="GET"}`, `{http_method="POST"}`},
			})
			a.NoError(err)

			for _, series := range r.Data {
				a.Contains([]string{"GET", "POST"}, series["http_method"])
			}
		})
	})
	t.Run("LogQueries", func(t *testing.T) {
		tests := []struct {
			query   string
			entries int
		}{
			// Label matchers.
			// By trace id.
			{`{trace_id="af36000000000000c517000000000003"}`, 1},
			{`{trace_id="AF36000000000000C517000000000003"}`, 1},
			{`{trace_id=~"AF3600.+000C517000.+00003"}`, 1},
			{`{trace_id="badbadbadbadbadbaddeadbeafbadbad"}`, 0},
			{`{trace_id=~"bad.+"}`, 0},
			// By severity.
			{`{level="Info"}`, 121},
			{`{level="INFO"}`, 121},
			{`{level=~"I.+"}`, 121},
			{`{level!~"(WARN|DEBUG)"}`, 123},
			{`{level=~"(WARN|DEBUG)"}`, 0},
			// All by service name.
			{`{service_name="testService"}`, len(set.Records)},
			{`{service_name=~"test.+"}`, len(set.Records)},
			// Effectively match GET.
			{`{http_method="GET"}`, 21},
			{`{http_method=~".*GET.*"}`, 21},
			{`{http_method=~"^GET$"}`, 21},
			{`{http_method!~"(HEAD|POST|DELETE|PUT|PATCH|TRACE|OPTIONS)"}`, 21},
			// Also with dots.
			{`{http.method="GET"}`, 21},
			{`{http.method=~".*GET.*"}`, 21},
			{`{http.method=~"^GET$"}`, 21},
			{`{http.method!~"(HEAD|POST|DELETE|PUT|PATCH|TRACE|OPTIONS)"}`, 21},
			// Try other methods.
			{`{http_method="DELETE"}`, 20},
			{`{http_method="GET"}`, 21},
			{`{http_method="HEAD"}`, 22},
			{`{http_method="PATCH"}`, 19},
			{`{http_method="POST"}`, 21},
			{`{http_method="PUT"}`, 20},
			{`{http_method="GET"} | json`, 21},
			// Negative label matcher.
			{`{http_method!="HEAD"}`, len(set.Records) - 22},
			{`{http_method!~"^HEAD$"}`, len(set.Records) - 22},
			// Multiple lables.
			{`{http_method="HEAD",http_status_code="500"}`, 2},
			{`{http_method="HEAD",http_status_code=~"^500$"}`, 2},
			{`{http_method=~".*HEAD.*",http_status_code=~"^500$"}`, 2},
			// Also with dots.
			{`{http.method="HEAD",http.status_code="500"}`, 2},
			{`{http.method="HEAD",http.status_code=~"^500$"}`, 2},
			{`{http.method=~".*HEAD.*",http.status_code=~"^500$"}`, 2},

			// Line filter.
			{`{http_method=~".+"} |= "GET"`, 21},
			{`{http_method=~".+"} |= "DELETE"`, 20},
			{`{http_method=~".+"} |= "HEAD" |= " 500 "`, 2},
			{`{http_method=~".+"} |~ "DELETE"`, 20},
			{`{http_method=~".+"} |~ "HEAD" |= " 500 "`, 2},
			{`{http_method=~".+"} |~ "(GET|HEAD)"`, 43},
			{`{http_method=~".+"} |~ "GE.+"`, 21},
			// Try to not use offloading.
			{`{http_method=~".+"} | line_format "{{ __line__ }}" |= "DELETE"`, 20},
			{`{http_method=~".+"} | line_format "{{ __line__ }}" |= "HEAD" |= " 500 "`, 2},
			{`{http_method=~".+"} |= "HEAD" | line_format "{{ __line__ }}" |= " 500 "`, 2},
			// Negative line matcher.
			{`{http_method=~".+"} != "HEAD"`, len(set.Records) - 22},
			{`{http_method=~".+"} !~ "HEAD"`, len(set.Records) - 22},
			// Trace to logs.
			{`{http_method=~".+"} |= "af36000000000000c517000000000003"`, 1}, // lower case
			{`{http_method=~".+"} |= "AF36000000000000C517000000000003"`, 1}, // upper case
			{`{http_method=~".+"} |= "aF36000000000000c517000000000003"`, 1}, // mixed case

			// Label filter.
			{`{http_method=~".+"} | http_method = "GET"`, 21},
			{`{http_method=~".+"} | http_method = "HEAD", http_status_code = "500"`, 2},
			// Number of lines per protocol.
			//
			// 	"HTTP/1.0" 55
			// 	"HTTP/1.1" 10
			// 	"HTTP/2.0" 58
			//
			{`{http_method=~".+"} | json | protocol = "HTTP/1.0"`, 55},
			{`{http_method=~".+"} | json | protocol = "HTTP/1.1"`, 10},
			{`{http_method=~".+"} | json | protocol = "HTTP/2.0"`, 58},
			{`{http_method=~".+"} | json | protocol =~ "HTTP/1.\\d"`, 55 + 10},
			{`{http_method=~".+"} | json | protocol != "HTTP/2.0"`, 55 + 10},
			{`{http_method=~".+"} | json | protocol !~ "HTTP/2.\\d"`, 55 + 10},
			// IP label filter.
			{`{http_method="HEAD"} | client_address = "236.7.233.166"`, 1},
			{`{http_method="HEAD"} | client_address = ip("236.7.233.166")`, 1},
			{`{http_method="HEAD"} | client_address = ip("236.7.233.0/24")`, 1},
			{`{http_method="HEAD"} | client_address = ip("236.7.233.0-236.7.233.255")`, 1},

			// Distinct filter.
			{`{http_method=~".+"} | distinct http_method`, 6},
			{`{http_method=~".+"} | distinct protocol`, 3},

			// Sure empty queries.
			{`{http_method="GET"} | http_method != "GET"`, 0},
			{`{http_method="HEAD"} | clearly_not_exist > 0`, 0},
		}

		for i, tt := range tests {
			tt := tt
			t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
				t.Parallel()

				defer func() {
					if t.Failed() {
						t.Logf("query: \n%s", tt.query)
					}
				}()

				for _, direction := range []lokiapi.Direction{
					lokiapi.DirectionForward,
					lokiapi.DirectionBackward,
				} {
					t.Run(string(direction), testLogQuery(c, LogQueryTest{
						Query:           tt.query,
						Set:             set,
						Direction:       direction,
						ExpectedEntries: tt.entries,
					}))
				}
			})
		}
	})
	t.Run("QueryLimit", func(t *testing.T) {
		for i, tt := range []struct {
			query string
		}{
			{`{http_method="HEAD"}`},
			{`{http_method="HEAD"} | json | line_format "{{ . }}"`},
		} {
			tt := tt
			t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
				t.Parallel()

				defer func() {
					if t.Failed() {
						t.Logf("query: \n%s", tt.query)
					}
				}()

				limit := 1
				resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
					Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
					Query: tt.query,
					Limit: lokiapi.NewOptInt(limit),
				})
				require.NoError(t, err)

				streams, ok := resp.Data.GetStreamsResult()
				require.True(t, ok)

				var entries int
				for _, stream := range streams.Result {
					entries += len(stream.Values)
				}
				require.Equal(t, limit, entries)
			})
		}
	})
	t.Run("MetricQueries", func(t *testing.T) {
		resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: `sum by (http_method) ( count_over_time({http_method=~".+"} [30s]) )`,
			// Query all data in a one step.
			Start: lokiapi.NewOptLokiTime(asLokiTime(set.End)),
			End:   lokiapi.NewOptLokiTime(asLokiTime(set.End + otelstorage.Timestamp(10*time.Second))),
			Step:  lokiapi.NewOptPrometheusDuration("30s"),
			Limit: lokiapi.NewOptInt(1000),
		})
		require.NoError(t, err)

		data, ok := resp.Data.GetMatrixResult()
		require.True(t, ok)
		matrix := data.Result
		require.NotEmpty(t, matrix)

		methods := map[string]string{}
		for _, series := range matrix {
			labels := series.Metric.Value
			assert.Contains(t, labels, "http_method")
			assert.Len(t, labels, 1)
			method := labels["http_method"]

			values := series.Values
			assert.Len(t, values, 1)

			methods[method] = values[0].V
		}

		expect := map[string]string{
			"GET":    "21",
			"HEAD":   "22",
			"DELETE": "20",
			"PUT":    "20",
			"POST":   "21",
			"PATCH":  "19",
		}
		assert.Equal(t, expect, methods)
	})
}

func asLokiTime(ts otelstorage.Timestamp) lokiapi.LokiTime {
	format := ts.AsTime().Format(time.RFC3339Nano)
	return lokiapi.LokiTime(format)
}

func labelSetHasAttrs(t assert.TestingT, set lokiapi.LabelSet, attrs pcommon.Map) {
	// Do not check length, since label set may contain some parsed labels.
	attrs.Range(func(k string, v pcommon.Value) bool {
		k = otelstorage.KeyToLabel(k)
		assert.Contains(t, set, k)
		assert.Equal(t, v.AsString(), set[k])
		return true
	})
}

type LogQueryTest struct {
	Query           string
	Set             *lokie2e.BatchSet
	Direction       lokiapi.Direction
	ExpectedEntries int
}

func testLogQuery(c *lokiapi.Client, params LogQueryTest) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			if t.Failed() {
				t.Logf("Query: %s", params.Query)
			}
		}()

		ctx := context.Background()

		if d, ok := t.Deadline(); ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, d)
			defer cancel()
		}

		resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: params.Query,
			// Always sending time range because default is current time.
			Start:     lokiapi.NewOptLokiTime(asLokiTime(params.Set.Start)),
			End:       lokiapi.NewOptLokiTime(asLokiTime(params.Set.End)),
			Direction: lokiapi.NewOptDirection(params.Direction),
			Limit:     lokiapi.NewOptInt(1000),
		})
		require.NoError(t, err)

		streams, ok := resp.Data.GetStreamsResult()
		require.True(t, ok)

		entries := 0
		for _, stream := range streams.Result {
			values := stream.Values
			for i, entry := range values {
				entries++

				if i > 0 {
					prevEntry := values[i-1]
					switch d := params.Direction; d {
					case lokiapi.DirectionForward:
						assert.LessOrEqual(t, prevEntry.T, entry.T)
					case lokiapi.DirectionBackward:
						assert.GreaterOrEqual(t, prevEntry.T, entry.T)
					default:
						t.Fatalf("unexpected direction %q", d)
					}
				}

				record, ok := params.Set.Records[pcommon.Timestamp(entry.T)]
				require.Truef(t, ok, "can't find log record %d", entry.T)

				line := record.Body().AsString()
				assert.Equal(t, line, entry.V)

				labelSetHasAttrs(t, stream.Stream.Value, record.Attributes())
			}
		}
		require.Equal(t, params.ExpectedEntries, entries)
	}
}
