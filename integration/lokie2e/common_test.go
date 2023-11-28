package lokie2e_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/integration/lokie2e"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokihandler"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func readBatchSet(p string) (s lokie2e.BatchSet, _ error) {
	f, err := os.Open(p)
	if err != nil {
		return s, err
	}
	defer func() {
		_ = f.Close()
	}()
	return lokie2e.ParseBatchSet(f)
}

func setupDB(
	ctx context.Context,
	t *testing.T,
	set lokie2e.BatchSet,
	inserter logstorage.Inserter,
	querier logstorage.Querier,
	engineQuerier logqlengine.Querier,
) *lokiapi.Client {
	consumer := logstorage.NewConsumer(inserter)
	for i, b := range set.Batches {
		if err := consumer.ConsumeLogs(ctx, b); err != nil {
			t.Fatalf("Send batch %d: %+v", i, err)
		}
	}

	engine := logqlengine.NewEngine(engineQuerier, logqlengine.Options{
		ParseOptions: logql.ParseOptions{AllowDots: true},
	})

	api := lokihandler.NewLokiAPI(querier, engine)
	lokih, err := lokiapi.NewServer(api)
	require.NoError(t, err)

	s := httptest.NewServer(lokih)
	t.Cleanup(s.Close)

	c, err := lokiapi.NewClient(s.URL, lokiapi.WithClient(s.Client()))
	require.NoError(t, err)
	return c
}

func runTest(
	ctx context.Context,
	t *testing.T,
	inserter logstorage.Inserter,
	querier logstorage.Querier,
	engineQuerier logqlengine.Querier,
) {
	set, err := readBatchSet("_testdata/logs.jsonl")
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Labels)
	require.NotEmpty(t, set.Records)
	require.NotZero(t, set.Start)
	require.NotZero(t, set.End)
	require.GreaterOrEqual(t, set.End, set.Start)
	c := setupDB(ctx, t, set, inserter, querier, engineQuerier)

	t.Run("Labels", func(t *testing.T) {
		a := require.New(t)

		r, err := c.Labels(ctx, lokiapi.LabelsParams{
			// Always sending time range because default is current time.
			Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
			End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
		})
		a.NoError(err)
		a.Len(r.Data, len(set.Labels))
		for _, label := range r.Data {
			a.Contains(set.Labels, label)
		}
	})
	t.Run("LabelValues", func(t *testing.T) {
		a := require.New(t)

		for labelName, labels := range set.Labels {
			labelValue := map[string]struct{}{}
			for _, t := range labels {
				labelValue[t.Value] = struct{}{}
			}

			r, err := c.LabelValues(ctx, lokiapi.LabelValuesParams{
				Name: labelName,
				// Always sending time range because default is current time.
				Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
				End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
			})
			a.NoError(err)
			a.Len(r.Data, len(labelValue))
			for _, val := range r.Data {
				a.Containsf(labelValue, val, "check label %q", labelName)
			}
		}
	})
	t.Run("LogQueries", func(t *testing.T) {
		// Example JQ expression to make testdata queries:
		//
		// 	.resourceLogs[].scopeLogs[].logRecords[]
		// 		| .body.stringValue
		// 		| fromjson
		// 		| select(.method=="GET")
		//
		tests := []struct {
			query   string
			entries int
		}{
			// Label matchers.
			// Effectively match GET.
			{`{http.method="GET"}`, 21},
			{`{http.method=~".*GET.*"}`, 21},
			{`{http.method=~"^GET$"}`, 21},
			{`{http.method!~"(HEAD|POST|DELETE|PUT|PATCH|TRACE|OPTIONS)"}`, 21},
			// Try other methods.
			{`{http.method="DELETE"}`, 20},
			{`{http.method="GET"}`, 21},
			{`{http.method="HEAD"}`, 22},
			{`{http.method="PATCH"}`, 19},
			{`{http.method="POST"}`, 21},
			{`{http.method="PUT"}`, 20},
			{`{http.method="GET"} | json`, 21},
			// Negative label matcher.
			{`{http.method!="HEAD"}`, len(set.Records) - 22},
			{`{http.method!~"^HEAD$"}`, len(set.Records) - 22},
			// Multiple lables.
			{`{http.method="HEAD",http.status="500"}`, 2},
			{`{http.method="HEAD",http.status=~"^500$"}`, 2},
			{`{http.method=~".*HEAD.*",http.status=~"^500$"}`, 2},

			// Line filter.
			{`{http.method=~".+"} |= "\"method\": \"GET\""`, 21},
			{`{http.method=~".+"} |= "\"method\": \"DELETE\""`, 20},
			{`{http.method=~".+"} |= "\"method\": \"HEAD\"" |= "\"status\":500"`, 2},
			{`{http.method=~".+"} |~ "\"method\":\\s*\"DELETE\""`, 20},
			{`{http.method=~".+"} |~ "\"method\":\\s*\"HEAD\"" |= "\"status\":500"`, 2},
			// Try to not use offloading.
			{`{http.method=~".+"} | line_format "{{ __line__ }}" |= "\"method\": \"DELETE\""`, 20},
			{`{http.method=~".+"} | line_format "{{ __line__ }}" |= "\"method\": \"HEAD\"" |= "\"status\":500"`, 2},
			{`{http.method=~".+"} |= "\"method\": \"HEAD\"" | line_format "{{ __line__ }}" |= "\"status\":500"`, 2},
			// Negative line matcher.
			{`{http.method=~".+"} != "\"method\": \"HEAD\""`, len(set.Records) - 22},
			{`{http.method=~".+"} !~ "\"method\":\\s*\"HEAD\""`, len(set.Records) - 22},
			// IP line filter.
			{`{http.method="HEAD"} |= ip("236.7.233.166")`, 1},

			// Label filter.
			{`{http.method=~".+"} | http.method = "GET"`, 21},
			{`{http.method=~".+"} | http.method = "HEAD", http.status = "500"`, 2},
			// Number of lines per protocol.
			//
			// 	"HTTP/1.0" 55
			// 	"HTTP/1.1" 38
			// 	"HTTP/2.0" 30
			//
			{`{http.method=~".+"} | json | protocol = "HTTP/1.0"`, 55},
			{`{http.method=~".+"} | json | protocol =~ "HTTP/1.\\d"`, 55 + 38},
			{`{http.method=~".+"} | json | protocol != "HTTP/2.0"`, 55 + 38},
			{`{http.method=~".+"} | json | protocol !~ "HTTP/2.\\d"`, 55 + 38},
			// IP label filter.
			{`{http.method="HEAD"} | json | host = "236.7.233.166"`, 1},
			{`{http.method="HEAD"} | json | host == ip("236.7.233.166")`, 1},
			{`{http.method="HEAD"} | json | host == ip("236.7.233.0/24")`, 1},
			{`{http.method="HEAD"} | json | host == ip("236.7.233.0-236.7.233.255")`, 1},

			// Distinct filter.
			{`{http.method=~".+"} | distinct http.method`, 6},
			{`{http.method=~".+"} | json | distinct method`, 6},
			{`{http.method=~".+"} | json | distinct protocol`, 3},

			// Sure empty queries.
			{`{http.method="GET"} | json | http.method != "GET"`, 0},
			{`{http.method="HEAD"} | clearly_not_exist > 0`, 0},
		}
		labelSetHasAttrs := func(t assert.TestingT, set lokiapi.LabelSet, attrs pcommon.Map) {
			// Do not check length, since label set may contain some parsed labels.
			attrs.Range(func(k string, v pcommon.Value) bool {
				assert.Contains(t, set, k)
				assert.Equal(t, v.AsString(), set[k])
				return true
			})
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

				resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
					Query: tt.query,
					// Always sending time range because default is current time.
					Start: lokiapi.NewOptLokiTime(asLokiTime(set.Start)),
					End:   lokiapi.NewOptLokiTime(asLokiTime(set.End)),
					Limit: lokiapi.NewOptInt(1000),
				})
				require.NoError(t, err)

				streams, ok := resp.Data.GetStreamsResult()
				require.True(t, ok)

				entries := 0
				for _, stream := range streams.Result {
					for _, entry := range stream.Values {
						entries++

						record, ok := set.Records[pcommon.Timestamp(entry.T)]
						require.Truef(t, ok, "can't find log record %d", entry.T)

						line := record.Body().AsString()
						assert.Equal(t, line, entry.V)

						labelSetHasAttrs(t, stream.Stream.Value, record.Attributes())
					}
				}
				require.Equal(t, tt.entries, entries)
			})
		}
	})
	t.Run("MetricQueries", func(t *testing.T) {
		resp, err := c.QueryRange(ctx, lokiapi.QueryRangeParams{
			Query: `sum by (http.method) ( count_over_time({http.method=~".+"} [30s]) )`,
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
			assert.Contains(t, labels, "http.method")
			assert.Len(t, labels, 1)
			method := labels["http.method"]

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
