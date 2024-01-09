package prome2e_test

import (
	"context"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"golang.org/x/exp/maps"

	"github.com/go-faster/oteldb/integration/prome2e"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promhandler"
)

// MetricsConsumer is metrics consumer.
type MetricsConsumer interface {
	ConsumeMetrics(ctx context.Context, ld pmetric.Metrics) error
}

func readBatchSet(p string) (s prome2e.BatchSet, _ error) {
	f, err := os.Open(p)
	if err != nil {
		return s, err
	}
	defer func() {
		_ = f.Close()
	}()
	return prome2e.ParseBatchSet(f)
}

func setupDB(
	ctx context.Context,
	t *testing.T,
	set prome2e.BatchSet,
	consumer MetricsConsumer,
	querier storage.Queryable,
	exemplarQuerier storage.ExemplarQueryable,
) *promapi.Client {
	for i, b := range set.Batches {
		if err := consumer.ConsumeMetrics(ctx, b); err != nil {
			t.Fatalf("Send batch %d: %+v", i, err)
		}
	}

	engine := promql.NewEngine(promql.EngineOpts{
		Timeout:              time.Minute,
		MaxSamples:           1_000_000,
		EnableNegativeOffset: true,
	})
	api := promhandler.NewPromAPI(engine, querier, exemplarQuerier, promhandler.PromAPIOptions{})
	promh, err := promapi.NewServer(api)
	require.NoError(t, err)

	s := httptest.NewServer(promh)
	t.Cleanup(s.Close)

	c, err := promapi.NewClient(s.URL, promapi.WithClient(s.Client()))
	require.NoError(t, err)
	return c
}

func runTest(
	ctx context.Context,
	t *testing.T,
	consumer MetricsConsumer,
	querier storage.Queryable,
	exemplarQuerier storage.ExemplarQueryable,
) {
	set, err := readBatchSet("_testdata/metrics.json")
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Labels)
	c := setupDB(ctx, t, set, consumer, querier, exemplarQuerier)

	t.Run("Labels", func(t *testing.T) {
		t.Run("GetLabels", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetLabels(ctx, promapi.GetLabelsParams{})
			a.NoError(err)
			a.ElementsMatch(maps.Keys(set.Labels), []string(r.Data))
		})
		t.Run("PostLabels", func(t *testing.T) {
			a := require.New(t)

			r, err := c.PostLabels(ctx, &promapi.LabelsForm{})
			a.NoError(err)
			a.ElementsMatch(maps.Keys(set.Labels), []string(r.Data))
		})
	})
	t.Run("LabelValues", func(t *testing.T) {
		a := require.New(t)

		for labelName, valueSet := range set.Labels {
			r, err := c.GetLabelValues(ctx, promapi.GetLabelValuesParams{Label: labelName})
			a.NoError(err)
			a.ElementsMatch(maps.Keys(valueSet), []string(r.Data), "check label %q", labelName)
		}
	})
	t.Run("Series", func(t *testing.T) {
		testName := func(name string) func(t *testing.T) {
			return func(t *testing.T) {
				a := require.New(t)

				r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
					Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
					End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
					Match: []string{name + "{}"},
				})
				a.NoError(err)

				a.NotEmpty(r.Data)
				for _, labels := range r.Data {
					a.Equal(name, labels["__name__"])
				}
			}
		}
		t.Run("PointByName", testName(`prometheus_http_requests_total`))
		t.Run("HistogramByName", testName(`prometheus_http_request_duration_seconds_count`))
		t.Run("SummaryByName", testName(`go_gc_duration_seconds`))
		t.Run("PointByMappedName", testName(`process_runtime_go_gc_count`))

		t.Run("OneMatcher", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`prometheus_http_requests_total{
						code="200",
						handler=~"/api/v1.+",
						handler!="/api/v1/series",
						handler!~"/api/v1/query(_range)?"
					}`,
				},
			})
			a.NoError(err)

			a.NotEmpty(r.Data)
			for _, labels := range r.Data {
				a.Equal("200", labels["code"])

				handler := labels["handler"]
				// Check that handler=~"/api/v1.+" is satisfied.
				a.Contains(handler, "/api/v1")

				// Check that handler!="/api/v1/series" is satisfied.
				a.NotEqual("/api/v1/series", handler)

				// Check that handler!~"/api/v1/query(_range)?" is satisfied.
				a.NotEqual("/api/v1/query", handler)
				a.NotEqual("/api/v1/query_range", handler)
			}
		})
		t.Run("MultipleMatchers", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`prometheus_http_requests_total{
						handler="/api/v1/query"
					}`,
					`prometheus_http_requests_total{
						handler="/api/v1/series"
					}`,
				},
			})
			a.NoError(err)

			a.NotEmpty(r.Data)
			for _, labels := range r.Data {
				a.Contains([]string{
					"/api/v1/query",
					"/api/v1/series",
				}, labels["handler"])
			}
		})
		t.Run("OutOfRange", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1000000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1100000000.0`),
				Match: []string{
					`prometheus_http_requests_total{}`,
				},
			})
			a.NoError(err)
			a.Empty(r.Data)
		})
		t.Run("NoMatch", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`prometheus_http_requests_total{
						clearly="not_exist"
					}`,
				},
			})
			a.NoError(err)
			a.Empty(r.Data)
		})
		t.Run("InvalidTimestamp", func(t *testing.T) {
			a := require.New(t)

			_, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`abcd`),
				Match: []string{
					`prometheus_http_requests_total{}`,
				},
			})
			perr := new(promapi.FailStatusCode)
			a.ErrorAs(err, &perr)
			a.Equal(promapi.FailErrorTypeBadData, perr.Response.ErrorType)
		})
		t.Run("InvalidMatcher", func(t *testing.T) {
			a := require.New(t)

			_, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`\{\}`,
				},
			})
			perr := new(promapi.FailStatusCode)
			a.ErrorAs(err, &perr)
			a.Equal(promapi.FailErrorTypeBadData, perr.Response.ErrorType)
		})
	})
	t.Run("QueryRange", func(t *testing.T) {
		a := require.New(t)

		r, err := c.GetQueryRange(ctx, promapi.GetQueryRangeParams{
			Query: `count(prometheus_http_requests_total{})`,
			Start: getPromTS(set.Start),
			End:   getPromTS(set.End),
			Step:  "5s",
		})
		a.NoError(err)

		data := r.Data
		a.Equal(promapi.MatrixData, data.Type)

		mat := data.Matrix.Result
		a.Len(mat, 1)
		values := mat[0].Values
		a.NotEmpty(values)

		for _, point := range values {
			a.Equal(float64(51), point.V)
		}
	})
}

func getPromTS(ts pcommon.Timestamp) promapi.PrometheusTimestamp {
	v := strconv.FormatInt(ts.AsTime().Unix(), 10)
	return promapi.PrometheusTimestamp(v)
}
