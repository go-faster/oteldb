package prome2e_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/integration/prome2e"
	"github.com/go-faster/oteldb/internal/otelreceiver"
	"github.com/go-faster/oteldb/internal/promapi"
	"github.com/go-faster/oteldb/internal/promhandler"
)

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
	consumer otelreceiver.MetricsConsumer,
	querier storage.Queryable,
) *promapi.Client {
	for i, b := range set.Batches {
		if err := consumer.ConsumeMetrics(ctx, b); err != nil {
			t.Fatalf("Send batch %d: %+v", i, err)
		}
	}

	engine := promql.NewEngine(promql.EngineOpts{})
	api := promhandler.NewPromAPI(engine, querier, promhandler.PromAPIOptions{})
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
	consumer otelreceiver.MetricsConsumer,
	querier storage.Queryable,
) {
	set, err := readBatchSet("_testdata/metrics.json")
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Labels)
	c := setupDB(ctx, t, set, consumer, querier)

	t.Run("Labels", func(t *testing.T) {
		t.Run("GetLabels", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetLabels(ctx, promapi.GetLabelsParams{})
			a.NoError(err)
			a.Len(r.Data, len(set.Labels))
			for _, label := range r.Data {
				a.Contains(set.Labels, label)
			}
		})
		t.Run("PostLabels", func(t *testing.T) {
			a := require.New(t)

			r, err := c.PostLabels(ctx, &promapi.LabelsForm{})
			a.NoError(err)
			a.Len(r.Data, len(set.Labels))
			for _, label := range r.Data {
				a.Contains(set.Labels, label)
			}
		})
	})
	t.Run("LabelValues", func(t *testing.T) {
		a := require.New(t)

		for labelName, valueSet := range set.Labels {
			r, err := c.GetLabelValues(ctx, promapi.GetLabelValuesParams{Label: labelName})
			a.NoError(err)
			a.Len(r.Data, len(valueSet))
			for _, val := range r.Data {
				a.Containsf(valueSet, val, "check label %q", labelName)
			}
		}
	})
	t.Run("Series", func(t *testing.T) {
		t.Run("ByName", func(t *testing.T) {
			a := require.New(t)

			r, err := c.GetSeries(ctx, promapi.GetSeriesParams{
				Start: promapi.NewOptPrometheusTimestamp(`1600000000.0`),
				End:   promapi.NewOptPrometheusTimestamp(`1800000000.0`),
				Match: []string{
					`prometheus_http_requests_total{}`,
				},
			})
			a.NoError(err)

			a.NotEmpty(r.Data)
			for _, labels := range r.Data {
				a.Equal("prometheus_http_requests_total", labels["__name__"])
			}
		})
		t.Run("Matchers", func(t *testing.T) {
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
	})
}
