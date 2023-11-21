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
}
