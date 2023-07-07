package lokie2e_test

import (
	"context"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/integration/lokie2e"
	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokihandler"
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
	set, err := readBatchSet("_testdata/logs.json")
	require.NoError(t, err)
	require.NotEmpty(t, set.Batches)
	require.NotEmpty(t, set.Labels)
	require.NotEmpty(t, set.Records)
	c := setupDB(ctx, t, set, inserter, querier, engineQuerier)

	t.Run("Labels", func(t *testing.T) {
		a := require.New(t)

		r, err := c.Labels(ctx, lokiapi.LabelsParams{})
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

			r, err := c.LabelValues(ctx, lokiapi.LabelValuesParams{Name: labelName})
			a.NoError(err)
			a.Len(r.Data, len(labelValue))
			for _, val := range r.Data {
				a.Containsf(labelValue, val, "check label %q", labelName)
			}
		}
	})
}
