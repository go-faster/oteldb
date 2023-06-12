package prome2e

import (
	"context"
	_ "embed"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/go-faster/oteldb/internal/promapi"
)

type jxEncode interface {
	Encode(e *jx.Encoder)
}

func printJSON(t *testing.T, v jxEncode, name string) {
	var e jx.Encoder
	e.SetIdent(2)
	v.Encode(&e)

	t.Logf("%s:\n%s", name, e.String())
}

//go:embed prometheus.yml
var prometheusConfig []byte

func TestPrometheusOAS(t *testing.T) {
	ctx := context.Background()
	// Provide config to prometheus testcontainer.

	dir := t.TempDir()
	configPath := filepath.Join(dir, "prometheus.yml")
	require.NoError(t, os.WriteFile(configPath, prometheusConfig, 0644))

	req := testcontainers.ContainerRequest{
		Image:        "prom/prometheus:v2.44.0",
		ExposedPorts: []string{"9090/tcp"},
		Cmd: []string{
			"--log.level=debug",
			"--config.file=/etc/prometheus/prometheus.yml",
			"--storage.tsdb.path=/prometheus",
			"--web.console.libraries=/usr/share/prometheus/console_libraries",
			"--web.console.templates=/usr/share/prometheus/consoles",
		},
		WaitingFor: wait.ForLog("Server is ready to receive web requests"),
		Mounts: testcontainers.ContainerMounts{
			{
				Source: testcontainers.GenericBindMountSource{
					HostPath: configPath,
				},
				Target:   "/etc/prometheus/prometheus.yml",
				ReadOnly: true,
			},
		},
	}
	promContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Logger:           testcontainers.TestLogger(t),
	})
	require.NoError(t, err, "container start")
	t.Cleanup(func() {
		require.NoError(t, promContainer.Terminate(context.Background()), "container terminate")
	})

	endpoint, err := promContainer.Endpoint(ctx, "")
	require.NoError(t, err, "container endpoint")

	u := url.URL{
		Scheme: "http",
		Host:   endpoint,
	}

	api, err := promapi.NewClient(u.String())
	require.NoError(t, err, "api client")

	{
		res, err := api.GetQuery(ctx, promapi.GetQueryParams{
			Query: "go_info{}",
		})
		require.NoError(t, err, "api query")
		require.True(t, res.Data.IsVector(), "should be scalar")
		printJSON(t, res.Data, "blank query result")
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	bo := backoff.WithContext(backoff.NewConstantBackOff(time.Millisecond*100), ctx)
	v, err := backoff.RetryWithData(func() (*promapi.Vector, error) {
		res, err := api.GetQuery(ctx, promapi.GetQueryParams{
			Query: "go_info{}",
		})
		if err != nil {
			return nil, errors.Wrap(err, "api query")
		}
		v, ok := res.Data.GetVector()
		if !ok {
			return nil, backoff.Permanent(errors.New("not a vector"))
		}
		if len(v.Result) == 0 {
			// Waiting to be scraped.
			return nil, errors.New("empty vector")
		}
		return &v, nil
	}, bo)
	require.NoError(t, err, "api query")
	require.Len(t, v.Result, 1, "should be one result")
	printJSON(t, v, "query result")

	vr := v.Result[0]
	assert.Equal(t, "go_info", vr.Metric["__name__"], "metric name")
	assert.Greater(t, vr.Value.T, 0.0, "value")
	assert.NotEmpty(t, vr.Value.V)
}
