package tempoe2e_test

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/chstorage"
)

func TestCH(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dsn := os.Getenv("E2E_CH_DSN")
	if dsn == "" {
		t.Skip("Set E2E_CH_DSN to run")
	}

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	pass, _ := u.User.Password()
	opts := ch.Options{
		Address:  u.Host,
		Database: strings.TrimPrefix(u.Path, "/"),
		User:     u.User.Username(),
		Password: pass,
	}

	connectBackoff := backoff.NewExponentialBackOff()
	connectBackoff.InitialInterval = 2 * time.Second
	connectBackoff.MaxElapsedTime = time.Minute
	c, err := backoff.RetryWithData(func() (*chpool.Pool, error) {
		c, err := chpool.Dial(ctx, chpool.Options{
			ClientOptions: opts,
		})
		if err != nil {
			return nil, errors.Wrap(err, "dial")
		}
		return c, nil
	}, connectBackoff)
	if err != nil {
		t.Fatal(err)
	}

	prefix := "traces_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	tables := chstorage.Tables{
		Spans: prefix + "_spans",
		Tags:  prefix + "_tags",
	}
	t.Logf("Test tables prefix: %s", prefix)
	require.NoError(t, tables.Create(ctx, c))

	inserter := chstorage.NewInserter(c, tables)
	querier := chstorage.NewQuerier(c, tables)
	runTest(ctx, t, inserter, querier)
}
