package metricsharding

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"

	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

func TestIntegrationWorkload(t *testing.T) {
	if os.Getenv("E2E_BENCH") != "1" {
		t.Skip()
	}

	const (
		totalTestPoints = 1_000_000_000 // 1B
		uniqueRate      = 5_000         // each 50K
		totalBatches    = totalTestPoints / uniqueRate
	)
	ctx := context.Background()
	yc, err := ytrpc.NewClient(&yt.Config{
		RPCProxy:              "localhost:8002",
		Token:                 "admin",
		DisableProxyDiscovery: true,
	})
	require.NoError(t, err)

	const tenantID = 222
	sharder := NewSharder(yc, ShardingOptions{})
	if err := sharder.CreateTenant(ctx, tenantID, time.Now()); err != nil {
		t.Fatal(err)
	}

	tenant := sharder.shardOpts.TenantPath(tenantID)
	active := tenant.Child(`active`)
	t.Logf("active: %#q", active)

	now := time.Now()

	rnd := rand.New(rand.NewSource(1))

	for i := 0; i < totalBatches; i++ {
		var points []any
		var rh, ah otelstorage.Hash
		rnd.Read(rh[:])
		rnd.Read(ah[:])
		for j := 0; j < uniqueRate; j++ {
			delta := time.Duration(i+j) * time.Millisecond
			ts := now.Add(delta)
			points = append(points, metricstorage.Point{
				Metric:        "foo",
				ResourceHash:  rh,
				AttributeHash: ah,
				Timestamp:     otelstorage.NewTimestampFromTime(ts),
				Point:         float64(j),
			})
		}
		if err := yc.InsertRows(ctx, active.Child("points"), points, nil); err != nil {
			t.Fatal(err)
		}
	}
}
