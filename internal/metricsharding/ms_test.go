package metricsharding

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"

	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

const (
	totalTestPoints = 1_000_000_000 // 1B
	uniqueRate      = 5_000         // each 50K
	totalBatches    = totalTestPoints / uniqueRate
)

func TestFoo(t *testing.T) {
	ctx := context.Background()

	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:                 "localhost:8000",
		Token:                 "admin",
		DisableProxyDiscovery: true,
	})
	require.NoError(t, err)

	const tenantID = 222
	sharder := NewSharder(yc, ShardingOptions{})
	if err := sharder.CreateTenant(ctx, tenantID, time.Now()); err != nil {
		t.Fatal(err)
	}

	tenant := sharder.tenantPath(tenantID)
	active := tenant.Child(`active`)
	t.Logf("active: %#q", active)

	now := time.Now()
	for i := 0; i < totalBatches; i++ {
		var points []any
		rh := sha1.Sum([]byte(fmt.Sprintf("r%d", i)))
		ah := sha1.Sum([]byte(fmt.Sprintf("a%d", i)))
		for j := 0; j < uniqueRate; j++ {
			delta := time.Duration(i+j) * time.Millisecond
			ts := now.Add(delta)
			points = append(points, metricstorage.Point{
				Metric:        "foo",
				ResourceHash:  rh[:],
				AttributeHash: ah[:],
				Timestamp:     otelstorage.NewTimestampFromTime(ts),
				Point:         float64(j),
			})
		}
		if err := yc.InsertRows(ctx, active.Child("points"), points, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClickHouse(t *testing.T) {
	ctx := context.Background()
	c, err := ch.Dial(ctx, ch.Options{})
	require.NoError(t, err)

	ddl := ch.Query{
		Body: `CREATE TABLE IF NOT EXISTS metrics
(
    name       LowCardinality(String),
    resource   UInt128,
    attributes UInt128,
    timestamp  DateTime64(9) CODEC(DoubleDelta),
    value      Float64 CODEC(Gorilla)
)
    ENGINE = MergeTree()
    PARTITION BY toYearWeek(timestamp)
    ORDER BY (name, resource, attributes, timestamp);`,
	}
	require.NoError(t, c.Do(ctx, ddl))

	var (
		cName     = new(proto.ColStr).LowCardinality()
		cResource = new(proto.ColUInt128)
		cAttr     = new(proto.ColUInt128)
		cTime     = new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano)
		cValue    = new(proto.ColFloat64)
	)
	rows := proto.Input{
		{Name: "name", Data: cName},
		{Name: "resource", Data: cResource},
		{Name: "attributes", Data: cAttr},
		{Name: "timestamp", Data: cTime},
		{Name: "value", Data: cValue},
	}
	var total int
	rnd := rand.New(rand.NewSource(1))
	fill := func() {
		rows.Reset()
		ts := time.Now()
		res := proto.UInt128{
			Low:  rnd.Uint64(),
			High: rnd.Uint64(),
		}
		attr := proto.UInt128{
			Low:  rnd.Uint64(),
			High: rnd.Uint64(),
		}
		for i := 0; i < uniqueRate; i++ {
			cName.Append("foo")
			cResource.Append(res)
			cAttr.Append(attr)
			cTime.Append(ts.Add(time.Duration(i) * time.Millisecond))
			cValue.Append(float64(i))
		}
	}
	fill()
	q := ch.Query{
		Body:  rows.Into("metrics"),
		Input: rows,
		OnInput: func(ctx context.Context) error {
			total++
			if total >= totalBatches {
				return io.EOF
			}
			fill()
			return nil
		},
	}
	if err := c.Do(ctx, q); err != nil {
		t.Fatal(err)
	}
}
