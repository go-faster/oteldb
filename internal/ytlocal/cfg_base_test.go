package ytlocal

import (
	"bytes"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

func newBaseServer() BaseServer {
	return BaseServer{
		RPCPort:        9015,
		MonitoringPort: 9090,
		TimestampProvider: Connection{
			Addresses: []string{
				"1.master.yt.go-faster.org:9010",
				"2.master.yt.go-faster.org:9010",
				"3.master.yt.go-faster.org:9010",
			},
		},
		ClusterConnection: ClusterConnection{
			ClusterName: "ytlocal",
			DiscoveryConnection: Connection{
				Addresses: []string{
					"1.master.yt.go-faster.org:9020",
					"2.master.yt.go-faster.org:9020",
					"3.master.yt.go-faster.org:9020",
				},
			},
			PrimaryMaster: Connection{
				Addresses: []string{
					"1.master.yt.go-faster.org:9010",
					"2.master.yt.go-faster.org:9010",
					"3.master.yt.go-faster.org:9010",
				},
				CellID: "ffcef5128-9be15fe9-10242-ffffffcb",
			},
		},
		AddressResolver: AddressResolver{
			Retries:    1000,
			EnableIPv6: false,
			EnableIPv4: true,
		},
		Logging: Logging{},
	}
}

func encode(t *testing.T, name string, v any) {
	t.Helper()
	out := new(bytes.Buffer)
	w := yson.NewWriterFormat(out, yson.FormatPretty)
	e := yson.NewEncoderWriter(w)
	require.NoError(t, e.Encode(v))
	gold.Str(t, out.String(), name+".yson")
}
