package ytlocal

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
)

func TestJSON(t *testing.T) {
	for _, tc := range []struct {
		Name  string
		Value any
	}{
		{
			Name: "clusters-config",
			Value: ClusterConfig{
				Clusters: []UICluster{
					{
						ID:             "hahn",
						Name:           "Hahn",
						Proxy:          "http-proxy.yt.go-faster.org",
						Secure:         false,
						Theme:          "lavander",
						Authentication: "Basic",
						Group:          "YT",
						Environment:    "production",
						Description:    "First YT cluster",
						PrimaryMaster: ClusterPrimaryMaster{
							CellTag: 1,
						},
					},
				},
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			out := new(bytes.Buffer)
			e := json.NewEncoder(out)
			e.SetIndent("", "  ")
			require.NoError(t, e.Encode(tc.Value))
			gold.Str(t, out.String(), tc.Name+".json")
		})
	}
}
