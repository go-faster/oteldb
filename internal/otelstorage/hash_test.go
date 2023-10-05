package otelstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

var testHash = Hash{
	10, 20, 30, 40, 50, 60, 70, 80,
	80, 70, 60, 50, 40, 30, 20, 10,
}

func TestHashYSON(t *testing.T) {
	tests := []struct {
		ID Hash
	}{
		{testHash},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			id := tt.ID

			data, err := yson.Marshal(id)
			require.NoError(t, err)

			var id2 Hash
			require.NoError(t, yson.Unmarshal(data, &id2))

			require.Equal(t, id, id2)
		})
	}
	t.Run("NilCheck", func(t *testing.T) {
		var idNil *Hash
		require.ErrorContains(t,
			idNil.UnmarshalYSON(yson.NewReader(nil)),
			"can't unmarshal to",
		)
	})
}
