package ytquery

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

func Test_placeholderSet_MarshalYSON(t *testing.T) {
	tests := []struct {
		values []any
		expect string
	}{
		{nil, "{}"},
		{[]any{1}, "{placeholder0=1;}"},
		{[]any{1, "foo", true}, "{placeholder0=1;placeholder1=foo;placeholder2=%true;}"},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			p := placeholderSet{
				values: tt.values,
			}

			data, err := yson.Marshal(p)
			require.NoError(t, err)
			require.Equal(t, tt.expect, string(data))
		})
	}
}
