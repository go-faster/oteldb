package jsonexpr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectorTree(t *testing.T) {
	require.True(t, MakeSelectorTree(nil).IsEmpty())
}
