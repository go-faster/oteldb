package logqlabels

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyAggregatedLabels(t *testing.T) {
	al := AggregatedLabelsFromSet(LabelSet{}, nil, nil)
	el := EmptyAggregatedLabels()
	require.Equal(t, el.Key(), al.Key())
}
