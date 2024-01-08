package main

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriter_Encode(t *testing.T) {
	buf := &bytes.Buffer{}
	inputs := [][]byte{
		[]byte("hello"),
		[]byte("world"),
	}
	w := &Writer{writer: buf}
	for _, data := range inputs {
		require.NoError(t, w.Encode(data))
	}
	t.Run("Reader", func(t *testing.T) {
		r := NewReader(bytes.NewReader(buf.Bytes()))
		var result [][]byte
		for r.Decode() {
			data := r.Data()
			result = append(result, slices.Clone(data))
		}
		require.NoError(t, r.Err())
		require.Equal(t, inputs, result)
	})
}
