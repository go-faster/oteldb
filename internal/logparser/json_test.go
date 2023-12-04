package logparser

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
)

func TestGenericJSONParser_Parse(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("_testdata", "genericjson", "zap.jsonl"))
	require.NoError(t, err, "read testdata")

	var parser GenericJSONParser
	scanner := bufio.NewScanner(bytes.NewReader(data))

	var i int
	for scanner.Scan() {
		i++
		t.Run(fmt.Sprintf("Line%02d", i), func(t *testing.T) {
			line, err := parser.Parse(scanner.Bytes())
			require.NoError(t, err, "parse")

			name := fmt.Sprintf("genericjson_%02d.json", i)
			gold.Str(t, line.String(), name)
		})
	}
}

func BenchmarkGenericJSONParser_Parse(b *testing.B) {
	b.ReportAllocs()
	data, err := os.ReadFile(filepath.Join("_testdata", "genericjson", "zap.jsonl"))
	require.NoError(b, err, "read testdata")

	var parser GenericJSONParser
	scanner := bufio.NewScanner(bytes.NewReader(data))

	var i int
	b.ResetTimer()
	for scanner.Scan() {
		i++
		b.Run(fmt.Sprintf("Line%02d", i), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(scanner.Bytes())))

			for j := 0; j < b.N; j++ {
				_, err := parser.Parse(scanner.Bytes())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
