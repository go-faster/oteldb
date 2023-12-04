package logparser

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-faster/jx"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
)

func TestGenericJSONParser_Parse(t *testing.T) {
	files, err := os.ReadDir(filepath.Join("_testdata", "genericjson"))
	require.NoError(t, err, "read testdata")

	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("_testdata", "genericjson", file.Name()))
			require.NoError(t, err, "read testdata")

			var parser GenericJSONParser

			scanner := bufio.NewScanner(bytes.NewReader(data))

			var i int
			for scanner.Scan() {
				s := strings.TrimSpace(scanner.Text())
				if s == "" {
					continue
				}
				i++
				t.Run(fmt.Sprintf("Line%02d", i), func(t *testing.T) {
					t.Logf("%s", s)
					if !jx.Valid(scanner.Bytes()) {
						t.Fatal("invalid")
					}
					line, err := parser.Parse([]byte(s))
					require.NoError(t, err, "parse")
					name := fmt.Sprintf("genericjson_%s_%02d.json",
						strings.TrimSuffix(file.Name(), filepath.Ext(file.Name())), i,
					)
					gold.Str(t, line.String(), name)
				})
			}
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
