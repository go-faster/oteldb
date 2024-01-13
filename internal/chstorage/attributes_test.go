package chstorage

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

func Test_attributeCol(t *testing.T) {
	var hashes []otelstorage.Hash
	col := newAttributesColumn()

	for _, s := range []string{
		"foo",
		"foo",
		"bar",
		"foo",
		"baz",
	} {
		m := pcommon.NewMap()
		m.PutStr("v", s)
		v := otelstorage.Attrs(m)
		col.Append(v)
		hashes = append(hashes, v.Hash())
	}
	for j := 0; j < 3; j++ {
		m := pcommon.NewMap()
		v := otelstorage.Attrs(m)
		col.Append(v)
		hashes = append(hashes, v.Hash())
	}

	rows := len(hashes)

	var buf proto.Buffer
	col.EncodeColumn(&buf)
	t.Run("Golden", func(t *testing.T) {
		gold.Bytes(t, buf.Buf, "col_attr")
	})
	t.Run("Ok", func(t *testing.T) {
		br := bytes.NewReader(buf.Buf)
		r := proto.NewReader(br)
		dec := newAttributesColumn()
		require.NoError(t, dec.DecodeColumn(r, rows))

		var gotHashes []otelstorage.Hash
		for i := 0; i < dec.Rows(); i++ {
			gotHashes = append(gotHashes, dec.Row(i).Hash())
		}
		require.Equal(t, hashes, gotHashes)
		require.Equal(t, rows, dec.Rows())
		dec.Reset()
		require.Equal(t, 0, dec.Rows())
		require.Equal(t, proto.ColumnTypeLowCardinality.Sub(proto.ColumnTypeString), dec.Type())
	})
}
