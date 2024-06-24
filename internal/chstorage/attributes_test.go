package chstorage

import (
	"bytes"
	"testing"
	"unicode"

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

func testMap() pcommon.Map {
	m := pcommon.NewMap()
	m.PutStr("net.transport", "ip_tcp")
	m.PutStr("net.sock.family", "inet")
	m.PutStr("net.sock.host.addr", "192.168.210.83")
	m.PutStr("net.host.name", "shop-backend.local")
	m.PutStr("http.flavor", "1.1")
	m.PutStr("http.method", "PUT")
	m.PutInt("http.status_code", 204)
	m.PutStr("http.url", "https://shop-backend.local:8409/article-to-cart")
	m.PutStr("http.scheme", "https")
	m.PutStr("http.target", "/article-to-cart")
	m.PutInt("http.response_content_length", 937939)
	m.PutInt("http.request_content_length", 39543)
	return m
}

func TestEncodeAttributes(t *testing.T) {
	data := encodeAttributes(testMap(), [2]string{"le", "50"})
	require.JSONEq(t, `{
"net.transport": "ip_tcp",
"net.sock.family": "inet",
"net.sock.host.addr": "192.168.210.83",
"net.host.name": "shop-backend.local",
"http.flavor": "1.1",
"http.method": "PUT",
"http.status_code": 204,
"http.url": "https://shop-backend.local:8409/article-to-cart",
"http.scheme": "https",
"http.target": "/article-to-cart",
"http.response_content_length": 937939,
"http.request_content_length": 39543,
"le": "50"
	}`, string(data))

	// See https://clickhouse.com/docs/en/sql-reference/functions/json-functions#simplejson-visitparam-functions.
	require.False(t,
		bytes.ContainsFunc(data, unicode.IsSpace),
		"ensure that resulting JSON is simpleJSON-compatible",
	)
}
