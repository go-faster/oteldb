//go:build !purego && !appengine

package logqlengine

import (
	"unsafe"

	"github.com/go-faster/jx"
)

func decodeStr(s string) *jx.Decoder {
	data := unsafe.Slice(unsafe.StringData(s), len(s)) // #nosec: G103
	return jx.DecodeBytes(data)
}
