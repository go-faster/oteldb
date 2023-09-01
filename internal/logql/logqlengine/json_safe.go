//go:build purego || appengine

package logqlengine

import "github.com/go-faster/jx"

func decodeStr(s string) *jx.Decoder {
	return jx.DecodeStr(s)
}
