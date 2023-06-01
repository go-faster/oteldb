package ytquery

import (
	"strconv"
	"strings"

	"go.ytsaurus.tech/yt/go/yson"
)

type placeholderSet struct {
	values []any
}

func (p *placeholderSet) writePlaceholder(sb *strings.Builder, v any) {
	p.values = append(p.values, v)
	idx := len(p.values) - 1

	sb.WriteByte('{')
	sb.WriteString("placeholder")
	writeInt(sb, idx)
	sb.WriteByte('}')
}

func (p placeholderSet) MarshalYSON(w *yson.Writer) error {
	buf := make([]byte, 0, 32)
	w.BeginMap()
	for i, v := range p.values {
		buf = buf[:0]
		buf = append(buf, "placeholder"...)
		buf = strconv.AppendInt(buf, int64(i), 10)

		w.MapKeyBytes(buf)
		w.Any(v)
	}
	w.EndMap()
	return nil
}
