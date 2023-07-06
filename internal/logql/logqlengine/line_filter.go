package logqlengine

import (
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LineFilter is a line matching Processor.
type LineFilter[M StringMatcher] struct {
	matcher M
}

func buildLineFilter(stage *logql.LineFilter) (Processor, error) {
	if stage.IP {
		return nil, &UnsupportedError{Msg: "ip line filter is not supported yet"}
	}
	switch stage.Op {
	case logql.OpEq:
		return &LineFilter[ContainsMatcher]{
			matcher: ContainsMatcher{Value: stage.Value},
		}, nil
	case logql.OpNotEq:
		return &LineFilter[NotMatcher[string, ContainsMatcher]]{
			matcher: NotMatcher[string, ContainsMatcher]{Next: ContainsMatcher{Value: stage.Value}},
		}, nil
	case logql.OpRe:
		return &LineFilter[RegexpMatcher]{
			matcher: RegexpMatcher{Re: stage.Re},
		}, nil
	case logql.OpNotRe:
		return &LineFilter[NotMatcher[string, RegexpMatcher]]{
			matcher: NotMatcher[string, RegexpMatcher]{Next: RegexpMatcher{Re: stage.Re}},
		}, nil
	default:
		return nil, errors.Errorf("unknown operation %q", stage.Op)
	}
}

// Process implements Processor.
func (lf *LineFilter[M]) Process(_ otelstorage.Timestamp, line string, _ LabelSet) (_ string, keep bool) {
	keep = lf.matcher.Match(line)
	return line, keep
}
