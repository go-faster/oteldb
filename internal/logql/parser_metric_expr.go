package logql

import (
	"math"
	"regexp"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql/lexer"
)

func (p *parser) parseMetricExpr() (expr MetricExpr, err error) {
	switch t := p.peek(); t.Type {
	case lexer.OpenParen:
		p.next()

		subExpr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}

		expr = &ParenExpr{X: subExpr}
	case lexer.CountOverTime,
		lexer.Rate,
		lexer.RateCounter,
		lexer.BytesOverTime,
		lexer.BytesRate,
		lexer.AvgOverTime,
		lexer.SumOverTime,
		lexer.MinOverTime,
		lexer.MaxOverTime,
		lexer.StdvarOverTime,
		lexer.StddevOverTime,
		lexer.QuantileOverTime,
		lexer.FirstOverTime,
		lexer.LastOverTime,
		lexer.AbsentOverTime:
		expr, err = p.parseRangeAggregationExpr()
		if err != nil {
			return nil, err
		}
	case lexer.Sum,
		lexer.Avg,
		lexer.Count,
		lexer.Max,
		lexer.Min,
		lexer.Stddev,
		lexer.Stdvar,
		lexer.Bottomk,
		lexer.Topk,
		lexer.Sort,
		lexer.SortDesc:
		expr, err = p.parseVectorAggregationExpr()
		if err != nil {
			return nil, err
		}
	case lexer.Number, lexer.Add, lexer.Sub:
		expr, err = p.parseLiteralExpr()
		if err != nil {
			return nil, err
		}
	case lexer.LabelReplace:
		expr, err = p.parseLabelReplace()
		if err != nil {
			return nil, err
		}
	case lexer.Vector:
		expr, err = p.parseVectorExpr()
		if err != nil {
			return nil, err
		}
	default:
		return nil, p.unexpectedToken(t)
	}
	var binOp BinOp
	switch t := p.peek(); t.Type {
	case lexer.Or:
		binOp = OpOr
	case lexer.And:
		binOp = OpAnd
	case lexer.Unless:
		binOp = OpUnless
	case lexer.Add:
		binOp = OpAdd
	case lexer.Sub:
		binOp = OpSub
	case lexer.Mul:
		binOp = OpMul
	case lexer.Div:
		binOp = OpDiv
	case lexer.Mod:
		binOp = OpMod
	case lexer.Pow:
		binOp = OpPow
	case lexer.CmpEq:
		binOp = OpEq
	case lexer.NotEq:
		binOp = OpNotEq
	case lexer.Gt:
		binOp = OpGt
	case lexer.Gte:
		binOp = OpGte
	case lexer.Lt:
		binOp = OpLt
	case lexer.Lte:
		binOp = OpLte
	default:
		return expr, nil
	}
	p.next()

	modifier, err := p.parseBinOpModifier()
	if err != nil {
		return nil, err
	}

	right, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if binOp.IsLogic() {
		if v, ok := expr.(*LiteralExpr); ok {
			return nil, errors.Errorf("unexpected left scalar %v in a logical operation %s", v.Value, binOp)
		}
		if v, ok := right.(*LiteralExpr); ok {
			return nil, errors.Errorf("unexpected right scalar %v in a logical operation %s", v.Value, binOp)
		}
	}

	return &BinOpExpr{Left: expr, Op: binOp, Modifier: modifier, Right: right}, nil
}

func (p *parser) parseRangeAggregationExpr() (e *RangeAggregationExpr, _ error) {
	e = new(RangeAggregationExpr)
	switch t := p.next(); t.Type {
	case lexer.CountOverTime:
		e.Op = RangeOpCount
	case lexer.Rate:
		e.Op = RangeOpRate
	case lexer.RateCounter:
		e.Op = RangeOpRateCounter
	case lexer.BytesOverTime:
		e.Op = RangeOpBytes
	case lexer.BytesRate:
		e.Op = RangeOpBytesRate
	case lexer.AvgOverTime:
		e.Op = RangeOpAvg
	case lexer.SumOverTime:
		e.Op = RangeOpSum
	case lexer.MinOverTime:
		e.Op = RangeOpMin
	case lexer.MaxOverTime:
		e.Op = RangeOpMax
	case lexer.StdvarOverTime:
		e.Op = RangeOpStdvar
	case lexer.StddevOverTime:
		e.Op = RangeOpStddev
	case lexer.QuantileOverTime:
		e.Op = RangeOpQuantile
	case lexer.FirstOverTime:
		e.Op = RangeOpFirst
	case lexer.LastOverTime:
		e.Op = RangeOpLast
	case lexer.AbsentOverTime:
		e.Op = RangeOpAbsent
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	if t := p.peek(); t.Type == lexer.Number {
		param, err := p.parseNumber()
		if err != nil {
			return nil, err
		}
		e.Parameter = &param

		if err := p.consume(lexer.Comma); err != nil {
			return nil, err
		}
	}

	expr, err := p.parseRangeExpr()
	if err != nil {
		return nil, err
	}
	e.Range = expr

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}

	switch t := p.peek(); t.Type {
	case lexer.By, lexer.Without:
		e.Grouping, err = p.parseGrouping()
		if err != nil {
			return nil, err
		}
	}

	err = e.validate()
	return e, err
}

func (p *parser) parseVectorAggregationExpr() (e *VectorAggregationExpr, err error) {
	e = new(VectorAggregationExpr)
	switch t := p.next(); t.Type {
	case lexer.Sum:
		e.Op = VectorOpSum
	case lexer.Avg:
		e.Op = VectorOpAvg
	case lexer.Count:
		e.Op = VectorOpCount
	case lexer.Max:
		e.Op = VectorOpMax
	case lexer.Min:
		e.Op = VectorOpMin
	case lexer.Stddev:
		e.Op = VectorOpStddev
	case lexer.Stdvar:
		e.Op = VectorOpStdvar
	case lexer.Bottomk:
		e.Op = VectorOpBottomk
	case lexer.Topk:
		e.Op = VectorOpTopk
	case lexer.Sort:
		e.Op = VectorOpSort
	case lexer.SortDesc:
		e.Op = VectorOpSortDesc
	}

	parseMetricExpr := func() error {
		if err := p.consume(lexer.OpenParen); err != nil {
			return err
		}

		if t := p.peek(); t.Type == lexer.Number {
			param, err := p.parseInt()
			if err != nil {
				return err
			}
			e.Parameter = &param

			if err := p.consume(lexer.Comma); err != nil {
				return err
			}
		}

		expr, err := p.parseMetricExpr()
		if err != nil {
			return err
		}
		e.Expr = expr

		return p.consume(lexer.CloseParen)
	}

	switch t := p.peek(); t.Type {
	case lexer.By, lexer.Without:
		e.Grouping, err = p.parseGrouping()
		if err != nil {
			return nil, err
		}

		if err := parseMetricExpr(); err != nil {
			return nil, err
		}
	case lexer.OpenParen:
		if err := parseMetricExpr(); err != nil {
			return nil, err
		}

		switch gt := p.peek(); gt.Type {
		case lexer.By, lexer.Without:
			e.Grouping, err = p.parseGrouping()
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, p.unexpectedToken(t)
	}

	err = e.validate()
	return e, err
}

func (p *parser) parseLiteralExpr() (*LiteralExpr, error) {
	sign := float64(1)
	switch t := p.next(); t.Type {
	case lexer.Add:
		sign = 1
	case lexer.Sub:
		sign = -1
	case lexer.Number:
		p.unread()
	default:
		return nil, p.unexpectedToken(t)
	}

	f, err := p.parseNumber()
	if err != nil {
		return nil, err
	}
	return &LiteralExpr{Value: math.Copysign(f, sign)}, nil
}

// Parses PromQL function.
//
//	label_replace(v instant-vector, dst_label string, replacement string, src_label string, regex string)
func (p *parser) parseLabelReplace() (lr *LabelReplaceExpr, err error) {
	lr = new(LabelReplaceExpr)
	if err := p.consume(lexer.LabelReplace); err != nil {
		return nil, err
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	lr.Expr, err = p.parseMetricExpr()
	if err != nil {
		return nil, err
	}

	readParam := func(to *string) error {
		if err := p.consume(lexer.Comma); err != nil {
			return err
		}

		val, err := p.parseString()
		if err != nil {
			return err
		}
		*to = val
		return nil
	}

	if err := readParam(&lr.DstLabel); err != nil {
		return nil, err
	}
	if err := readParam(&lr.Replacement); err != nil {
		return nil, err
	}
	if err := readParam(&lr.SrcLabel); err != nil {
		return nil, err
	}
	// TODO(tdakkota): compile regex?
	if err := readParam(&lr.Regex); err != nil {
		return nil, err
	}
	lr.Re, err = regexp.Compile("^(?:" + lr.Regex + ")$")
	if err != nil {
		return nil, errors.Wrapf(err, "invalid regex in label_replace %q", lr.Regex)
	}

	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}
	return lr, nil
}

func (p *parser) parseVectorExpr() (ve *VectorExpr, err error) {
	ve = new(VectorExpr)
	if err := p.consume(lexer.Vector); err != nil {
		return nil, err
	}

	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}
	ve.Value, err = p.parseNumber()
	if err != nil {
		return nil, err
	}
	if err := p.consume(lexer.CloseParen); err != nil {
		return nil, err
	}
	return ve, nil
}

func (p *parser) parseGrouping() (g *Grouping, err error) {
	g = new(Grouping)
	switch t := p.next(); t.Type {
	case lexer.By:
	case lexer.Without:
		g.Without = true
	default:
		return nil, p.unexpectedToken(t)
	}

	g.Labels, err = p.parseLabels()
	return g, err
}

// parseLabels parses a possibly empty parenthesized list of comma-separated labels.
func (p *parser) parseLabels() (labels []Label, _ error) {
	if err := p.consume(lexer.OpenParen); err != nil {
		return nil, err
	}

	// Empty grouping.
	if t := p.peek(); t.Type == lexer.CloseParen {
		p.next()
		return labels, nil
	}

	for {
		label, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		labels = append(labels, label)

		switch t := p.next(); t.Type {
		case lexer.CloseParen:
			return labels, nil
		case lexer.Comma:
		default:
			return nil, p.unexpectedToken(t)
		}
	}
}
