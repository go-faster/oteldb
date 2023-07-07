package logql

import (
	"regexp"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/logql/lexer"
)

func (p *parser) parsePipeline(allowUnwrap bool) (stages []PipelineStage, err error) {
	for {
		switch t := p.peek(); t.Type {
		case lexer.PipeExact, lexer.PipeMatch, lexer.NotEq, lexer.NotRe: // ( "|=" | "|~" | "!=" | "!~" )
			lf, err := p.parseLineFilter()
			if err != nil {
				return stages, err
			}
			stages = append(stages, lf)
		case lexer.Pipe: // "|"
			p.next()

			switch t := p.next(); t.Type {
			case lexer.JSON:
				labels, exprs, err := p.parseLabelExtraction()
				if err != nil {
					return stages, err
				}
				stages = append(stages, &JSONExpressionParser{Labels: labels, Exprs: exprs})
			case lexer.Logfmt:
				labels, exprs, err := p.parseLabelExtraction()
				if err != nil {
					return stages, err
				}
				stages = append(stages, &LogfmtExpressionParser{Labels: labels, Exprs: exprs})
			case lexer.Regexp:
				p, err := p.parseRegexpLabelParser()
				if err != nil {
					return stages, err
				}
				stages = append(stages, p)
			case lexer.Pattern:
				pattern, err := p.parseString()
				if err != nil {
					return stages, err
				}
				// FIXME(tdakkota): parse pattern?
				stages = append(stages, &PatternLabelParser{Pattern: pattern})
			case lexer.Unpack:
				stages = append(stages, &UnpackLabelParser{})
			case lexer.LineFormat:
				tmpl, err := p.parseString()
				if err != nil {
					return stages, err
				}
				// FIXME(tdakkota): parse template?
				stages = append(stages, &LineFormat{Template: tmpl})
			case lexer.Decolorize:
				stages = append(stages, &DecolorizeExpr{})
			case lexer.Ident, lexer.OpenParen:
				p.unread()

				pred, err := p.parseLabelPredicate()
				if err != nil {
					return stages, err
				}
				stages = append(stages, &LabelFilter{Pred: pred})
			case lexer.LabelFormat:
				lf, err := p.parseLabelFormatExpr()
				if err != nil {
					return stages, err
				}
				stages = append(stages, lf)
			case lexer.Keep:
				keep, err := p.parseKeepLabelsExpr()
				if err != nil {
					return stages, err
				}
				stages = append(stages, keep)
			case lexer.Drop:
				drop, err := p.parseDropLabelsExpr()
				if err != nil {
					return stages, err
				}
				stages = append(stages, drop)
			case lexer.Distinct:
				distinct, err := p.parseDistinctFilter()
				if err != nil {
					return stages, err
				}
				stages = append(stages, distinct)
			case lexer.Unwrap:
				// Only for metricExpr.
				// Allow caller parse it afterwards.
				if allowUnwrap {
					p.unread()
					return stages, nil
				}
				// Fail otherwise.
				fallthrough
			default:
				// TODO(tdakkota): parse other filters
				return stages, p.unexpectedToken(t)
			}
		default:
			return stages, nil
		}
	}
}

func (p *parser) parseLineFilter() (f *LineFilter, err error) {
	t := p.next()

	f = new(LineFilter)
	switch t.Type {
	case lexer.PipeExact: // "|="
		f.Op = OpEq
	case lexer.PipeMatch: // "|~"
		f.Op = OpRe
	case lexer.NotEq: // "!="
		f.Op = OpNotEq
	case lexer.NotRe: // "!~"
		f.Op = OpNotRe
	default:
		return nil, p.unexpectedToken(t)
	}

	switch t := p.peek(); t.Type {
	case lexer.String:
		f.Value, err = p.parseString()
		if err != nil {
			return nil, err
		}

		switch f.Op {
		case OpRe, OpNotRe:
			f.Re, err = regexp.Compile(f.Value)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid regex in line filter %q", f.Value)
			}
		}
	case lexer.IP:
		p.next()

		switch f.Op {
		case OpEq, OpNotEq:
		default:
			return nil, errors.Errorf("invalid IP line filter operation %q", f.Op)
		}

		if err := p.consume(lexer.OpenParen); err != nil {
			return nil, err
		}

		f.Value, err = p.parseString()
		if err != nil {
			return nil, err
		}
		f.IP = true

		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}
	default:
		return nil, p.unexpectedToken(t)
	}
	return f, nil
}

func (p *parser) parseLabelExtraction() (labels []Label, exprs []LabelExtractionExpr, err error) {
	for {
		if t := p.peek(); t.Type != lexer.Ident {
			return labels, exprs, nil
		}

		label, err := p.parseIdent()
		if err != nil {
			return labels, exprs, err
		}

		switch t := p.peek(); t.Type {
		case lexer.Comma:
			labels = append(labels, label)
		case lexer.Eq:
			p.next()

			expr, err := p.parseString()
			if err != nil {
				return labels, exprs, err
			}
			exprs = append(exprs, LabelExtractionExpr{
				Label: label,
				Expr:  expr,
			})

			if t := p.peek(); t.Type != lexer.Comma {
				continue
			}
		default:
			labels = append(labels, label)
			continue
		}

		// Consume comma.
		p.next()
		// Expect a label after that.
		if t := p.peek(); t.Type != lexer.Ident {
			return labels, exprs, p.unexpectedToken(t)
		}
	}
}

func (p *parser) parseRegexpLabelParser() (*RegexpLabelParser, error) {
	pattern, err := p.parseString()
	if err != nil {
		return nil, err
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid regex in regexp stage %q", pattern)
	}

	mapping := map[int]Label{}
	unique := map[string]struct{}{}
	for i, name := range re.SubexpNames() {
		// Not capturing.
		if name == "" {
			continue
		}

		if _, ok := unique[name]; ok {
			return nil, errors.Wrapf(err, "duplicate capture %q", name)
		}
		unique[name] = struct{}{}

		if err := IsValidLabel(name, p.allowDots); err != nil {
			return nil, errors.Wrapf(err, "invalid label name %q", name)
		}
		mapping[i] = Label(name)
	}

	return &RegexpLabelParser{
		Regexp:  re,
		Mapping: mapping,
	}, nil
}

func (p *parser) parseLabelPredicate() (pred LabelPredicate, _ error) {
	switch t := p.next(); t.Type {
	case lexer.OpenParen:
		lp, err := p.parseLabelPredicate()
		if err != nil {
			return nil, err
		}
		if err := p.consume(lexer.CloseParen); err != nil {
			return nil, err
		}

		pred = &LabelPredicateParen{X: lp}
	case lexer.Ident:
		var op BinOp

		opTok := p.next()
		switch opTok.Type {
		case lexer.Eq:
			op = OpEq
		case lexer.CmpEq:
			op = OpEq
		case lexer.NotEq:
			op = OpNotEq
		case lexer.Re:
			op = OpRe
		case lexer.NotRe:
			op = OpNotRe
		case lexer.Gt:
			op = OpGt
		case lexer.Gte:
			op = OpGte
		case lexer.Lt:
			op = OpLt
		case lexer.Lte:
			op = OpLte
		default:
			return nil, p.unexpectedToken(opTok)
		}

		switch literalTok := p.peek(); literalTok.Type {
		case lexer.String:
			switch opTok.Type {
			case lexer.Eq, lexer.NotEq, lexer.Re, lexer.NotRe:
			default:
				return nil, errors.Errorf("invalid operation %q", opTok.Type)
			}

			v, err := p.parseString()
			if err != nil {
				return nil, err
			}

			var re *regexp.Regexp
			switch op {
			case OpRe, OpNotRe:
				re, err = regexp.Compile("^(?:" + v + ")$")
				if err != nil {
					return nil, errors.Wrapf(err, "invalid regex in label matcher predicate %q", v)
				}
			}
			pred = &LabelMatcher{Label: Label(t.Text), Op: op, Value: v, Re: re}
		case lexer.Number:
			switch opTok.Type {
			case lexer.CmpEq, lexer.NotEq, lexer.Lt, lexer.Lte, lexer.Gt, lexer.Gte:
			default:
				return nil, errors.Errorf("invalid operation %q", opTok.Type)
			}

			v, err := p.parseNumber()
			if err != nil {
				return nil, err
			}
			pred = &NumberFilter{Label: Label(t.Text), Op: op, Value: v}
		case lexer.Duration:
			switch opTok.Type {
			case lexer.CmpEq, lexer.NotEq, lexer.Lt, lexer.Lte, lexer.Gt, lexer.Gte:
			default:
				return nil, errors.Errorf("invalid operation %q", opTok.Type)
			}

			d, err := p.parseDuration()
			if err != nil {
				return nil, err
			}
			pred = &DurationFilter{Label: Label(t.Text), Op: op, Value: d}
		case lexer.Bytes:
			switch opTok.Type {
			case lexer.CmpEq, lexer.NotEq, lexer.Lt, lexer.Lte, lexer.Gt, lexer.Gte:
			default:
				return nil, errors.Errorf("invalid operation %q", opTok.Type)
			}

			b, err := p.parseBytes()
			if err != nil {
				return nil, err
			}
			pred = &BytesFilter{Label: Label(t.Text), Op: op, Value: b}
		case lexer.IP:
			switch opTok.Type {
			case lexer.CmpEq, lexer.NotEq:
			default:
				return nil, errors.Errorf("invalid operation %q", opTok.Type)
			}
			// Read "ip" token.
			p.next()

			if err := p.consume(lexer.OpenParen); err != nil {
				return nil, err
			}

			ipPattern, err := p.parseString()
			if err != nil {
				return nil, err
			}

			if err := p.consume(lexer.CloseParen); err != nil {
				return nil, err
			}

			pred = &IPFilter{Label: Label(t.Text), Op: op, Value: ipPattern}
		default:
			return nil, p.unexpectedToken(literalTok)
		}

	default:
		return nil, p.unexpectedToken(t)
	}

	var binOp BinOp
	switch nextTok := p.next(); nextTok.Type {
	case lexer.Ident:
		p.unread()
		binOp = OpAnd
	case lexer.Comma, lexer.And:
		binOp = OpAnd
	case lexer.Or:
		binOp = OpOr
	default:
		p.unread()
		return pred, nil
	}

	right, err := p.parseLabelPredicate()
	if err != nil {
		return nil, err
	}
	return &LabelPredicateBinOp{Left: pred, Op: binOp, Right: right}, nil
}

func (p *parser) parseLabelFormatExpr() (lf *LabelFormatExpr, err error) {
	lf = new(LabelFormatExpr)

	for {
		label, err := p.parseIdent()
		if err != nil {
			return nil, err
		}

		if err := p.consume(lexer.Eq); err != nil {
			return nil, err
		}

		switch t := p.peek(); t.Type {
		case lexer.Ident:
			value, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			lf.Labels = append(lf.Labels, RenameLabel{Label: label, To: value})
		case lexer.String:
			value, err := p.parseString()
			if err != nil {
				return nil, err
			}
			lf.Values = append(lf.Values, LabelTemplate{Label: label, Template: value})
		default:
			return nil, p.unexpectedToken(t)
		}

		if t := p.peek(); t.Type != lexer.Comma {
			return lf, nil
		}
		p.next()
	}
}

func (p *parser) parseKeepLabelsExpr() (*KeepLabelsExpr, error) {
	labels, matchers, err := p.parseLabelsAndMatchers()
	if err != nil {
		return nil, err
	}
	return &KeepLabelsExpr{Labels: labels, Matchers: matchers}, nil
}

func (p *parser) parseDropLabelsExpr() (*DropLabelsExpr, error) {
	labels, matchers, err := p.parseLabelsAndMatchers()
	if err != nil {
		return nil, err
	}
	return &DropLabelsExpr{Labels: labels, Matchers: matchers}, nil
}

func (p *parser) parseLabelsAndMatchers() (labels []Label, matchers []LabelMatcher, _ error) {
	for {
		if err := p.consume(lexer.Ident); err != nil {
			return labels, matchers, err
		}

		switch t := p.peek(); t.Type {
		case lexer.Eq, lexer.NotEq, lexer.Re, lexer.NotRe:
			p.unread()

			m, err := p.parseLabelMatcher()
			if err != nil {
				return labels, matchers, err
			}
			matchers = append(matchers, m)
		default:
			p.unread()

			label, err := p.parseIdent()
			if err != nil {
				return labels, matchers, err
			}
			labels = append(labels, label)
		}

		if t := p.peek(); t.Type != lexer.Comma {
			return labels, matchers, nil
		}
		// Consume comma.
		p.next()
	}
}

func (p *parser) parseDistinctFilter() (df *DistinctFilter, _ error) {
	df = new(DistinctFilter)
	for {
		label, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		df.Labels = append(df.Labels, label)

		if t := p.peek(); t.Type != lexer.Comma {
			return df, nil
		}
		// Consume comma.
		p.next()
	}
}
