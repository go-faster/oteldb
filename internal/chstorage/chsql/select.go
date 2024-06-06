package chsql

import (
	"context"
	"strconv"
	"strings"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// SelectQuery is a SELECT query builder.
type SelectQuery struct {
	table    string
	sub      *SelectQuery
	distinct bool
	columns  []ResultColumn

	// where is a set of expression joined by AND
	where []Expr
	order []orderExpr

	limit int
}

type orderExpr struct {
	expr  Expr
	order Order
}

// Select creates a new [SelectQuery].
func Select(table string, columns ...ResultColumn) *SelectQuery {
	return &SelectQuery{
		table:   table,
		columns: columns,
	}
}

// SelectFrom creates a new [SelectQuery] from subquery.
func SelectFrom(sub *SelectQuery, columns ...ResultColumn) *SelectQuery {
	return &SelectQuery{
		sub:     sub,
		columns: columns,
	}
}

// Distinct sets if query is `DISTINCT`.
func (q *SelectQuery) Distinct(b bool) *SelectQuery {
	q.distinct = true
	return q
}

// Where adds filters to query.
func (q *SelectQuery) Where(filters ...Expr) *SelectQuery {
	q.where = append(q.where, filters...)
	return q
}

// Order adds order to query.
func (q *SelectQuery) Order(e Expr, order Order) *SelectQuery {
	q.order = append(q.order, orderExpr{expr: e, order: order})
	return q
}

// Limit sets query limit.
//
// If n is equal to or less than zero, limit is ignored.
func (q *SelectQuery) Limit(n int) *SelectQuery {
	q.limit = n
	return q
}

// OnResult defines [ch.Query.OnResult] callback type.
type OnResult = func(ctx context.Context, block proto.Block) error

// Prepare builds SQL query and passes columns to [ch.Query].
func (q *SelectQuery) Prepare(onResult OnResult) (ch.Query, error) {
	p := GetPrinter()
	defer PutPrinter(p)

	if err := q.WriteSQL(p); err != nil {
		return ch.Query{}, err
	}

	return ch.Query{
		Body:     p.String(),
		Result:   q.Results(),
		OnResult: onResult,
	}, nil
}

// Results returns list of result columns.
func (q *SelectQuery) Results() (r proto.Results) {
	r = make(proto.Results, len(q.columns))
	for _, c := range q.columns {
		r = append(r, proto.ResultColumn{
			Name: c.Name,
			Data: c.Data,
		})
	}
	return r
}

// WriteSQL writes SQL query.
func (q *SelectQuery) WriteSQL(p *Printer) error {
	p.Select()
	if q.distinct {
		p.Distinct()
	}

	if len(q.columns) == 0 {
		return errors.New("no columns")
	}
	for i, c := range q.columns {
		if i != 0 {
			p.Comma()
		}
		cexpr := c.Expr
		if cexpr.IsZero() {
			cexpr = Ident(c.Name)
		}
		if needColumnAlias(c.Name, cexpr) {
			// Do not alias the name if name is explicitly aliased by user.
			cexpr = binaryOp(cexpr, "AS", Ident(c.Name))
		}
		p.OpenParen()
		if err := p.WriteExpr(cexpr); err != nil {
			return errors.Wrapf(err, "column %q", c.Name)
		}
		p.CloseParen()
	}
	p.From()
	switch {
	case q.table != "":
		p.Ident(q.table)
	case q.sub != nil:
		p.OpenParen()
		if err := q.sub.WriteSQL(p); err != nil {
			return err
		}
		p.CloseParen()
	default:
		return errors.New("either table or sub-query must be present")
	}
	if len(q.where) > 0 {
		p.Where()
		for i, filter := range q.where {
			if i != 0 {
				p.And()
			}
			p.OpenParen()
			if err := p.WriteExpr(filter); err != nil {
				return errors.Wrapf(err, "filter %d", i)
			}
			p.CloseParen()
		}
	}
	if len(q.order) > 0 {
		p.Order()
		p.By()

		for i, e := range q.order {
			if i != 0 {
				p.Comma()
			}
			if err := p.WriteExpr(e.expr); err != nil {
				return errors.Wrapf(err, "order by %d", i)
			}
			switch e.order {
			case Asc:
				p.Asc()
			case Desc:
				p.Desc()
			default:
				return errors.Errorf("unexpected order %v", e.order)
			}
		}
	}

	if q.limit > 0 {
		p.Limit()
		p.Literal(strconv.Itoa(q.limit))
	}
	return nil
}

func needColumnAlias(name string, cexpr Expr) bool {
	switch cexpr.typ {
	case exprBinaryOp:
		return !strings.EqualFold(cexpr.tok, "AS")
	case exprIdent:
		return cexpr.tok != name
	default:
		return true
	}
}

// ResultColumn defines a column result.
type ResultColumn struct {
	Name string
	Expr Expr
	Data proto.ColResult
}

// Column returns new Result
func Column(name string, data proto.ColResult) ResultColumn {
	return ResultColumn{
		Name: name,
		Expr: Ident(name),
		Data: data,
	}
}
