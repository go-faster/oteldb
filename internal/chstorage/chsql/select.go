package chsql

import (
	"strconv"
	"strings"

	"github.com/go-faster/errors"

	"github.com/ClickHouse/ch-go/proto"
)

// SelectQuery is a SELECT query builder.
type SelectQuery struct {
	table    string
	sub      *SelectQuery
	distinct bool
	columns  []ResultColumn

	// where is a set of expression joined by AND
	where []expr

	limit int
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
func (q *SelectQuery) Where(filters ...expr) *SelectQuery {
	q.where = append(q.where, filters...)
	return q
}

// Limit sets query limit.
//
// If n is equal to or less than zero, limit is ignored.
func (q *SelectQuery) Limit(n int) *SelectQuery {
	q.limit = n
	return q
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
		if !(cexpr.typ == exprBinaryOp && strings.EqualFold(cexpr.tok, "AS")) {
			// Do not alias the name if name is explicitly aliased by user.
			cexpr = binaryOp(c.Expr, "AS", Ident(c.Name))
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

	if q.limit > 0 {
		p.Limit()
		p.Literal(strconv.Itoa(q.limit))
	}
	return nil
}

// ResultColumn defines a column result.
type ResultColumn struct {
	Name   string
	Expr   expr
	Result proto.ColResult
}

// Column returns new Result
func Column(name string, result proto.ColResult) ResultColumn {
	return ResultColumn{
		Name:   name,
		Expr:   Ident(name),
		Result: result,
	}
}
