package ytquery

import (
	"strings"
)

// Predicate defines a logical predicate.
type Predicate interface {
	writePred(sb *strings.Builder, p *placeholderSet)
}

// Eq checks that expression is equal to given value.
func Eq(expr string, v any) Predicate {
	return eqOp{
		expr: expr,
		val:  v,
	}
}

// Ne checks that expression is not equal to given value.
func Ne(expr string, v any) Predicate {
	return eqOp{
		expr: expr,
		val:  v,
		not:  true,
	}
}

type eqOp struct {
	expr string
	val  any
	not  bool
}

func (pred eqOp) writePred(sb *strings.Builder, p *placeholderSet) {
	if v := pred.val; v == nil {
		if pred.not {
			sb.WriteString("NOT ")
		}
		sb.WriteString("is_null(")
		sb.WriteString(pred.expr)
		sb.WriteByte(')')
	} else {
		sb.WriteString(pred.expr)
		if pred.not {
			sb.WriteString(" != ")
		} else {
			sb.WriteString(" = ")
		}
		p.writePlaceholder(sb, v)
	}
}

// Ge checks that expression is equal or greater than given value.
func Ge(expr string, v any) Predicate {
	return cmpOp{
		expr: expr,
		op:   ">=",
		val:  v,
	}
}

// Le checks that expression is equal or greater than given value.
func Le(expr string, v any) Predicate {
	return cmpOp{
		expr: expr,
		op:   "<=",
		val:  v,
	}
}

// Gt checks that expression is greater than given value.
func Gt(expr string, v any) Predicate {
	return cmpOp{
		expr: expr,
		op:   ">",
		val:  v,
	}
}

// Lt checks that expression is lesser than given value.
func Lt(expr string, v any) Predicate {
	return cmpOp{
		expr: expr,
		op:   "<",
		val:  v,
	}
}

type cmpOp struct {
	expr string
	op   string
	val  any
}

func (pred cmpOp) writePred(sb *strings.Builder, p *placeholderSet) {
	sb.WriteString(pred.expr)
	sb.WriteByte(' ')
	sb.WriteString(pred.op)
	sb.WriteByte(' ')
	p.writePlaceholder(sb, pred.val)
}

// And combines predicate using AND operator.
func And(preds ...Predicate) Predicate {
	return andOp{
		operands: preds,
	}
}

type andOp struct {
	operands []Predicate
}

func (pred andOp) writePred(sb *strings.Builder, p *placeholderSet) {
	if len(pred.operands) < 1 {
		return
	}

	sb.WriteByte('(')
	for i, o := range pred.operands {
		if i != 0 {
			sb.WriteString(" AND ")
		}
		o.writePred(sb, p)
	}
	sb.WriteByte(')')
}
