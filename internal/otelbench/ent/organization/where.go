// Code generated by ent, DO NOT EDIT.

package organization

import (
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"github.com/go-faster/oteldb/internal/otelbench/ent/predicate"
)

// ID filters vertices based on their ID field.
func ID(id int64) predicate.Organization {
	return predicate.Organization(sql.FieldEQ(FieldID, id))
}

// IDEQ applies the EQ predicate on the ID field.
func IDEQ(id int64) predicate.Organization {
	return predicate.Organization(sql.FieldEQ(FieldID, id))
}

// IDNEQ applies the NEQ predicate on the ID field.
func IDNEQ(id int64) predicate.Organization {
	return predicate.Organization(sql.FieldNEQ(FieldID, id))
}

// IDIn applies the In predicate on the ID field.
func IDIn(ids ...int64) predicate.Organization {
	return predicate.Organization(sql.FieldIn(FieldID, ids...))
}

// IDNotIn applies the NotIn predicate on the ID field.
func IDNotIn(ids ...int64) predicate.Organization {
	return predicate.Organization(sql.FieldNotIn(FieldID, ids...))
}

// IDGT applies the GT predicate on the ID field.
func IDGT(id int64) predicate.Organization {
	return predicate.Organization(sql.FieldGT(FieldID, id))
}

// IDGTE applies the GTE predicate on the ID field.
func IDGTE(id int64) predicate.Organization {
	return predicate.Organization(sql.FieldGTE(FieldID, id))
}

// IDLT applies the LT predicate on the ID field.
func IDLT(id int64) predicate.Organization {
	return predicate.Organization(sql.FieldLT(FieldID, id))
}

// IDLTE applies the LTE predicate on the ID field.
func IDLTE(id int64) predicate.Organization {
	return predicate.Organization(sql.FieldLTE(FieldID, id))
}

// Name applies equality check predicate on the "name" field. It's identical to NameEQ.
func Name(v string) predicate.Organization {
	return predicate.Organization(sql.FieldEQ(FieldName, v))
}

// HTMLURL applies equality check predicate on the "html_url" field. It's identical to HTMLURLEQ.
func HTMLURL(v string) predicate.Organization {
	return predicate.Organization(sql.FieldEQ(FieldHTMLURL, v))
}

// NameEQ applies the EQ predicate on the "name" field.
func NameEQ(v string) predicate.Organization {
	return predicate.Organization(sql.FieldEQ(FieldName, v))
}

// NameNEQ applies the NEQ predicate on the "name" field.
func NameNEQ(v string) predicate.Organization {
	return predicate.Organization(sql.FieldNEQ(FieldName, v))
}

// NameIn applies the In predicate on the "name" field.
func NameIn(vs ...string) predicate.Organization {
	return predicate.Organization(sql.FieldIn(FieldName, vs...))
}

// NameNotIn applies the NotIn predicate on the "name" field.
func NameNotIn(vs ...string) predicate.Organization {
	return predicate.Organization(sql.FieldNotIn(FieldName, vs...))
}

// NameGT applies the GT predicate on the "name" field.
func NameGT(v string) predicate.Organization {
	return predicate.Organization(sql.FieldGT(FieldName, v))
}

// NameGTE applies the GTE predicate on the "name" field.
func NameGTE(v string) predicate.Organization {
	return predicate.Organization(sql.FieldGTE(FieldName, v))
}

// NameLT applies the LT predicate on the "name" field.
func NameLT(v string) predicate.Organization {
	return predicate.Organization(sql.FieldLT(FieldName, v))
}

// NameLTE applies the LTE predicate on the "name" field.
func NameLTE(v string) predicate.Organization {
	return predicate.Organization(sql.FieldLTE(FieldName, v))
}

// NameContains applies the Contains predicate on the "name" field.
func NameContains(v string) predicate.Organization {
	return predicate.Organization(sql.FieldContains(FieldName, v))
}

// NameHasPrefix applies the HasPrefix predicate on the "name" field.
func NameHasPrefix(v string) predicate.Organization {
	return predicate.Organization(sql.FieldHasPrefix(FieldName, v))
}

// NameHasSuffix applies the HasSuffix predicate on the "name" field.
func NameHasSuffix(v string) predicate.Organization {
	return predicate.Organization(sql.FieldHasSuffix(FieldName, v))
}

// NameEqualFold applies the EqualFold predicate on the "name" field.
func NameEqualFold(v string) predicate.Organization {
	return predicate.Organization(sql.FieldEqualFold(FieldName, v))
}

// NameContainsFold applies the ContainsFold predicate on the "name" field.
func NameContainsFold(v string) predicate.Organization {
	return predicate.Organization(sql.FieldContainsFold(FieldName, v))
}

// HTMLURLEQ applies the EQ predicate on the "html_url" field.
func HTMLURLEQ(v string) predicate.Organization {
	return predicate.Organization(sql.FieldEQ(FieldHTMLURL, v))
}

// HTMLURLNEQ applies the NEQ predicate on the "html_url" field.
func HTMLURLNEQ(v string) predicate.Organization {
	return predicate.Organization(sql.FieldNEQ(FieldHTMLURL, v))
}

// HTMLURLIn applies the In predicate on the "html_url" field.
func HTMLURLIn(vs ...string) predicate.Organization {
	return predicate.Organization(sql.FieldIn(FieldHTMLURL, vs...))
}

// HTMLURLNotIn applies the NotIn predicate on the "html_url" field.
func HTMLURLNotIn(vs ...string) predicate.Organization {
	return predicate.Organization(sql.FieldNotIn(FieldHTMLURL, vs...))
}

// HTMLURLGT applies the GT predicate on the "html_url" field.
func HTMLURLGT(v string) predicate.Organization {
	return predicate.Organization(sql.FieldGT(FieldHTMLURL, v))
}

// HTMLURLGTE applies the GTE predicate on the "html_url" field.
func HTMLURLGTE(v string) predicate.Organization {
	return predicate.Organization(sql.FieldGTE(FieldHTMLURL, v))
}

// HTMLURLLT applies the LT predicate on the "html_url" field.
func HTMLURLLT(v string) predicate.Organization {
	return predicate.Organization(sql.FieldLT(FieldHTMLURL, v))
}

// HTMLURLLTE applies the LTE predicate on the "html_url" field.
func HTMLURLLTE(v string) predicate.Organization {
	return predicate.Organization(sql.FieldLTE(FieldHTMLURL, v))
}

// HTMLURLContains applies the Contains predicate on the "html_url" field.
func HTMLURLContains(v string) predicate.Organization {
	return predicate.Organization(sql.FieldContains(FieldHTMLURL, v))
}

// HTMLURLHasPrefix applies the HasPrefix predicate on the "html_url" field.
func HTMLURLHasPrefix(v string) predicate.Organization {
	return predicate.Organization(sql.FieldHasPrefix(FieldHTMLURL, v))
}

// HTMLURLHasSuffix applies the HasSuffix predicate on the "html_url" field.
func HTMLURLHasSuffix(v string) predicate.Organization {
	return predicate.Organization(sql.FieldHasSuffix(FieldHTMLURL, v))
}

// HTMLURLIsNil applies the IsNil predicate on the "html_url" field.
func HTMLURLIsNil() predicate.Organization {
	return predicate.Organization(sql.FieldIsNull(FieldHTMLURL))
}

// HTMLURLNotNil applies the NotNil predicate on the "html_url" field.
func HTMLURLNotNil() predicate.Organization {
	return predicate.Organization(sql.FieldNotNull(FieldHTMLURL))
}

// HTMLURLEqualFold applies the EqualFold predicate on the "html_url" field.
func HTMLURLEqualFold(v string) predicate.Organization {
	return predicate.Organization(sql.FieldEqualFold(FieldHTMLURL, v))
}

// HTMLURLContainsFold applies the ContainsFold predicate on the "html_url" field.
func HTMLURLContainsFold(v string) predicate.Organization {
	return predicate.Organization(sql.FieldContainsFold(FieldHTMLURL, v))
}

// HasRepositories applies the HasEdge predicate on the "repositories" edge.
func HasRepositories() predicate.Organization {
	return predicate.Organization(func(s *sql.Selector) {
		step := sqlgraph.NewStep(
			sqlgraph.From(Table, FieldID),
			sqlgraph.Edge(sqlgraph.O2M, false, RepositoriesTable, RepositoriesColumn),
		)
		sqlgraph.HasNeighbors(s, step)
	})
}

// HasRepositoriesWith applies the HasEdge predicate on the "repositories" edge with a given conditions (other predicates).
func HasRepositoriesWith(preds ...predicate.Repository) predicate.Organization {
	return predicate.Organization(func(s *sql.Selector) {
		step := newRepositoriesStep()
		sqlgraph.HasNeighborsWith(s, step, func(s *sql.Selector) {
			for _, p := range preds {
				p(s)
			}
		})
	})
}

// And groups predicates with the AND operator between them.
func And(predicates ...predicate.Organization) predicate.Organization {
	return predicate.Organization(sql.AndPredicates(predicates...))
}

// Or groups predicates with the OR operator between them.
func Or(predicates ...predicate.Organization) predicate.Organization {
	return predicate.Organization(sql.OrPredicates(predicates...))
}

// Not applies the not operator on the given predicate.
func Not(p predicate.Organization) predicate.Organization {
	return predicate.Organization(sql.NotPredicates(p))
}