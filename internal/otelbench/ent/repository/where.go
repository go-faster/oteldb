// Code generated by ent, DO NOT EDIT.

package repository

import (
	"time"

	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"github.com/go-faster/oteldb/internal/otelbench/ent/predicate"
)

// ID filters vertices based on their ID field.
func ID(id int64) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldID, id))
}

// IDEQ applies the EQ predicate on the ID field.
func IDEQ(id int64) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldID, id))
}

// IDNEQ applies the NEQ predicate on the ID field.
func IDNEQ(id int64) predicate.Repository {
	return predicate.Repository(sql.FieldNEQ(FieldID, id))
}

// IDIn applies the In predicate on the ID field.
func IDIn(ids ...int64) predicate.Repository {
	return predicate.Repository(sql.FieldIn(FieldID, ids...))
}

// IDNotIn applies the NotIn predicate on the ID field.
func IDNotIn(ids ...int64) predicate.Repository {
	return predicate.Repository(sql.FieldNotIn(FieldID, ids...))
}

// IDGT applies the GT predicate on the ID field.
func IDGT(id int64) predicate.Repository {
	return predicate.Repository(sql.FieldGT(FieldID, id))
}

// IDGTE applies the GTE predicate on the ID field.
func IDGTE(id int64) predicate.Repository {
	return predicate.Repository(sql.FieldGTE(FieldID, id))
}

// IDLT applies the LT predicate on the ID field.
func IDLT(id int64) predicate.Repository {
	return predicate.Repository(sql.FieldLT(FieldID, id))
}

// IDLTE applies the LTE predicate on the ID field.
func IDLTE(id int64) predicate.Repository {
	return predicate.Repository(sql.FieldLTE(FieldID, id))
}

// Name applies equality check predicate on the "name" field. It's identical to NameEQ.
func Name(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldName, v))
}

// FullName applies equality check predicate on the "full_name" field. It's identical to FullNameEQ.
func FullName(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldFullName, v))
}

// HTMLURL applies equality check predicate on the "html_url" field. It's identical to HTMLURLEQ.
func HTMLURL(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldHTMLURL, v))
}

// Description applies equality check predicate on the "description" field. It's identical to DescriptionEQ.
func Description(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldDescription, v))
}

// LastPushedAt applies equality check predicate on the "last_pushed_at" field. It's identical to LastPushedAtEQ.
func LastPushedAt(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldLastPushedAt, v))
}

// LastEventAt applies equality check predicate on the "last_event_at" field. It's identical to LastEventAtEQ.
func LastEventAt(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldLastEventAt, v))
}

// NameEQ applies the EQ predicate on the "name" field.
func NameEQ(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldName, v))
}

// NameNEQ applies the NEQ predicate on the "name" field.
func NameNEQ(v string) predicate.Repository {
	return predicate.Repository(sql.FieldNEQ(FieldName, v))
}

// NameIn applies the In predicate on the "name" field.
func NameIn(vs ...string) predicate.Repository {
	return predicate.Repository(sql.FieldIn(FieldName, vs...))
}

// NameNotIn applies the NotIn predicate on the "name" field.
func NameNotIn(vs ...string) predicate.Repository {
	return predicate.Repository(sql.FieldNotIn(FieldName, vs...))
}

// NameGT applies the GT predicate on the "name" field.
func NameGT(v string) predicate.Repository {
	return predicate.Repository(sql.FieldGT(FieldName, v))
}

// NameGTE applies the GTE predicate on the "name" field.
func NameGTE(v string) predicate.Repository {
	return predicate.Repository(sql.FieldGTE(FieldName, v))
}

// NameLT applies the LT predicate on the "name" field.
func NameLT(v string) predicate.Repository {
	return predicate.Repository(sql.FieldLT(FieldName, v))
}

// NameLTE applies the LTE predicate on the "name" field.
func NameLTE(v string) predicate.Repository {
	return predicate.Repository(sql.FieldLTE(FieldName, v))
}

// NameContains applies the Contains predicate on the "name" field.
func NameContains(v string) predicate.Repository {
	return predicate.Repository(sql.FieldContains(FieldName, v))
}

// NameHasPrefix applies the HasPrefix predicate on the "name" field.
func NameHasPrefix(v string) predicate.Repository {
	return predicate.Repository(sql.FieldHasPrefix(FieldName, v))
}

// NameHasSuffix applies the HasSuffix predicate on the "name" field.
func NameHasSuffix(v string) predicate.Repository {
	return predicate.Repository(sql.FieldHasSuffix(FieldName, v))
}

// NameEqualFold applies the EqualFold predicate on the "name" field.
func NameEqualFold(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEqualFold(FieldName, v))
}

// NameContainsFold applies the ContainsFold predicate on the "name" field.
func NameContainsFold(v string) predicate.Repository {
	return predicate.Repository(sql.FieldContainsFold(FieldName, v))
}

// FullNameEQ applies the EQ predicate on the "full_name" field.
func FullNameEQ(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldFullName, v))
}

// FullNameNEQ applies the NEQ predicate on the "full_name" field.
func FullNameNEQ(v string) predicate.Repository {
	return predicate.Repository(sql.FieldNEQ(FieldFullName, v))
}

// FullNameIn applies the In predicate on the "full_name" field.
func FullNameIn(vs ...string) predicate.Repository {
	return predicate.Repository(sql.FieldIn(FieldFullName, vs...))
}

// FullNameNotIn applies the NotIn predicate on the "full_name" field.
func FullNameNotIn(vs ...string) predicate.Repository {
	return predicate.Repository(sql.FieldNotIn(FieldFullName, vs...))
}

// FullNameGT applies the GT predicate on the "full_name" field.
func FullNameGT(v string) predicate.Repository {
	return predicate.Repository(sql.FieldGT(FieldFullName, v))
}

// FullNameGTE applies the GTE predicate on the "full_name" field.
func FullNameGTE(v string) predicate.Repository {
	return predicate.Repository(sql.FieldGTE(FieldFullName, v))
}

// FullNameLT applies the LT predicate on the "full_name" field.
func FullNameLT(v string) predicate.Repository {
	return predicate.Repository(sql.FieldLT(FieldFullName, v))
}

// FullNameLTE applies the LTE predicate on the "full_name" field.
func FullNameLTE(v string) predicate.Repository {
	return predicate.Repository(sql.FieldLTE(FieldFullName, v))
}

// FullNameContains applies the Contains predicate on the "full_name" field.
func FullNameContains(v string) predicate.Repository {
	return predicate.Repository(sql.FieldContains(FieldFullName, v))
}

// FullNameHasPrefix applies the HasPrefix predicate on the "full_name" field.
func FullNameHasPrefix(v string) predicate.Repository {
	return predicate.Repository(sql.FieldHasPrefix(FieldFullName, v))
}

// FullNameHasSuffix applies the HasSuffix predicate on the "full_name" field.
func FullNameHasSuffix(v string) predicate.Repository {
	return predicate.Repository(sql.FieldHasSuffix(FieldFullName, v))
}

// FullNameEqualFold applies the EqualFold predicate on the "full_name" field.
func FullNameEqualFold(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEqualFold(FieldFullName, v))
}

// FullNameContainsFold applies the ContainsFold predicate on the "full_name" field.
func FullNameContainsFold(v string) predicate.Repository {
	return predicate.Repository(sql.FieldContainsFold(FieldFullName, v))
}

// HTMLURLEQ applies the EQ predicate on the "html_url" field.
func HTMLURLEQ(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldHTMLURL, v))
}

// HTMLURLNEQ applies the NEQ predicate on the "html_url" field.
func HTMLURLNEQ(v string) predicate.Repository {
	return predicate.Repository(sql.FieldNEQ(FieldHTMLURL, v))
}

// HTMLURLIn applies the In predicate on the "html_url" field.
func HTMLURLIn(vs ...string) predicate.Repository {
	return predicate.Repository(sql.FieldIn(FieldHTMLURL, vs...))
}

// HTMLURLNotIn applies the NotIn predicate on the "html_url" field.
func HTMLURLNotIn(vs ...string) predicate.Repository {
	return predicate.Repository(sql.FieldNotIn(FieldHTMLURL, vs...))
}

// HTMLURLGT applies the GT predicate on the "html_url" field.
func HTMLURLGT(v string) predicate.Repository {
	return predicate.Repository(sql.FieldGT(FieldHTMLURL, v))
}

// HTMLURLGTE applies the GTE predicate on the "html_url" field.
func HTMLURLGTE(v string) predicate.Repository {
	return predicate.Repository(sql.FieldGTE(FieldHTMLURL, v))
}

// HTMLURLLT applies the LT predicate on the "html_url" field.
func HTMLURLLT(v string) predicate.Repository {
	return predicate.Repository(sql.FieldLT(FieldHTMLURL, v))
}

// HTMLURLLTE applies the LTE predicate on the "html_url" field.
func HTMLURLLTE(v string) predicate.Repository {
	return predicate.Repository(sql.FieldLTE(FieldHTMLURL, v))
}

// HTMLURLContains applies the Contains predicate on the "html_url" field.
func HTMLURLContains(v string) predicate.Repository {
	return predicate.Repository(sql.FieldContains(FieldHTMLURL, v))
}

// HTMLURLHasPrefix applies the HasPrefix predicate on the "html_url" field.
func HTMLURLHasPrefix(v string) predicate.Repository {
	return predicate.Repository(sql.FieldHasPrefix(FieldHTMLURL, v))
}

// HTMLURLHasSuffix applies the HasSuffix predicate on the "html_url" field.
func HTMLURLHasSuffix(v string) predicate.Repository {
	return predicate.Repository(sql.FieldHasSuffix(FieldHTMLURL, v))
}

// HTMLURLIsNil applies the IsNil predicate on the "html_url" field.
func HTMLURLIsNil() predicate.Repository {
	return predicate.Repository(sql.FieldIsNull(FieldHTMLURL))
}

// HTMLURLNotNil applies the NotNil predicate on the "html_url" field.
func HTMLURLNotNil() predicate.Repository {
	return predicate.Repository(sql.FieldNotNull(FieldHTMLURL))
}

// HTMLURLEqualFold applies the EqualFold predicate on the "html_url" field.
func HTMLURLEqualFold(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEqualFold(FieldHTMLURL, v))
}

// HTMLURLContainsFold applies the ContainsFold predicate on the "html_url" field.
func HTMLURLContainsFold(v string) predicate.Repository {
	return predicate.Repository(sql.FieldContainsFold(FieldHTMLURL, v))
}

// DescriptionEQ applies the EQ predicate on the "description" field.
func DescriptionEQ(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldDescription, v))
}

// DescriptionNEQ applies the NEQ predicate on the "description" field.
func DescriptionNEQ(v string) predicate.Repository {
	return predicate.Repository(sql.FieldNEQ(FieldDescription, v))
}

// DescriptionIn applies the In predicate on the "description" field.
func DescriptionIn(vs ...string) predicate.Repository {
	return predicate.Repository(sql.FieldIn(FieldDescription, vs...))
}

// DescriptionNotIn applies the NotIn predicate on the "description" field.
func DescriptionNotIn(vs ...string) predicate.Repository {
	return predicate.Repository(sql.FieldNotIn(FieldDescription, vs...))
}

// DescriptionGT applies the GT predicate on the "description" field.
func DescriptionGT(v string) predicate.Repository {
	return predicate.Repository(sql.FieldGT(FieldDescription, v))
}

// DescriptionGTE applies the GTE predicate on the "description" field.
func DescriptionGTE(v string) predicate.Repository {
	return predicate.Repository(sql.FieldGTE(FieldDescription, v))
}

// DescriptionLT applies the LT predicate on the "description" field.
func DescriptionLT(v string) predicate.Repository {
	return predicate.Repository(sql.FieldLT(FieldDescription, v))
}

// DescriptionLTE applies the LTE predicate on the "description" field.
func DescriptionLTE(v string) predicate.Repository {
	return predicate.Repository(sql.FieldLTE(FieldDescription, v))
}

// DescriptionContains applies the Contains predicate on the "description" field.
func DescriptionContains(v string) predicate.Repository {
	return predicate.Repository(sql.FieldContains(FieldDescription, v))
}

// DescriptionHasPrefix applies the HasPrefix predicate on the "description" field.
func DescriptionHasPrefix(v string) predicate.Repository {
	return predicate.Repository(sql.FieldHasPrefix(FieldDescription, v))
}

// DescriptionHasSuffix applies the HasSuffix predicate on the "description" field.
func DescriptionHasSuffix(v string) predicate.Repository {
	return predicate.Repository(sql.FieldHasSuffix(FieldDescription, v))
}

// DescriptionEqualFold applies the EqualFold predicate on the "description" field.
func DescriptionEqualFold(v string) predicate.Repository {
	return predicate.Repository(sql.FieldEqualFold(FieldDescription, v))
}

// DescriptionContainsFold applies the ContainsFold predicate on the "description" field.
func DescriptionContainsFold(v string) predicate.Repository {
	return predicate.Repository(sql.FieldContainsFold(FieldDescription, v))
}

// LastPushedAtEQ applies the EQ predicate on the "last_pushed_at" field.
func LastPushedAtEQ(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldLastPushedAt, v))
}

// LastPushedAtNEQ applies the NEQ predicate on the "last_pushed_at" field.
func LastPushedAtNEQ(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldNEQ(FieldLastPushedAt, v))
}

// LastPushedAtIn applies the In predicate on the "last_pushed_at" field.
func LastPushedAtIn(vs ...time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldIn(FieldLastPushedAt, vs...))
}

// LastPushedAtNotIn applies the NotIn predicate on the "last_pushed_at" field.
func LastPushedAtNotIn(vs ...time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldNotIn(FieldLastPushedAt, vs...))
}

// LastPushedAtGT applies the GT predicate on the "last_pushed_at" field.
func LastPushedAtGT(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldGT(FieldLastPushedAt, v))
}

// LastPushedAtGTE applies the GTE predicate on the "last_pushed_at" field.
func LastPushedAtGTE(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldGTE(FieldLastPushedAt, v))
}

// LastPushedAtLT applies the LT predicate on the "last_pushed_at" field.
func LastPushedAtLT(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldLT(FieldLastPushedAt, v))
}

// LastPushedAtLTE applies the LTE predicate on the "last_pushed_at" field.
func LastPushedAtLTE(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldLTE(FieldLastPushedAt, v))
}

// LastPushedAtIsNil applies the IsNil predicate on the "last_pushed_at" field.
func LastPushedAtIsNil() predicate.Repository {
	return predicate.Repository(sql.FieldIsNull(FieldLastPushedAt))
}

// LastPushedAtNotNil applies the NotNil predicate on the "last_pushed_at" field.
func LastPushedAtNotNil() predicate.Repository {
	return predicate.Repository(sql.FieldNotNull(FieldLastPushedAt))
}

// LastEventAtEQ applies the EQ predicate on the "last_event_at" field.
func LastEventAtEQ(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldEQ(FieldLastEventAt, v))
}

// LastEventAtNEQ applies the NEQ predicate on the "last_event_at" field.
func LastEventAtNEQ(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldNEQ(FieldLastEventAt, v))
}

// LastEventAtIn applies the In predicate on the "last_event_at" field.
func LastEventAtIn(vs ...time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldIn(FieldLastEventAt, vs...))
}

// LastEventAtNotIn applies the NotIn predicate on the "last_event_at" field.
func LastEventAtNotIn(vs ...time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldNotIn(FieldLastEventAt, vs...))
}

// LastEventAtGT applies the GT predicate on the "last_event_at" field.
func LastEventAtGT(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldGT(FieldLastEventAt, v))
}

// LastEventAtGTE applies the GTE predicate on the "last_event_at" field.
func LastEventAtGTE(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldGTE(FieldLastEventAt, v))
}

// LastEventAtLT applies the LT predicate on the "last_event_at" field.
func LastEventAtLT(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldLT(FieldLastEventAt, v))
}

// LastEventAtLTE applies the LTE predicate on the "last_event_at" field.
func LastEventAtLTE(v time.Time) predicate.Repository {
	return predicate.Repository(sql.FieldLTE(FieldLastEventAt, v))
}

// LastEventAtIsNil applies the IsNil predicate on the "last_event_at" field.
func LastEventAtIsNil() predicate.Repository {
	return predicate.Repository(sql.FieldIsNull(FieldLastEventAt))
}

// LastEventAtNotNil applies the NotNil predicate on the "last_event_at" field.
func LastEventAtNotNil() predicate.Repository {
	return predicate.Repository(sql.FieldNotNull(FieldLastEventAt))
}

// HasOrganization applies the HasEdge predicate on the "organization" edge.
func HasOrganization() predicate.Repository {
	return predicate.Repository(func(s *sql.Selector) {
		step := sqlgraph.NewStep(
			sqlgraph.From(Table, FieldID),
			sqlgraph.Edge(sqlgraph.M2O, true, OrganizationTable, OrganizationColumn),
		)
		sqlgraph.HasNeighbors(s, step)
	})
}

// HasOrganizationWith applies the HasEdge predicate on the "organization" edge with a given conditions (other predicates).
func HasOrganizationWith(preds ...predicate.Organization) predicate.Repository {
	return predicate.Repository(func(s *sql.Selector) {
		step := newOrganizationStep()
		sqlgraph.HasNeighborsWith(s, step, func(s *sql.Selector) {
			for _, p := range preds {
				p(s)
			}
		})
	})
}

// HasCommits applies the HasEdge predicate on the "commits" edge.
func HasCommits() predicate.Repository {
	return predicate.Repository(func(s *sql.Selector) {
		step := sqlgraph.NewStep(
			sqlgraph.From(Table, FieldID),
			sqlgraph.Edge(sqlgraph.O2M, false, CommitsTable, CommitsColumn),
		)
		sqlgraph.HasNeighbors(s, step)
	})
}

// HasCommitsWith applies the HasEdge predicate on the "commits" edge with a given conditions (other predicates).
func HasCommitsWith(preds ...predicate.GitCommit) predicate.Repository {
	return predicate.Repository(func(s *sql.Selector) {
		step := newCommitsStep()
		sqlgraph.HasNeighborsWith(s, step, func(s *sql.Selector) {
			for _, p := range preds {
				p(s)
			}
		})
	})
}

// And groups predicates with the AND operator between them.
func And(predicates ...predicate.Repository) predicate.Repository {
	return predicate.Repository(sql.AndPredicates(predicates...))
}

// Or groups predicates with the OR operator between them.
func Or(predicates ...predicate.Repository) predicate.Repository {
	return predicate.Repository(sql.OrPredicates(predicates...))
}

// Not applies the not operator on the given predicate.
func Not(p predicate.Repository) predicate.Repository {
	return predicate.Repository(sql.NotPredicates(p))
}