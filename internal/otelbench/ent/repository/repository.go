// Code generated by ent, DO NOT EDIT.

package repository

import (
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
)

const (
	// Label holds the string label denoting the repository type in the database.
	Label = "repository"
	// FieldID holds the string denoting the id field in the database.
	FieldID = "id"
	// FieldName holds the string denoting the name field in the database.
	FieldName = "name"
	// FieldFullName holds the string denoting the full_name field in the database.
	FieldFullName = "full_name"
	// FieldHTMLURL holds the string denoting the html_url field in the database.
	FieldHTMLURL = "html_url"
	// FieldDescription holds the string denoting the description field in the database.
	FieldDescription = "description"
	// FieldLastPushedAt holds the string denoting the last_pushed_at field in the database.
	FieldLastPushedAt = "last_pushed_at"
	// FieldLastEventAt holds the string denoting the last_event_at field in the database.
	FieldLastEventAt = "last_event_at"
	// EdgeOrganization holds the string denoting the organization edge name in mutations.
	EdgeOrganization = "organization"
	// EdgeCommits holds the string denoting the commits edge name in mutations.
	EdgeCommits = "commits"
	// GitCommitFieldID holds the string denoting the ID field of the GitCommit.
	GitCommitFieldID = "sha"
	// Table holds the table name of the repository in the database.
	Table = "repositories"
	// OrganizationTable is the table that holds the organization relation/edge.
	OrganizationTable = "repositories"
	// OrganizationInverseTable is the table name for the Organization entity.
	// It exists in this package in order to avoid circular dependency with the "organization" package.
	OrganizationInverseTable = "organizations"
	// OrganizationColumn is the table column denoting the organization relation/edge.
	OrganizationColumn = "organization_repositories"
	// CommitsTable is the table that holds the commits relation/edge.
	CommitsTable = "git_commits"
	// CommitsInverseTable is the table name for the GitCommit entity.
	// It exists in this package in order to avoid circular dependency with the "gitcommit" package.
	CommitsInverseTable = "git_commits"
	// CommitsColumn is the table column denoting the commits relation/edge.
	CommitsColumn = "repository_commits"
)

// Columns holds all SQL columns for repository fields.
var Columns = []string{
	FieldID,
	FieldName,
	FieldFullName,
	FieldHTMLURL,
	FieldDescription,
	FieldLastPushedAt,
	FieldLastEventAt,
}

// ForeignKeys holds the SQL foreign-keys that are owned by the "repositories"
// table and are not defined as standalone fields in the schema.
var ForeignKeys = []string{
	"organization_repositories",
}

// ValidColumn reports if the column name is valid (part of the table columns).
func ValidColumn(column string) bool {
	for i := range Columns {
		if column == Columns[i] {
			return true
		}
	}
	for i := range ForeignKeys {
		if column == ForeignKeys[i] {
			return true
		}
	}
	return false
}

var (
	// DefaultDescription holds the default value on creation for the "description" field.
	DefaultDescription string
)

// OrderOption defines the ordering options for the Repository queries.
type OrderOption func(*sql.Selector)

// ByID orders the results by the id field.
func ByID(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldID, opts...).ToFunc()
}

// ByName orders the results by the name field.
func ByName(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldName, opts...).ToFunc()
}

// ByFullName orders the results by the full_name field.
func ByFullName(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldFullName, opts...).ToFunc()
}

// ByHTMLURL orders the results by the html_url field.
func ByHTMLURL(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldHTMLURL, opts...).ToFunc()
}

// ByDescription orders the results by the description field.
func ByDescription(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldDescription, opts...).ToFunc()
}

// ByLastPushedAt orders the results by the last_pushed_at field.
func ByLastPushedAt(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldLastPushedAt, opts...).ToFunc()
}

// ByLastEventAt orders the results by the last_event_at field.
func ByLastEventAt(opts ...sql.OrderTermOption) OrderOption {
	return sql.OrderByField(FieldLastEventAt, opts...).ToFunc()
}

// ByOrganizationField orders the results by organization field.
func ByOrganizationField(field string, opts ...sql.OrderTermOption) OrderOption {
	return func(s *sql.Selector) {
		sqlgraph.OrderByNeighborTerms(s, newOrganizationStep(), sql.OrderByField(field, opts...))
	}
}

// ByCommitsCount orders the results by commits count.
func ByCommitsCount(opts ...sql.OrderTermOption) OrderOption {
	return func(s *sql.Selector) {
		sqlgraph.OrderByNeighborsCount(s, newCommitsStep(), opts...)
	}
}

// ByCommits orders the results by commits terms.
func ByCommits(term sql.OrderTerm, terms ...sql.OrderTerm) OrderOption {
	return func(s *sql.Selector) {
		sqlgraph.OrderByNeighborTerms(s, newCommitsStep(), append([]sql.OrderTerm{term}, terms...)...)
	}
}
func newOrganizationStep() *sqlgraph.Step {
	return sqlgraph.NewStep(
		sqlgraph.From(Table, FieldID),
		sqlgraph.To(OrganizationInverseTable, FieldID),
		sqlgraph.Edge(sqlgraph.M2O, true, OrganizationTable, OrganizationColumn),
	)
}
func newCommitsStep() *sqlgraph.Step {
	return sqlgraph.NewStep(
		sqlgraph.From(Table, FieldID),
		sqlgraph.To(CommitsInverseTable, GitCommitFieldID),
		sqlgraph.Edge(sqlgraph.O2M, false, CommitsTable, CommitsColumn),
	)
}