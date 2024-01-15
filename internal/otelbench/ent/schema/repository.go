package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type Repository struct {
	ent.Schema
}

func (Repository) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Unique().Immutable().Comment("GitHub repository ID."),
		field.String("name").Comment("GitHub repository name."),
		field.String("full_name").Comment("GitHub repository full name.").Unique(),
		field.String("html_url").Comment("GitHub repository URL.").Optional(),
		field.String("description").Default("").Comment("GitHub repository description."),
		field.Time("last_pushed_at").Optional(),
		field.Time("last_event_at").Optional(),
	}
}

func (Repository) Indexes() []ent.Index {
	return []ent.Index{}
}

func (Repository) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("organization", Organization.Type).Ref("repositories").Unique().Comment("GitHub organization."),
		edge.To("commits", GitCommit.Type).Comment("Commits."),
	}
}
