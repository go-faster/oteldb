package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type Organization struct {
	ent.Schema
}

func (Organization) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").Unique().Immutable().Comment("GitHub organization ID."),
		field.String("name").Comment("GitHub organization name."),
		field.String("html_url").Comment("GitHub organization URL.").Optional(),
	}
}

func (Organization) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("repositories", Repository.Type).Comment("GitHub repositories."),
	}
}
