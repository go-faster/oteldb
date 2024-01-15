package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

type GitCommit struct {
	ent.Schema
}

// Fields of the GitCommit.
func (GitCommit) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").StorageKey("sha").Immutable().Unique().Comment("GitCommit SHA."),
		field.String("message").Comment("GitCommit message."),
		field.String("author_login").Comment("GitCommit author."),
		field.Int64("author_id").Comment("GitCommit author ID."),
		field.Time("date").Comment("GitCommit date."),
	}
}

func (GitCommit) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("repository", Repository.Type).Ref("commits").Unique().Comment("GitHub Repository."),
	}
}
