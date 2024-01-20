package entdb

import (
	"database/sql"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/go-faster/errors"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/go-faster/oteldb/internal/otelbench/ent"
)

// Open new connection
func Open(uri string) (*ent.Client, error) {
	db, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, errors.Wrap(err, "sql.Open")
	}

	// Create an ent.Driver from `db`.
	drv := entsql.OpenDB(dialect.Postgres, db)
	return ent.NewClient(ent.Driver(drv)), nil
}
