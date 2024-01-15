//go:build ignore
// +build ignore

package main

import (
	"log"

	"entgo.io/ent/entc"
	"entgo.io/ent/entc/gen"
	"github.com/go-faster/errors"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func run() error {
	if err := entc.Generate("./schema", &gen.Config{
		Features: []gen.Feature{
			gen.FeatureUpsert,
			gen.FeatureVersionedMigration,
			gen.FeatureIntercept,
			gen.FeatureNamedEdges,
		},
	}); err != nil {
		return errors.Wrap(err, "ent codegen")
	}

	return nil
}
