//go:build tools

package oteldb

import (
	_ "entgo.io/ent/cmd/ent"
	_ "github.com/dmarkham/enumer"
	_ "github.com/ogen-go/ogen"
	_ "github.com/ogen-go/ogen/middleware"
	_ "golang.org/x/tools/cmd/stringer"
)
