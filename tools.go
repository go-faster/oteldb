//go:build tools

package oteldb

import (
	_ "github.com/ogen-go/ogen"
	_ "github.com/ogen-go/ogen/middleware"

	_ "golang.org/x/tools/cmd/stringer"
)
