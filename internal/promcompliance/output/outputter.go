package output

import (
	"github.com/go-faster/oteldb/internal/promcompliance/comparer"
	"github.com/go-faster/oteldb/internal/promcompliance/config"
)

// An Outputter outputs a number of test results.
type Outputter func(results []*comparer.Result, includePassing bool, tweaks []*config.QueryTweak)
