package output

import (
	"encoding/json"
	"fmt"

	"github.com/go-faster/oteldb/internal/promcompliance/comparer"
	"github.com/go-faster/oteldb/internal/promcompliance/config"
)

// JSON produces JSON-based output for a number of query results.
func JSON(results []*comparer.Result, includePassing bool, tweaks []*config.QueryTweak) {
	buf, err := json.Marshal(map[string]interface{}{
		"totalResults":   len(results), // Needed because we may exclude passing results.
		"results":        results,
		"includePassing": includePassing,
		"queryTweaks":    tweaks,
	})
	if err != nil {
		panic(err)
	}
	fmt.Print(string(buf))
}
