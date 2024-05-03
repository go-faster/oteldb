package logqlpattern

import (
	"strings"

	"github.com/go-faster/oteldb/internal/logql"
)

// Match matches given pattern against input string.
func Match(p Pattern, input string, match func(label logql.Label, value string)) bool {
	parts := p.Parts
	if len(parts) == 0 && input == "" {
		return true
	}

	var ok bool
	for i, part := range parts {
		switch part.Type {
		case Literal:
			// Consume literal from input.
			input, ok = strings.CutPrefix(input, part.Value)
			if !ok {
				return false
			}
		case Capture:
			var (
				label = logql.Label(part.Value)
				value string
			)
			if i+1 < len(parts) {
				// Capture everything until next part.
				next := parts[i+1]
				value, _, ok = strings.Cut(input, next.Value)
			} else {
				// Capture remaining string.
				value = input
				ok = true
			}
			input = input[len(value):]

			// Don't capture `_` labels.
			if label != "_" {
				match(label, value)
			}
			// If match failed, capture value anyway.
			if !ok || value == "" {
				return false
			}
		}
	}

	return input == ""
}
