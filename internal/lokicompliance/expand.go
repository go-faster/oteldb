package lokicompliance

import (
	"slices"
	"strings"
	"text/template"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

var testVariantArgs = map[string][]string{
	"range":        {"1s", "15s", "1m", "5m", "15m", "1h"},
	"offset":       {"1m", "5m", "10m"},
	"simpleAggrOp": {"sum", "avg", "max", "min", "count", "stddev", "stdvar"},
	"topBottomOp":  {"topk", "bottomk"},
	"quantile": {
		"-0.5",
		"0.1",
		"0.5",
		"0.75",
		"0.95",
		"0.90",
		"0.99",
		"1",
		"1.5",
	},
	"lineFilterOp": {"|=", "!=", "~=", "!~"},
	"arithBinOp":   {"+", "-", "*", "/", "%", "^"},
	"logicBinOp":   {"and", "or", "unless"},
}

func execTemplateToString(t *template.Template, data any) (string, error) {
	var sb strings.Builder
	if err := t.Execute(&sb, data); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func getQueries(
	t *template.Template,
	variantArgs []string,
	args map[string]string,
	add func(string),
) error {
	if len(variantArgs) == 0 {
		q, err := execTemplateToString(t, args)
		if err != nil {
			return err
		}
		add(q)
		return nil
	}

	arg := variantArgs[0]
	values, ok := testVariantArgs[arg]
	if !ok {
		return errors.Errorf("unknown arg %q", arg)
	}
	variantArgs = variantArgs[1:]

	for _, val := range values {
		args[arg] = val
		if err := getQueries(t, variantArgs, args, add); err != nil {
			return err
		}
	}
	return nil
}

// ExpandQuery expands given test case.
func ExpandQuery(cfg *Config, start, end time.Time, step time.Duration) (r []*TestCase, _ error) {
	var (
		params    = cfg.QueryParameters
		limit     = 10000
		direction = lokiapi.DirectionForward
	)
	if l := params.Limit; l != nil {
		limit = *l
	}
	if d := params.Direction; d != "" {
		direction = d
	}

	for _, tc := range cfg.TestCases {
		templ, err := template.New("query").Parse(tc.Query)
		if err != nil {
			return nil, errors.Wrapf(err, "parse query template %q", tc.Query)
		}

		// Sort and deduplicate args.
		args := tc.VariantArgs
		slices.Sort(args)
		args = slices.Compact(args)

		if err := getQueries(
			templ,
			tc.VariantArgs,
			make(map[string]string, len(args)),
			func(query string) {
				r = append(r, &TestCase{
					Query:          query,
					SkipComparison: tc.SkipComparison,
					ShouldFail:     tc.ShouldFail,
					Start:          start,
					End:            end,
					Step:           step,
					Limit:          limit,
					Direction:      direction,
				})
			},
		); err != nil {
			return nil, errors.Wrapf(err, "expand query %q", tc.Query)
		}
	}

	return r, nil
}
