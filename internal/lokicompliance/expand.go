package lokicompliance

import (
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

var testVariantArgs = map[string][]string{
	"range":          {"1s", "15s", "1m", "5m", "15m", "1h"},
	"offset":         {"1m", "5m", "10m"},
	"simpleVecAggOp": {"sum", "avg", "max", "min", "count", "stddev", "stdvar"},
	"simpleRangeAggOp": {
		"count_over_time",
		"rate",
		"bytes_over_time",
		"bytes_rate",
	},
	"unwrapRangeAggOp": {
		"rate_counter",
		"avg_over_time",
		"sum_over_time",
		"min_over_time",
		"max_over_time",
		"stdvar_over_time",
		"stddev_over_time",
		"first_over_time",
		"last_over_time",
	},

	"topBottomOp": {"topk", "bottomk"},
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

var templateFuncMap = template.FuncMap{
	"quote": strconv.Quote,
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
		templ, err := template.New("query").
			Funcs(templateFuncMap).
			Parse(tc.Query)
		if err != nil {
			return nil, errors.Wrapf(err, "parse query template %q", tc.Query)
		}

		// Sort and deduplicate args.
		args := tc.VariantArgs
		slices.Sort(args)
		args = slices.Compact(args)

		if err := getQueries(
			templ,
			args,
			make(map[string]string, len(args)),
			func(query string) {
				r = append(r, &TestCase{
					Query:          query,
					SkipComparison: tc.SkipComparison,
					ShouldFail:     tc.ShouldFail,
					ShouldBeEmpty:  tc.ShouldBeEmpty,
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
