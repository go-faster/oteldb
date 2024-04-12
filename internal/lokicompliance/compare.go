// Package lokicompliance provides utilities for Loki/LogQL compliance testing.
package lokicompliance

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-faster/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/go-faster/oteldb/internal/lokiapi"
)

const (
	defaultFraction = 0.00001
	defaultMargin   = 0.0
)

var _ LokiAPI = (*lokiapi.Client)(nil)

// LokiAPI represents LogQL API.
type LokiAPI interface {
	Query(ctx context.Context, params lokiapi.QueryParams) (*lokiapi.QueryResponse, error)
	QueryRange(ctx context.Context, params lokiapi.QueryRangeParams) (*lokiapi.QueryResponse, error)
}

// TestCase represents a fully expanded query to be tested.
type TestCase struct {
	Query          string            `json:"query"`
	SkipComparison bool              `json:"skipComparison"`
	ShouldFail     bool              `json:"shouldFail"`
	Start          time.Time         `json:"start"`
	End            time.Time         `json:"end"`
	Step           time.Duration     `json:"step"`
	Limit          int               `json:"limit"`
	Direction      lokiapi.Direction `json:"direction"`
}

// A Comparer allows comparing query results for test cases between a reference API and a test API.
type Comparer struct {
	refAPI         LokiAPI
	testAPI        LokiAPI
	compareOptions cmp.Options
}

// New returns a new Comparer.
func New(refAPI, testAPI LokiAPI) *Comparer {
	var options cmp.Options
	addFloatCompareOptions(&options)
	return &Comparer{
		refAPI:         refAPI,
		testAPI:        testAPI,
		compareOptions: options,
	}
}

// Result tracks a single test case's query comparison result.
type Result struct {
	TestCase          *TestCase `json:"testCase"`
	Diff              string    `json:"diff"`
	UnexpectedFailure string    `json:"unexpectedFailure"`
	UnexpectedSuccess bool      `json:"unexpectedSuccess"`
	Unsupported       bool      `json:"unsupported"`
}

// Success returns true if the comparison result was successful.
func (r *Result) Success() bool {
	return r.Diff == "" && !r.UnexpectedSuccess && r.UnexpectedFailure == ""
}

func getLokiTime(t time.Time) lokiapi.LokiTime {
	ts := strconv.FormatInt(t.UnixNano(), 10)
	return lokiapi.LokiTime(ts)
}

func getLokiDuration(t time.Duration) lokiapi.PrometheusDuration {
	return lokiapi.PrometheusDuration(t.String())
}

// Compare runs a test case query against the reference API and the test API and compares the results.
func (c *Comparer) Compare(ctx context.Context, tc *TestCase) (*Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	params := lokiapi.QueryRangeParams{
		Query:     tc.Query,
		Start:     lokiapi.NewOptLokiTime(getLokiTime(tc.Start)),
		End:       lokiapi.NewOptLokiTime(getLokiTime(tc.End)),
		Step:      lokiapi.NewOptPrometheusDuration(getLokiDuration(tc.Step)),
		Direction: lokiapi.NewOptDirection(tc.Direction),
		Limit:     lokiapi.NewOptInt(tc.Limit),
	}

	refResult, refErr := c.refAPI.QueryRange(ctx, params)
	testResult, testErr := c.testAPI.QueryRange(ctx, params)

	if (refErr != nil) != tc.ShouldFail {
		if refErr != nil {
			return nil, errors.Wrapf(refErr, "querying reference API for %q", tc.Query)
		}
		return nil, errors.Errorf("expected reference API query %q to fail, but succeeded", tc.Query)
	}

	if (testErr != nil) != tc.ShouldFail {
		if testErr != nil {
			var unsupported bool
			if esc, ok := errors.Into[*lokiapi.ErrorStatusCode](testErr); ok {
				unsupported = esc.StatusCode == http.StatusNotImplemented
			}
			return &Result{
				TestCase:          tc,
				UnexpectedFailure: testErr.Error(),
				Unsupported:       unsupported,
			}, nil
		}
		return &Result{TestCase: tc, UnexpectedSuccess: true}, nil
	}

	if tc.SkipComparison || tc.ShouldFail {
		return &Result{TestCase: tc}, nil
	}

	return &Result{
		TestCase: tc,
		Diff:     cmp.Diff(refResult, testResult, c.compareOptions),
	}, nil
}

type fpoint struct {
	T float64
	V float64
}

func addFloatCompareOptions(options *cmp.Options) {
	fraction := defaultFraction
	margin := defaultMargin
	*options = append(
		*options,
		cmpopts.EquateApprox(fraction, margin),
		// Ignore stats at all.
		cmp.Transformer("TranslateStats", func(stats *lokiapi.Stats) *lokiapi.Stats {
			return &lokiapi.Stats{}
		}),
		// Normalize label set value.
		cmp.Transformer("TranslateLabelSet", func(in lokiapi.OptLabelSet) lokiapi.OptLabelSet {
			if !in.Set || len(in.Value) == 0 {
				return lokiapi.OptLabelSet{Set: false}
			}
			return in
		}),
		// Translate fpoint strings into float64 so that cmpopts.EquateApprox() works.
		cmp.Transformer("TranslateFPoint", func(in lokiapi.FPoint) fpoint {
			v, _ := strconv.ParseFloat(in.V, 64)
			return fpoint{T: in.T, V: v}
		}),
		// A NaN is usually not treated as equal to another NaN, but we want to treat it as such here.
		cmpopts.EquateNaNs(),
	)
}
