package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/lokiapi"
	"github.com/go-faster/oteldb/internal/lokicompliance"
	"github.com/go-faster/oteldb/internal/lokihandler"
)

func expandTestCase(cfg *lokicompliance.Config, start, end time.Time, step time.Duration) []*lokicompliance.TestCase {
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

	r := make([]*lokicompliance.TestCase, 0, len(cfg.TestCases))
	for _, tc := range cfg.TestCases {
		r = append(r, &lokicompliance.TestCase{
			Query:          tc.Query,
			SkipComparison: tc.SkipComparison,
			ShouldFail:     tc.ShouldFail,
			Start:          start,
			End:            end,
			Step:           step,
			Limit:          limit,
			Direction:      direction,
		})
	}
	return r
}

func getNonZeroDuration(
	seconds float64,
	defaultDuration time.Duration,
) time.Duration {
	if seconds == 0.0 {
		return defaultDuration
	}
	return time.Duration(seconds * float64(time.Second))
}

func outp(output io.Writer, results []*lokicompliance.Result, includePassing bool) error {
	// JSONResult is the JSON output format.
	type JSONResult struct {
		TotalResults   int                      `json:"totalResults"`
		Results        []*lokicompliance.Result `json:"results,omitempty"`
		IncludePassing bool                     `json:"includePassing"`
	}

	buf, err := json.Marshal(JSONResult{
		TotalResults:   len(results),
		Results:        results,
		IncludePassing: includePassing,
	})
	if err != nil {
		return err
	}

	_, err = output.Write(buf)
	return err
}

func verifyTargetCompliance(results []*lokicompliance.Result, target float64) error {
	var successes, unsupported int
	for _, res := range results {
		if res.Success() {
			successes++
		}
		if res.Unsupported {
			unsupported++
		}
	}

	fmt.Println(strings.Repeat("=", 80))
	successPercentage := 100 * float64(successes) / float64(len(results))

	fmt.Printf("Total: %d / %d (%.2f%%) passed, %d unsupported\n",
		successes, len(results), successPercentage, unsupported,
	)
	fmt.Printf("Target: %.2f%%\n", target)

	if successPercentage < target {
		fmt.Println(color.RedString("FAILED"))
		return errors.Errorf("target is %v, passed is %.2f", target, successPercentage)
	}

	fmt.Println(color.GreenString("PASSED"))
	return nil
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	if i == nil {
		return ""
	}
	return strings.Join(*i, ",")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func run(ctx context.Context) error {
	var configFile arrayFlags
	flag.Var(&configFile, "config-file", "The path to the configuration file. If repeated, the specified files will be concatenated before YAML parsing.")
	var (
		queryParallelism = flag.Int("query-parallelism", 20, "Maximum number of comparison queries to run in parallel.")
		endDelta         = flag.Duration("end", 12*time.Minute, "The delta between the end time and current time, negated")
		rangeDuration    = flag.Duration("range", 10*time.Minute, "The duration of the query range.")
		stepDuration     = flag.Duration("step", 10*time.Second, "The step of the query.")

		outputFile        = flag.String("output-file", "", "Path to output file")
		outputFormat      = flag.String("output-format", "text", "The comparison output format. Valid values: [json]")
		outputPassing     = flag.Bool("output-passing", false, "Whether to also include passing test cases in the output.")
		minimumPercentage = flag.Float64("target", 0, "Minimum compliance percentage")
	)
	flag.Parse()

	if format := *outputFormat; format != "json" {
		return errors.Errorf("unknown output format %q", format)
	}

	cfg, err := lokicompliance.LoadFromFiles(configFile)
	if err != nil {
		return errors.Wrap(err, "loading configuration file")
	}
	refAPI, err := newLokiAPI(ctx, cfg.ReferenceTargetConfig)
	if err != nil {
		return errors.Wrap(err, "creating reference API")
	}
	testAPI, err := newLokiAPI(ctx, cfg.TestTargetConfig)
	if err != nil {
		return errors.Wrap(err, "creating test API")
	}
	comp := lokicompliance.New(refAPI, testAPI)

	end, err := lokihandler.ParseTimestamp(cfg.QueryParameters.EndTime, time.Now().Add(-*endDelta))
	if err != nil {
		return errors.Wrap(err, "parse end")
	}
	start := end.Add(-getNonZeroDuration(
		cfg.QueryParameters.RangeInSeconds,
		*rangeDuration,
	))
	step := getNonZeroDuration(cfg.QueryParameters.StepInSeconds, *stepDuration)

	var (
		testCases = expandTestCase(cfg, start, end, step)
		results   = make([]*lokicompliance.Result, len(testCases))

		wg          sync.WaitGroup
		progressBar = pb.StartNew(len(results))
		workCh      = make(chan struct{}, *queryParallelism)
	)
	wg.Add(len(results))

	allSuccess := atomic.NewBool(true)
	for i, tc := range testCases {
		workCh <- struct{}{}

		go func(i int, tc *lokicompliance.TestCase) {
			defer func() {
				<-workCh
				wg.Done()
			}()

			res, err := comp.Compare(tc)
			if err != nil {
				zctx.From(ctx).Error("Compare failed", zap.Error(err))
				return
			}
			results[i] = res
			if !res.Success() {
				allSuccess.Store(false)
			}
			progressBar.Increment()
		}(i, tc)
	}

	wg.Wait()
	progressBar.Finish()

	if !allSuccess.Load() {
		return errors.New("some queries are failed")
	}

	if p := *outputFile; p != "" {
		p = filepath.Clean(p)

		f, err := os.Create(p)
		if err != nil {
			return err
		}
		defer func() {
			_ = f.Close()
		}()

		if err := outp(f, results, *outputPassing); err != nil {
			return err
		}
	}

	if target := *minimumPercentage; target != 0 {
		if err := verifyTargetCompliance(results, target); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%+v", err)
		os.Exit(1)
	}
}
