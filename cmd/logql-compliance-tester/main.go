package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/fatih/color"
	"github.com/go-faster/errors"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/lokicompliance"
	"github.com/go-faster/oteldb/internal/lokihandler"
)

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
	for _, r := range results {
		if d := r.Diff; d != "" && !r.Unsupported {
			fmt.Printf("%q:\n%s\n", r.TestCase.Query, d)
		}
	}

	// JSONResult is the JSON output format.
	type JSONResult struct {
		TotalResults   int                      `json:"totalResults"`
		Results        []*lokicompliance.Result `json:"results,omitempty"`
		IncludePassing bool                     `json:"includePassing"`
	}

	buf, err := json.MarshalIndent(JSONResult{
		TotalResults:   len(results),
		Results:        results,
		IncludePassing: includePassing,
	}, "", "\t")
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
	if math.IsNaN(target) {
		return nil
	}
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
		minimumPercentage = flag.Float64("target", math.NaN(), "Minimum compliance percentage")
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

	testCases, err := lokicompliance.ExpandQuery(cfg, start, end, step)
	if err != nil {
		return errors.Wrap(err, "expand test cases")
	}

	var (
		results     = make([]*lokicompliance.Result, len(testCases))
		progressBar = pb.StartNew(len(results))
	)

	grp, grpCtx := errgroup.WithContext(ctx)
	if n := *queryParallelism; n > 0 {
		grp.SetLimit(n)
	}

	for i, tc := range testCases {
		i, tc := i, tc
		grp.Go(func() error {
			ctx := grpCtx

			res, err := comp.Compare(ctx, tc)
			if err != nil {
				return errors.Wrap(err, "compare")
			}
			results[i] = res

			progressBar.Increment()
			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return errors.Wrap(err, "run queries")
	}
	progressBar.Finish()

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

	return verifyTargetCompliance(results, *minimumPercentage)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
