// Binary promql-compliance-tester performs promql compliance testing based on provided
// configuration, comparing results with reference implementation.
//
// Fork of https://github.com/prometheus/compliance.
//
// Changes:
// - Added more configuration arguments
// - Embedded default HTML template
// - Cleaned up JSON output with omitempty
// - Fixed issues reported by linters
package main

import (
	"flag"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/go-faster/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"go.uber.org/atomic"

	"github.com/go-faster/oteldb/internal/promcompliance/comparer"
	"github.com/go-faster/oteldb/internal/promcompliance/config"
	"github.com/go-faster/oteldb/internal/promcompliance/output"
	"github.com/go-faster/oteldb/internal/promcompliance/testcases"
)

func newPromAPI(targetConfig config.TargetConfig) (v1.API, error) {
	apiConfig := api.Config{Address: targetConfig.QueryURL}
	if len(targetConfig.Headers) > 0 || targetConfig.BasicAuthUser != "" {
		apiConfig.RoundTripper = roundTripperWithSettings{headers: targetConfig.Headers, basicAuthUser: targetConfig.BasicAuthUser, basicAuthPass: targetConfig.BasicAuthPass}
	}
	client, err := api.NewClient(apiConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "creating Prometheus API client for %q: %v", targetConfig.QueryURL, err)
	}

	return v1.NewAPI(client), nil
}

type roundTripperWithSettings struct {
	headers       map[string]string
	basicAuthUser string
	basicAuthPass string
}

func (rt roundTripperWithSettings) RoundTrip(req *http.Request) (*http.Response, error) {
	// Per RoundTrip's documentation, RoundTrip should not modify the request,
	// except for consuming and closing the Request's Body.
	// TODO: Update the Go Prometheus client code to support adding headers to request.

	if rt.basicAuthUser != "" {
		req.SetBasicAuth(rt.basicAuthUser, rt.basicAuthPass)
	}

	for key, value := range rt.headers {
		if strings.EqualFold(key, "host") {
			req.Host = value
		} else {
			req.Header.Add(key, value)
		}
	}
	return http.DefaultTransport.RoundTrip(req)
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	var configFiles arrayFlags
	flag.Var(&configFiles, "config-file", "The path to the configuration file. If repeated, the specified files will be concatenated before YAML parsing.")
	outputFormat := flag.String("output-format", "text", "The comparison output format. Valid values: [text, html, json]")
	outputHTMLTemplate := flag.String("output-html-template", "", "The HTML template to use when using HTML as the output format.")
	outputPassing := flag.Bool("output-passing", false, "Whether to also include passing test cases in the output.")
	queryParallelism := flag.Int("query-parallelism", 20, "Maximum number of comparison queries to run in parallel.")
	endDelta := flag.Duration("end", 12*time.Minute, "The delta between the end time and current time, negated")
	rangeDuration := flag.Duration("range", 10*time.Minute, "The duration of the query range.")
	resolutionDuration := flag.Duration("resolution", 10*time.Second, "The resolution of the query.")
	flag.Parse()

	var outp output.Outputter
	switch *outputFormat {
	case "text":
		outp = output.Text
	case "html":
		var err error
		outp, err = output.HTML(*outputHTMLTemplate)
		if err != nil {
			log.Fatalf("Error reading output HTML template: %v", err)
		}
	case "json":
		outp = output.JSON
	case "tsv":
		outp = output.TSV
	default:
		log.Fatalf("Invalid output format %q", *outputFormat)
	}

	cfg, err := config.LoadFromFiles(configFiles)
	if err != nil {
		log.Fatalf("Error loading configuration file: %v", err)
	}
	refAPI, err := newPromAPI(cfg.ReferenceTargetConfig)
	if err != nil {
		log.Fatalf("Error creating reference API: %v", err)
	}
	testAPI, err := newPromAPI(cfg.TestTargetConfig)
	if err != nil {
		log.Fatalf("Error creating test API: %v", err)
	}

	comp := comparer.New(refAPI, testAPI, cfg.QueryTweaks)

	end := getTime(cfg.QueryTimeParameters.EndTime, time.Now().Add(-*endDelta))
	start := end.Add(
		-getNonZeroDuration(cfg.QueryTimeParameters.RangeInSeconds, *rangeDuration),
	)
	resolution := getNonZeroDuration(
		cfg.QueryTimeParameters.ResolutionInSeconds, *resolutionDuration,
	)
	expandedTestCases := testcases.ExpandTestCases(cfg.TestCases, cfg.QueryTweaks, start, end, resolution)

	var (
		results     = make([]*comparer.Result, len(expandedTestCases))
		progressBar = pb.StartNew(len(results))
	)
	// Progress bar messes up Github Actions logs, so keep it static.
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		progressBar.SetTemplate(pb.Default + `{{ "\n" }}`)
	}

	var wg sync.WaitGroup
	wg.Add(len(results))
	workCh := make(chan struct{}, *queryParallelism)

	allSuccess := atomic.NewBool(true)
	for i, tc := range expandedTestCases {
		workCh <- struct{}{}

		go func(i int, tc *comparer.TestCase) {
			res, err := comp.Compare(tc)
			if err != nil {
				log.Fatalf("Error running comparison: %v", err)
			}
			results[i] = res
			if !res.Success() {
				allSuccess.Store(false)
			}
			progressBar.Increment()
			<-workCh
			wg.Done()
		}(i, tc)
	}

	wg.Wait()
	progressBar.Finish()

	outp(results, *outputPassing, cfg.QueryTweaks)

	if !allSuccess.Load() {
		os.Exit(1)
	}
}

func getTime(timeStr string, defaultTime time.Time) time.Time {
	result, err := parseTime(timeStr)
	if err != nil {
		return defaultTime
	}
	return result
}

func getNonZeroDuration(
	seconds float64, defaultDuration time.Duration,
) time.Duration {
	if seconds == 0.0 {
		return defaultDuration
	}
	return time.Duration(seconds * float64(time.Second))
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}
