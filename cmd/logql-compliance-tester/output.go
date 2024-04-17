package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/lokicompliance"
)

func printOutput(results []*lokicompliance.Result, cfg OutputConfig) error {
	if p := cfg.OutputFile; p != "" {
		p = filepath.Clean(p)

		f, err := os.Create(p)
		if err != nil {
			return err
		}
		defer func() {
			_ = f.Close()
		}()

		if err := outp(f, results, cfg.OutputPassing); err != nil {
			return err
		}
	}

	return verifyTargetCompliance(results, cfg.MinimumPercentage)
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
	if !includePassing {
		var failed []*lokicompliance.Result
		for _, r := range results {
			if !r.Success() {
				failed = append(failed, r)
			}
		}
		results = failed
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
