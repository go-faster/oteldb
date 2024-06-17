package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
	"github.com/spf13/cobra"
	"golang.org/x/perf/benchfmt"
)

type PromQLAnalyze struct {
	Input  string
	Format string
}

func (a PromQLAnalyze) Run() error {
	data, err := os.ReadFile(a.Input)
	if err != nil {
		return errors.Wrap(err, "read file")
	}
	var report PromQLReport
	if err := yamlx.Unmarshal(data, &report); err != nil {
		return errors.Wrap(err, "unmarshal yaml")
	}

	if a.Format == "" {
		a.Format = "pretty"
	}
	switch a.Format {
	case "pretty":
		return a.renderPretty(report, os.Stdout)
	case "benchstat":
		return a.renderBenchstat(report, os.Stdout)
	default:
		return errors.Errorf("unknown format %q", a.Format)
	}
}

func (a PromQLAnalyze) renderPretty(report PromQLReport, w io.Writer) error {
	var buf bytes.Buffer
	for _, q := range report.Queries {
		if q.ID != 0 {
			fmt.Fprintf(&buf, "[Q%d] ", q.ID)
		}
		if q.Query != "" {
			fmt.Fprintln(&buf, q.Query)
		} else {
			fmt.Fprintln(&buf, q.Matchers)
		}

		formatNanos := func(nanos int64) string {
			d := time.Duration(nanos) * time.Nanosecond
			return d.Round(time.Millisecond / 20).String()
		}
		fmt.Fprintln(&buf, " duration:", formatNanos(q.DurationNanos))

		if len(q.Queries) > 0 {
			fmt.Fprintln(&buf, " sql queries:", len(q.Queries))

			var memUsage, readBytes, readRows int64
			for _, v := range q.Queries {
				memUsage += v.MemoryUsage
				readBytes += v.ReadBytes
				readRows += v.ReadRows
			}

			fmt.Fprintln(&buf, " memory usage:", humanize.Bytes(uint64(memUsage)))
			fmt.Fprintln(&buf, " read bytes:", humanize.Bytes(uint64(readBytes)))
			fmt.Fprintln(&buf, " read rows:", fmtInt(int(readRows)))
		}
	}
	_, err := buf.WriteTo(w)
	return err
}

func (a PromQLAnalyze) renderBenchstat(report PromQLReport, w io.Writer) error {
	var recs []benchfmt.Result

	for _, q := range report.Queries {
		var name []byte
		if q.Title != "" {
			// Normalize title as benchmark name.
			for _, r := range q.Title {
				switch {
				case unicode.IsSpace(r):
					name = append(name, '_')
				case !strconv.IsPrint(r):
					s := strconv.QuoteRune(r)
					name = append(name, s[1:len(s)-1]...)
				default:
					name = utf8.AppendRune(name, r)
				}
			}
		}
		if len(name) == 0 {
			name = fmt.Appendf(name, "Query%d", q.ID)
		}

		rec := benchfmt.Result{
			Name: bytes.Join(
				[][]byte{
					[]byte(`PromQL`),
					name,
				},
				[]byte{'/'},
			),
		}
		rec.Values = append(rec.Values, benchfmt.Value{
			Value: float64(q.DurationNanos),
			Unit:  "ns/op",
		})

		var chDurationNanos, memUsage, readBytes, readRows int64
		for _, v := range q.Queries {
			chDurationNanos += v.DurationNanos
			memUsage += v.MemoryUsage
			readBytes += v.ReadBytes
			readRows += v.ReadRows
		}
		// NOTE(tdakkota): it is important to keep 'ns/op', `bytes/op`
		// 	suffix in Unit, because it lets benchstat to figure out measurement unit.
		rec.Values = append(rec.Values,
			benchfmt.Value{
				Value: float64(chDurationNanos),
				Unit:  "ch-ns/op",
			},
			benchfmt.Value{
				Value: float64(memUsage),
				Unit:  "ch-mem-bytes/op",
			},
			benchfmt.Value{
				Value: float64(readBytes),
				Unit:  "ch-read-bytes/op",
			},
			benchfmt.Value{
				Value: float64(readRows),
				Unit:  "ch-read-rows/op",
			},
		)

		recs = append(recs, rec)
	}
	slices.SortFunc(recs, func(a, b benchfmt.Result) int {
		return bytes.Compare(a.Name, b.Name)
	})

	fw := benchfmt.NewWriter(w)
	for i := range recs {
		if err := fw.Write(&recs[i]); err != nil {
			return err
		}
	}
	return nil
}

func newPromQLAnalyzeCommand() *cobra.Command {
	p := &PromQLAnalyze{}
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Run promql queries",
		RunE: func(*cobra.Command, []string) error {
			return p.Run()
		},
	}
	f := cmd.Flags()
	f.StringVarP(&p.Input, "input", "i", "report.yml", "Input file")
	f.StringVarP(&p.Format, "format", "f", "pretty", "Output format")
	return cmd
}
