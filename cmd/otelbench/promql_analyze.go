package main

import (
	"fmt"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	yamlx "github.com/go-faster/yaml"
	"github.com/spf13/cobra"
)

type PromQLAnalyze struct {
	Input string
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

	for _, q := range report.Queries {
		if q.ID != 0 {
			fmt.Printf("[Q%d] ", q.ID)
		}
		if q.Query != "" {
			fmt.Println(q.Query)
		} else {
			fmt.Println(q.Matchers)
		}

		formatNanos := func(nanos int64) string {
			d := time.Duration(nanos) * time.Nanosecond
			return d.Round(time.Millisecond / 20).String()
		}
		fmt.Println(" duration:", formatNanos(q.DurationNanos))

		if len(q.Queries) > 0 {
			fmt.Println(" sql queries:", len(q.Queries))

			var memUsage, readBytes, readRows int64
			for _, v := range q.Queries {
				memUsage += v.MemoryUsage
				readBytes += v.ReadBytes
				readRows += v.ReadRows
			}

			fmt.Println(" memory usage:", humanize.Bytes(uint64(memUsage)))
			fmt.Println(" read bytes:", humanize.Bytes(uint64(readBytes)))
			fmt.Println(" read rows:", fmtInt(int(readRows)))
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
	return cmd
}
