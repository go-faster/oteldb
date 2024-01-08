package main

import (
	"fmt"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"github.com/go-faster/yaml"
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
	if err := yaml.Unmarshal(data, &report); err != nil {
		return errors.Wrap(err, "unmarshal yaml")
	}

	for _, q := range report.Queries {
		if q.Query != "" {
			fmt.Println("query:", q.Query)
		} else {
			fmt.Println("matchers:", q.Matchers)
		}
		fmt.Println(" sql:", len(q.Queries))
		fmt.Println(" duration:", time.Duration(q.DurationNanos)*time.Nanosecond)

		var memUsage, readBytes int64
		for _, v := range q.Queries {
			memUsage += v.MemoryUsage
			readBytes += v.ReadBytes
		}

		fmt.Println(" memory usage:", humanize.Bytes(uint64(memUsage)))
		fmt.Println(" read bytes:", humanize.Bytes(uint64(readBytes)))
	}

	return nil
}

func newPromQLAnalyzeCommand() *cobra.Command {
	p := &PromQLAnalyze{}
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Run promql queries",
		RunE: func(cmd *cobra.Command, args []string) error {
			return p.Run()
		},
	}
	f := cmd.Flags()
	f.StringVarP(&p.Input, "input", "i", "report.yml", "Input file")
	return cmd
}
