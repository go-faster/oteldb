package main

import "github.com/spf13/cobra"

func newPromQLCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promql",
		Short: "Suite for promql benchmarks",
	}
	cmd.AddCommand(
		newPromQLBenchmarkCommand(),
		newPromQLAnalyzeCommand(),
		newPromQLConvertCommand(),
	)
	return cmd
}
