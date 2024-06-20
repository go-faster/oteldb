package main

import "github.com/spf13/cobra"

func newLogQLCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logql",
		Short: "Suite for LogQL benchmarks",
	}
	cmd.AddCommand(
		newLogQLBenchmarkCommand(),
	)
	return cmd
}
