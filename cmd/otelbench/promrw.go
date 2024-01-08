package main

import "github.com/spf13/cobra"

func newPrometheusRemoteWrite() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "promrw",
		Short: "promrw is a benchmarking suite for prometheus remote write",
	}
	rootCmd.AddCommand(
		newRecordCommand(),
		newReplayCommand(),
		newBenchCommand(),
	)
	return rootCmd
}
