package main

import "github.com/spf13/cobra"

func newOtelCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "otel",
		Short: "otel is a benchmarking suite for OpenTelemetry signals",
	}
	rootCmd.AddCommand(
		newOtelLogsCommand(),
	)
	return rootCmd
}

func newOtelLogsCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "logs",
		Short: "logs is a benchmarking suite for OpenTelemetry logs",
	}
	rootCmd.AddCommand(
		newOtelLogsBenchCommand(),
	)
	return rootCmd
}
