// Binary otelbench implements benchmarking suite for oteldb.
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "otelbench",
		Short: "otelbench is a benchmarking suite for querying oteldb",

		SilenceUsage: true,
	}
	rootCmd.AddCommand(
		newPromQLCommand(),
	)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}
