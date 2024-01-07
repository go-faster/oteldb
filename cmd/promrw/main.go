// Binary promrw implements prometheusremotewrite receiver that can record
// requests or send them to specified target.
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "promrw",
		Short: "promrw is a benchmarking suite for prometheus remote write",

		SilenceUsage: true,
	}
	rootCmd.AddCommand(
		newRecordCommand(),
		newReplayCommand(),
		newBenchCommand(),
	)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}
