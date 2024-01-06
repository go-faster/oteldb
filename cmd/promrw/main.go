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
		Short: "promrw is a tool for recording and sending prometheus remote write requests",
	}
	rootCmd.AddCommand(
		newRecorderCommand(),
		newSenderCommand(),
	)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}
