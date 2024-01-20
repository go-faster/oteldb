package main

import (
	"fmt"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/otelbench"
	"github.com/go-faster/oteldb/internal/otelbotapi"
)

func newPingCommand(cfg *apiSettings) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ping",
		Short: "Send ping request to otelbench api",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := cfg.Client()
			if err != nil {
				return errors.Wrap(err, "failed to create otelbotapi client")
			}
			if err := client.Ping(cmd.Context()); err != nil {
				return errors.Wrap(err, "failed to ping otelbench api")
			}
			fmt.Println("Ping successful")
			return nil
		},
	}
	return cmd
}

func newStatusCommand(cfg *apiSettings) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Get status of otelbench api",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := cfg.Client()
			if err != nil {
				return errors.Wrap(err, "failed to create otelbotapi client")
			}
			status, err := client.GetStatus(cmd.Context())
			if err != nil {
				return errors.Wrap(err, "failed to get status of otelbench api")
			}
			fmt.Println("Status:", status.Status)
			return nil
		},
	}
	return cmd
}

type apiSettings struct {
	httpAddr string
}

func (a *apiSettings) Client() (*otelbotapi.Client, error) {
	return otelbotapi.NewClient(a.httpAddr, otelbench.EnvSecuritySource())
}

func newAPICommand() *cobra.Command {
	cfg := &apiSettings{}
	cmd := &cobra.Command{
		Use:   "api",
		Short: "Commands to interact with otelbench api",
	}
	cmd.PersistentFlags().StringVar(&cfg.httpAddr, "http-addr", "https://bot.oteldb.tech", "http address of otelbench api")
	cmd.AddCommand(
		newPingCommand(cfg),
		newStatusCommand(cfg),
	)
	return cmd
}
