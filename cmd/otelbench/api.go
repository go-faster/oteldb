package main

import (
	"fmt"
	"os"

	"github.com/go-faster/errors"
	"github.com/spf13/cobra"

	"github.com/go-faster/oteldb/internal/otelbench"
	"github.com/go-faster/oteldb/internal/otelbotapi"
)

func newPingCommand(cfg *apiSettings) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ping",
		Short: "Send ping request to otelbench api",
		RunE: func(cmd *cobra.Command, _ []string) error {
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
		RunE: func(cmd *cobra.Command, _ []string) error {
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

type submitCommand struct {
	cfg *apiSettings
}

func (c *submitCommand) parse(data []byte) (*otelbotapi.SubmitReportReq, error) {
	var report otelbotapi.SubmitReportReq
	_ = data
	return &report, nil
}

func (c submitCommand) Run(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	client, err := c.cfg.Client()
	if err != nil {
		return errors.Wrap(err, "failed to create otelbotapi client")
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		return errors.Wrap(err, "failed to read file")
	}
	req, err := c.parse(data)
	if err != nil {
		return errors.Wrap(err, "failed to parse report")
	}
	if err := client.SubmitReport(ctx, req); err != nil {
		return errors.Wrap(err, "failed to submit report")
	}
	return nil
}

func newSubmitCommand(cfg *apiSettings) *cobra.Command {
	v := &submitCommand{cfg: cfg}
	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit benchmark to otelbench api",
		RunE:  v.Run,
		Args:  cobra.ExactArgs(1),
	}
	return cmd
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
		newSubmitCommand(cfg),
	)
	return cmd
}
