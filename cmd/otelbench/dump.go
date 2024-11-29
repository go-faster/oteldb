package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/go-faster/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newDumpCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "dump",
		Short: "Manage raw dumps",
	}
	rootCmd.AddCommand(
		newDumpCreateCommand(),
	)
	return rootCmd
}

func newDumpCreateCommand() *cobra.Command {
	var arg struct {
		Output   string
		Database string

		KubernetesNamespace string
		KubernetesService   string

		RemotePort int
		LocalPort  int
	}
	rootCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new dump",
		RunE: func(cobraCommand *cobra.Command, _ []string) error {
			g, ctx := errgroup.WithContext(cobraCommand.Context())
			done := make(chan struct{})
			localPort := 9000

			if err := os.MkdirAll(arg.Output, 0755); err != nil {
				return errors.Wrap(err, "create output directory")
			}

			checkConnection := func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				client, err := ch.Dial(ctx, ch.Options{
					Address: fmt.Sprintf("localhost:%d", localPort),
				})
				if err != nil {
					return errors.Wrap(err, "dial")
				}
				if err := client.Ping(ctx); err != nil {
					return errors.Wrap(err, "ping")
				}
				if err := client.Close(); err != nil {
					return errors.Wrap(err, "close")
				}
				return nil
			}

			g.Go(func() error {
				if err := checkConnection(ctx); err == nil {
					// Already connected.
					fmt.Println("Already connected")
					return nil
				}

				ctx, cancel := context.WithCancel(ctx)
				defer cancel()

				go func() {
					select {
					case <-done:
					case <-ctx.Done():
					}
					cancel()
				}()
				args := []string{
					"-n", arg.KubernetesNamespace,
					"port-forward",
					fmt.Sprintf("svc/%s", arg.KubernetesService),
					fmt.Sprintf("%d:%d", arg.LocalPort, arg.RemotePort),
				}
				cmd := exec.CommandContext(ctx, "kubectl", args...)
				cmd.Stderr = os.Stderr
				cmd.Stdout = os.Stdout

				return cmd.Run()
			})
			g.Go(func() error {
				// Wait for port-forward to be ready.
				for {
					if err := checkConnection(ctx); err == nil {
						break
					}
					select {
					case <-time.After(1 * time.Second):
						continue
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				fmt.Println("Port-forward ready")
				tables := []string{
					"logs",
					"logs_attrs",
					"metrics_exemplars",
					"metrics_exp_histograms",
					"metrics_labels",
					"metrics_points",
					"traces_spans",
					"traces_tags",
					"migration",
				}
				for _, table := range tables {
					query := fmt.Sprintf("SELECT * FROM %s.%s", arg.Database, table)
					// SELECT * FROM faster.logs INTO OUTFILE '/tmp/dump.bin' FORMAT Native;
					outFile := filepath.Join(arg.Output, fmt.Sprintf("%s.bin", table))
					query += fmt.Sprintf(" INTO OUTFILE '%s' TRUNCATE FORMAT Native", outFile)

					args := []string{
						"-h", "localhost",
						"-d", arg.Database,
						"--progress",
						"-q", query,
					}
					cmd := exec.CommandContext(ctx, "clickhouse-client", args...)
					cmd.Stderr = os.Stderr
					cmd.Stdout = os.Stdout
					cmd.Stdin = os.Stdin

					fmt.Println("Dumping table", table)
					fmt.Println(" Query:", query)
					fmt.Println(" Command:", cmd.String())

					if err := cmd.Run(); err != nil {
						return errors.Wrapf(err, "dump table %s", table)
					}
				}

				return nil
			})

			return g.Wait()
		},
	}
	f := rootCmd.Flags()
	f.StringVarP(&arg.Output, "output", "o", "", "Output directory")
	f.StringVarP(&arg.Database, "database", "d", "faster", "Database name")
	f.StringVar(&arg.KubernetesNamespace, "kubernetes-namespace", "clickhouse", "Kubernetes namespace")
	f.StringVar(&arg.KubernetesService, "kubernetes-service", "chi-db-cluster-0-0", "Kubernetes service")
	f.IntVar(&arg.RemotePort, "target-port", 9000, "Remote port")
	f.IntVar(&arg.LocalPort, "port", 9000, "Local port")

	return rootCmd
}
