package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/dustin/go-humanize"
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
		newDumpRestoreCommand(),
	)
	return rootCmd
}

func dumpTables() []string {
	return []string{
		"migration",
		"metrics_exemplars",
		"metrics_exp_histograms",
		"metrics_labels",
		"metrics_points",
		"traces_spans",
		"traces_tags",
		"logs_attrs",
		"logs",
	}
}

func newDumpCreateCommand() *cobra.Command {
	var arg struct {
		LimitTime  time.Duration
		LimitCount int

		Output      string
		Database    string
		Compression string

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

			fmt.Println("Dumping tables to", arg.Output)
			if err := os.MkdirAll(arg.Output, 0o755); err != nil {
				return errors.Wrap(err, "create output directory")
			}

			checkConnection := func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				client, err := ch.Dial(ctx, ch.Options{
					Address: fmt.Sprintf("localhost:%d", arg.LocalPort),
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
					fmt.Println("Clickhouse is already listening, not performing port-forwarding.")
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
				defer close(done)
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
				fmt.Println("Clickhouse connection is ready")
				tables := dumpTables()
				var files []os.FileInfo
				for _, table := range tables {
					query := fmt.Sprintf("SELECT * FROM %s.%s", arg.Database, table)
					query += " WHERE true"
					switch table {
					case "logs", "metrics_points", "traces_spans":
						tsField := "timestamp"
						if table == "traces_spans" {
							tsField = "start"
						}
						if arg.LimitTime > 0 {
							query += fmt.Sprintf(" AND %s > (now() - toIntervalSecond(%d))", tsField, int(arg.LimitTime.Seconds()))
						}
					}
					if arg.LimitCount > 0 {
						query += fmt.Sprintf(" LIMIT %d", arg.LimitCount)
					}

					// SELECT * FROM faster.logs INTO OUTFILE '/tmp/dump.bin' FORMAT Native;
					outFile := filepath.Join(arg.Output, fmt.Sprintf("%s.bin.%s", table, arg.Compression))
					query += fmt.Sprintf(" INTO OUTFILE '%s' TRUNCATE COMPRESSION '%s' FORMAT Native", outFile, arg.Compression)

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

					fmt.Println(">", table)
					if err := cmd.Run(); err != nil {
						return errors.Wrapf(err, "dump table %s", table)
					}

					stat, err := os.Stat(outFile)
					if err != nil {
						return errors.Wrap(err, "stat")
					}

					files = append(files, stat)
				}

				var totalBytes int64
				for _, file := range files {
					totalBytes += file.Size()
					// Pad to 35 characters, align left.
					fmt.Printf(" %35s %s\n", file.Name(), humanize.Bytes(uint64(file.Size())))
				}

				fmt.Printf(" %35s \n", "--------")
				fmt.Printf(" %35s %s\n", "Total", humanize.Bytes(uint64(totalBytes)))

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
	f.DurationVar(&arg.LimitTime, "duration", 0, "Limit oldest data with delta from now")
	f.IntVar(&arg.LimitCount, "limit", 0, "Limit oldest data with count")
	f.StringVar(&arg.Compression, "compression", "zstd", "Compression algorithm")

	return rootCmd
}

func newDumpRestoreCommand() *cobra.Command {
	var arg struct {
		Input       string
		Database    string
		Host        string
		Port        int
		Truncate    bool
		Compression string
	}
	rootCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a dump",
		RunE: func(cobraCommand *cobra.Command, _ []string) error {
			ctx := cobraCommand.Context()
			client, err := ch.Dial(ctx, ch.Options{
				Address:  net.JoinHostPort(arg.Host, strconv.Itoa(arg.Port)),
				Database: arg.Database,
			})
			if err != nil {
				return errors.Wrap(err, "dial")
			}

			truncate := func(ctx context.Context, table string) error {
				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				defer cancel()

				query := fmt.Sprintf("TRUNCATE TABLE %s.%s", arg.Database, table)
				if err := client.Do(ctx, ch.Query{Body: query}); err != nil {
					return errors.Wrap(err, "truncate")
				}

				return nil
			}

			tables := dumpTables()
			var found bool
			for _, table := range tables {
				file := filepath.Join(arg.Input, fmt.Sprintf("%s.bin.%s", table, arg.Compression))
				if _, err := os.Stat(file); err != nil {
					if os.IsNotExist(err) {
						fmt.Printf("Table %s not found\n", table)
						continue
					}
					return errors.Wrap(err, "stat")
				}
				if arg.Truncate {
					if err := truncate(ctx, table); err != nil {
						return errors.Wrapf(err, "truncate %s", table)
					}
				}

				found = true
				if err := func() error {
					q := fmt.Sprintf("INSERT INTO %s.%s FROM INFILE '%s' COMPRESSION '%s' FORMAT Native",
						arg.Database, table, file, arg.Compression,
					)
					args := []string{
						"-h", arg.Host,
						"-d", arg.Database,
						"--progress",
						"-q", q,
					}
					cmd := exec.CommandContext(ctx, "clickhouse-client", args...)
					cmd.Stderr = os.Stderr
					cmd.Stdout = os.Stdout

					fmt.Println(">", table)
					if err := cmd.Run(); err != nil {
						return errors.Wrapf(err, "restore %s", table)
					}

					return nil
				}(); err != nil {
					return errors.Wrapf(err, "restore %s", file)
				}
			}
			if !found {
				return errors.New("no tables found")
			}
			return nil
		},
	}

	f := rootCmd.Flags()
	f.StringVarP(&arg.Input, "input", "i", "", "Input directory")
	f.StringVarP(&arg.Database, "database", "d", "default", "Database name")
	f.IntVar(&arg.Port, "port", 9000, "Clickhouse port")
	f.StringVar(&arg.Host, "host", "localhost", "Clickhouse host")
	f.BoolVar(&arg.Truncate, "truncate", false, "Truncate tables before restore")
	f.StringVar(&arg.Compression, "compression", "zstd", "Compression algorithm")

	return rootCmd
}
