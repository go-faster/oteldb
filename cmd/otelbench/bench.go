package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/promapi"
)

type Bench struct {
	addr                       string
	node                       atomic.Pointer[[]byte]
	cfg                        atomic.Pointer[config]
	nodeExporterAddr           string
	clickhouseAddr             string
	queryAddr                  string
	queryInterval              time.Duration
	agentAddr                  string
	targetsCount               int
	pollExporterInterval       time.Duration
	scrapeInterval             time.Duration
	scrapeConfigUpdateInterval time.Duration
	scrapeConfigUpdatePercent  float64
	useVictoria                bool
	targets                    []string
	metricsInfo                atomic.Pointer[metricsInfo]
	storageInfo                atomic.Pointer[storageInfo]
}

type metricsInfo struct {
	Count int
	Size  int
	Hash  string
}

func (s *Bench) PollNodeExporter(ctx context.Context) {
	ticker := time.NewTicker(s.pollExporterInterval)
	defer ticker.Stop()
	if err := s.fetchNodeExporter(ctx); err != nil {
		zctx.From(ctx).Error("cannot fetch node exporter", zap.Error(err))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.fetchNodeExporter(ctx); err != nil {
				zctx.From(ctx).Error("cannot fetch node exporter", zap.Error(err))
			}
		}
	}
}

func (s *Bench) fetchNodeExporter(ctx context.Context) error {
	u := &url.URL{
		Scheme: "http",
		Path:   "/metrics",
		Host:   s.nodeExporterAddr,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	s.node.Store(&data)

	// Count metrics.
	var metricsCount int
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		metricsCount++
	}
	d := sha256.Sum256(data)
	h := fmt.Sprintf("%x", d[:8])
	s.metricsInfo.Store(&metricsInfo{
		Count: metricsCount,
		Size:  len(data),
		Hash:  h,
	})
	return nil
}

func (s *Bench) HandleConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	v := s.cfg.Load()
	_, _ = w.Write(v.marshalYAML())
}

func (s *Bench) ProgressConfig(ctx context.Context) error {
	// https://github.com/VictoriaMetrics/prometheus-benchmark/blob/50c5891/services/vmagent-config-updater/main.go#L33-L48
	rev := 0
	r := rand.New(rand.NewSource(1)) // #nosec G404
	p := s.scrapeConfigUpdatePercent / 100
	ticker := time.NewTicker(s.scrapeConfigUpdateInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			rev++
			revStr := fmt.Sprintf("r%d", rev)
			cfg := s.cfg.Load()
			for _, sc := range cfg.ScrapeConfigs {
				for _, stc := range sc.StaticConfigs {
					if r.Float64() >= p {
						continue
					}
					stc.Labels["revision"] = revStr
				}
			}
			s.cfg.Store(cfg)
		}
	}
}

func (s *Bench) HandleNodeExporter(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	v := s.node.Load()
	if v == nil {
		_, _ = w.Write([]byte("# no data"))
		return
	}
	_, _ = w.Write(*v)
}

type storageInfo struct {
	Start            time.Time
	Latest           time.Time
	Delta            time.Duration
	Rows             int
	DiskSizeBytes    int
	PrimaryKeySize   int
	CompressedSize   int
	UncompressedSize int
	CompressRatio    float64
	PointsPerSecond  int
}

func (s *Bench) fetchClickhouseStats(ctx context.Context) error {
	client, err := ch.Dial(ctx, ch.Options{
		Address: s.clickhouseAddr,
	})
	if err != nil {
		return errors.Wrap(err, "dial")
	}
	defer func() {
		_ = client.Close()
	}()
	var info storageInfo
	{
		var start proto.ColDateTime64
		if err := client.Do(ctx, ch.Query{
			Body: `SELECT min(timestamp) as start FROM metrics_points`,
			Result: proto.Results{
				{Name: "start", Data: &start},
			},
		}); err != nil {
			return errors.Wrap(err, "query")
		}
		info.Start = start.Row(0)
	}
	{
		var (
			seconds proto.ColDateTime
			delta   proto.ColInt32
			points  proto.ColUInt64
		)
		// Select aggregated points per second for last 100 seconds.
		if err := client.Do(ctx, ch.Query{
			Body: `SELECT toDateTime(toStartOfSecond(timestamp)) as ts, (now() - toDateTime(ts)) as delta, COUNT() as total
FROM metrics_points
WHERE timestamp > (now() - toIntervalSecond(100))
GROUP BY ts
HAVING total > 0
ORDER BY ts DESC
LIMIT 100`,
			Result: proto.Results{
				{Name: "ts", Data: &seconds},
				{Name: "delta", Data: &delta},
				{Name: "total", Data: &points},
			},
		}); err != nil {
			return errors.Wrap(err, "query")
		}
		if len(points) > 0 {
			info.PointsPerSecond = int(slices.Max(points))
			info.Delta = time.Duration(slices.Min(delta)) * time.Second
		} else {
			info.PointsPerSecond = 0
			info.Delta = -1
		}
		for i := 0; i < points.Rows(); i++ {
			ts := seconds.Row(i)
			v := points.Row(i)
			if v == 0 {
				continue
			}
			if ts.After(info.Latest) {
				info.Latest = ts
			}
		}
	}
	{
		var (
			table            proto.ColStr
			rows             proto.ColUInt64
			diskSize         proto.ColUInt64
			primaryKeysSize  proto.ColUInt64
			compressedSize   proto.ColUInt64
			uncompressedSize proto.ColUInt64
			compressRatio    proto.ColFloat64
		)
		const query = `select parts.*,
       columns.compressed_size,
       columns.uncompressed_size,
       columns.ratio
from (
         select table,
                sum(data_uncompressed_bytes)    AS uncompressed_size,
                sum(data_compressed_bytes)      AS compressed_size,
                round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
         from system.columns
         where database = 'default'
         group by table
         ) columns
         right join (
    select table,
           sum(rows)                        as rows,
           sum(bytes)                       as disk_size,
           sum(primary_key_bytes_in_memory) as primary_keys_size
    from system.parts
    where active and database = 'default'
    group by database, table
    ) parts on columns.table = parts.table
order by parts.disk_size desc`
		if err := client.Do(ctx, ch.Query{
			Body: query,
			Result: proto.Results{
				{Name: "parts.table", Data: &table},
				{Name: "rows", Data: &rows},
				{Name: "disk_size", Data: &diskSize},
				{Name: "primary_keys_size", Data: &primaryKeysSize},
				{Name: "compressed_size", Data: &compressedSize},
				{Name: "uncompressed_size", Data: &uncompressedSize},
				{Name: "ratio", Data: &compressRatio},
			},
		}); err != nil {
			return errors.Wrap(err, "query")
		}
		for i := 0; i < len(rows); i++ {
			switch table.Row(i) {
			case "metrics_points":
				info.Rows = int(rows.Row(i))
				info.DiskSizeBytes = int(diskSize.Row(i))
				info.PrimaryKeySize = int(primaryKeysSize.Row(i))
				info.CompressedSize = int(compressedSize.Row(i))
				info.UncompressedSize = int(uncompressedSize.Row(i))
				info.CompressRatio = compressRatio.Row(i)
			default:
				continue
			}
		}
	}

	s.storageInfo.Store(&info)

	return nil
}

func (s *Bench) RunClickhouseReporter(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.fetchClickhouseStats(ctx); err != nil {
				zctx.From(ctx).Error("cannot fetch clickhouse stats", zap.Error(err))
			}
		}
	}
}

func compactBytes(v int) string {
	s := humanize.Bytes(uint64(v))
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func (s *Bench) RunReporter(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()
	var lastHash string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			info := s.metricsInfo.Load()
			if info == nil {
				zctx.From(ctx).Info("no metrics info")
				continue
			}
			{
				if lastHash == info.Hash {
					zctx.From(ctx).Warn("Last hash is the same, node exporter stalled or not working?", zap.String("hash", info.Hash))
				}
				lastHash = info.Hash
			}
			var b strings.Builder
			b.WriteString(fmt.Sprintf("m=%s", fmtInt(info.Count*s.targetsCount)))
			if v := s.storageInfo.Load(); v != nil && s.clickhouseAddr != "" {
				now := time.Now()
				b.WriteString(" ")
				b.WriteString(fmt.Sprintf("uptime=%s", now.Sub(v.Start).Round(time.Second)))
				b.WriteString(" ")
				if v.Delta != -1 {
					b.WriteString(fmt.Sprintf("lag=%s", v.Delta.Round(time.Second)))
				} else {
					b.WriteString("lag=N/A")
				}
				b.WriteString(" ")
				b.WriteString(fmt.Sprintf("pps=%s", fmtInt(v.PointsPerSecond)))
				b.WriteString(" ")
				b.WriteString(fmt.Sprintf("rows=%s", fmtInt(v.Rows)))
				b.WriteString(" ")
				b.WriteString(
					fmt.Sprintf("%s -> %s (%.0fx)",
						compactBytes(v.CompressedSize),
						compactBytes(v.UncompressedSize),
						v.CompressRatio,
					),
				)
				bytesPerPoint := float64(v.CompressedSize) / float64(v.Rows)
				b.WriteString(" ")
				b.WriteString(fmt.Sprintf("%.1f b/point", bytesPerPoint))

				type metric struct {
					Name    string
					Seconds int
				}
				for _, m := range []metric{
					{Name: "d", Seconds: 60 * 60 * 24},
					{Name: "w", Seconds: 60 * 60 * 24 * 7},
					{Name: "m", Seconds: 60 * 60 * 24 * 30},
				} {
					rowsPerDay := v.PointsPerSecond * m.Seconds
					dataPerDay := float64(rowsPerDay) / float64(v.Rows) * float64(v.CompressedSize)
					b.WriteString(" ")
					b.WriteString(fmt.Sprintf("%s/%s", compactBytes(int(dataPerDay)), m.Name))
				}
			}
			fmt.Println(b.String())
		}
	}
}

func (s *Bench) RunNodeExporter(ctx context.Context) error {
	args := []string{
		"--no-collector.wifi",
		"--no-collector.hwmon",
		"--no-collector.time",
		"--no-collector.timex",
		"--no-collector.arp",
		"--no-collector.netdev",
		"--no-collector.netstat",
		"--collector.processes",
		"--web.max-requests=40",
		"--web.listen-address=" + s.nodeExporterAddr,
		"--log.format=json",
	}
	// #nosec G204
	cmd := exec.CommandContext(ctx, "node_exporter", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (s *Bench) RunAgent(ctx context.Context) error {
	if len(s.targets) != 1 {
		return errors.New("expected one target")
	}
	arg := []string{
		"--httpListenAddr=" + s.agentAddr,
		"--loggerFormat=json",
		"--remoteWrite.showURL",
		"--promscrape.config=http://" + s.addr + "/config",
		"--remoteWrite.url=" + s.targets[0],
		"--remoteWrite.forceVMProto",
	}
	// #nosec G204
	cmd := exec.CommandContext(ctx, "vmagent", arg...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (s *Bench) RunPrometheus(ctx context.Context, dir string) error {
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	prometheusConfigFile := filepath.Join(dir, "prometheus.yml")
	if err := os.WriteFile(prometheusConfigFile, s.cfg.Load().marshalYAML(), 0o600); err != nil {
		return err
	}
	// #nosec G204
	cmd := exec.CommandContext(ctx, "prometheus",
		"--config.file="+filepath.Join(dir, "prometheus.yml"),
		"--web.listen-address="+s.agentAddr,
		"--enable-feature=agent",
		"--enable-feature=new-service-discovery-manager",
		"--log.format=json",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = dir
	go func() {
		// Periodically update the config.
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := os.WriteFile(prometheusConfigFile, s.cfg.Load().marshalYAML(), 0o600); err != nil {
					zctx.From(ctx).Error("cannot update prometheus config", zap.Error(err))
				}
				if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
					zctx.From(ctx).Error("cannot send SIGHUP to prometheus", zap.Error(err))
				}
			}
		}
	}()
	return cmd.Run()
}

func (s *Bench) prometheusConfig() *config {
	cfg := newConfig(s.targetsCount, s.scrapeInterval, s.addr)
	if !s.useVictoria {
		var remotes []*remoteWriteConfig
		for i, target := range s.targets {
			remotes = append(remotes, &remoteWriteConfig{
				URL:  target,
				Name: fmt.Sprintf("target-%d", i),
				Metadata: &remoteWriteMetadataConfig{
					Send:         true,
					SendInterval: time.Second,
				},
			})
		}
		cfg.RemoteWrites = remotes
	}
	return cfg
}

func (s *Bench) parseTargets(args []string) error {
	for _, arg := range args {
		u, err := url.Parse(arg)
		if err != nil {
			return errors.Wrap(err, "parse url")
		}
		s.targets = append(s.targets, u.String())
	}
	if len(s.targets) == 0 {
		return errors.New("no targets")
	}
	return nil
}

func (s *Bench) waitForTarget(ctx context.Context, target string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

	e := backoff.NewExponentialBackOff()
	if err := backoff.RetryNotify(func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
		if err != nil {
			return err
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			_ = res.Body.Close()
		}()
		zctx.From(ctx).Info("Target ready", zap.String("target", target))
		return nil
	}, backoff.WithContext(e, ctx), func(err error, duration time.Duration) {
		zctx.From(ctx).Warn("cannot fetch target", zap.Error(err), zap.Duration("duration", duration))
	}); err != nil {
		return errors.Wrapf(err, "wait for target %q", target)
	}
	return nil
}

func (s *Bench) run(ctx context.Context) error {
	s.cfg.Store(s.prometheusConfig())

	// First, wait for target to be available.
	for _, target := range s.targets {
		if err := s.waitForTarget(ctx, target); err != nil {
			return err
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.ProgressConfig(ctx)
	})
	if s.clickhouseAddr != "" {
		g.Go(func() error {
			return s.RunClickhouseReporter(ctx)
		})
	}
	if s.queryAddr != "" {
		g.Go(func() error {
			return s.RunQueryReporter(ctx)
		})
	}
	g.Go(func() error {
		return s.RunReporter(ctx)
	})
	if s.useVictoria {
		g.Go(func() error {
			return s.RunAgent(ctx)
		})
	} else {
		prometheusDir, err := os.MkdirTemp("", "prometheus")
		if err != nil {
			return err
		}
		g.Go(func() error {
			return s.RunPrometheus(ctx, prometheusDir)
		})
	}
	g.Go(func() error {
		return s.RunNodeExporter(ctx)
	})
	g.Go(func() error {
		s.PollNodeExporter(ctx)
		return nil
	})
	g.Go(func() error {
		mux := http.NewServeMux()
		mux.HandleFunc("/node", s.HandleNodeExporter)
		mux.HandleFunc("/config", s.HandleConfig)
		srv := &http.Server{
			Addr:              s.addr,
			Handler:           mux,
			ReadHeaderTimeout: time.Second * 5,
		}
		go func() {
			<-ctx.Done()
			_ = srv.Close()
		}()
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	return g.Wait()
}

func prometheusTimestamp(t time.Time) promapi.PrometheusTimestamp {
	return promapi.PrometheusTimestamp(t.Format(time.RFC3339Nano))
}

type cpuQueryStats struct {
	Count    int
	Duration time.Duration
	Latest   time.Time
}

func (s *Bench) runQueryCPU(ctx context.Context, client promapi.Invoker) (*cpuQueryStats, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	const q = `count(count(node_cpu_seconds_total{instance="host-0",job="node_exporter"}) by (cpu))`
	start := time.Now()
	res, err := client.PostQueryRange(ctx, &promapi.QueryRangeForm{
		Query: q,
		Start: prometheusTimestamp(start.Add(-time.Minute)),
		End:   prometheusTimestamp(start),
		Step:  "1s",
	})
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}
	stats := &cpuQueryStats{
		Count:    -1,
		Duration: time.Since(start),
	}
	matrix, ok := res.Data.GetMatrix()
	if !ok {
		return nil, errors.Errorf("unexpected response type %q", res.Data.Type)
	}
	for _, result := range matrix.Result {
		for _, point := range result.Values {
			if stats.Count == -1 {
				stats.Count = int(point.V)
			}
			if point.V == 0 || stats.Count != int(point.V) {
				return nil, errors.Errorf("unexpected value %f", point.V)
			}
			ts := time.Unix(int64(point.T), 0)
			if ts.After(stats.Latest) {
				stats.Latest = ts
			}
		}
	}
	return stats, nil
}

func (s *Bench) RunQueryReporter(ctx context.Context) error {
	client, err := promapi.NewClient(s.queryAddr)
	if err != nil {
		return errors.Wrap(err, "create prometheus client")
	}

	ticker := time.NewTicker(s.queryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			stats, err := s.runQueryCPU(ctx, client)
			var b strings.Builder
			b.WriteString("query: ")
			if err != nil {
				b.WriteString(err.Error())
				continue
			}
			if stats.Count > 0 {
				b.WriteString(fmt.Sprintf("cpu=%s", fmtInt(stats.Count)))
			} else {
				b.WriteString("cpu=N/A")
			}
			b.WriteString(" ")
			b.WriteString(fmt.Sprintf("d=%s", stats.Duration.Round(time.Millisecond)))
			if !stats.Latest.IsZero() {
				b.WriteString(" ")
				b.WriteString(fmt.Sprintf("lag=%s", time.Since(stats.Latest).Round(time.Millisecond)))
			}
			fmt.Println(b.String())
		}
	}
}

func newBenchCommand() *cobra.Command {
	var b Bench
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Start remote write benchmark",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := b.parseTargets(args); err != nil {
				return err
			}
			return b.run(cmd.Context())
		},
	}
	f := cmd.Flags()
	f.StringVar(&b.nodeExporterAddr, "nodeExporterAddr", "127.0.0.1:9301", "address for node exporter to listen")
	f.StringVar(&b.addr, "addr", "127.0.0.1:8428", "address to listen")
	f.StringVar(&b.agentAddr, "agentAddr", "127.0.0.1:8429", "address for vmagent to listen")
	f.IntVar(&b.targetsCount, "targetsCount", 100, "The number of scrape targets to return from -httpListenAddr. Each target has the same address defined by -targetAddr")
	f.DurationVar(&b.scrapeInterval, "scrapeInterval", time.Second*5, "The scrape_interval to set at the scrape config returned from -httpListenAddr")
	f.DurationVar(&b.pollExporterInterval, "pollExporterInterval", time.Second, "Interval to poll the node exporter filling up cache")
	f.DurationVar(&b.scrapeConfigUpdateInterval, "scrapeConfigUpdateInterval", time.Minute*10, "The -scrapeConfigUpdatePercent scrape targets are updated in the scrape config returned from -httpListenAddr every -scrapeConfigUpdateInterval")
	f.Float64Var(&b.scrapeConfigUpdatePercent, "scrapeConfigUpdatePercent", 1, "The -scrapeConfigUpdatePercent scrape targets are updated in the scrape config returned from -httpListenAddr ever -scrapeConfigUpdateInterval")
	f.StringVar(&b.clickhouseAddr, "clickhouseAddr", "", "clickhouse tcp protocol addr to get actual stats from")
	f.StringVar(&b.queryAddr, "queryAddr", "", "addr to query PromQL from")
	f.DurationVar(&b.queryInterval, "queryInterval", time.Second*5, "interval to query PromQL")
	f.BoolVar(&b.useVictoria, "useVictoria", true, "use vmagent instead of prometheus")
	return cmd
}
