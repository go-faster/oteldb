package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/go-faster/yaml"
)

func loadConfig(name string) (cfg Config, _ error) {
	defer func() {
		// Environment variable has higher precedence.
		if dsn := os.Getenv("CH_DSN"); dsn != "" {
			cfg.DSN = dsn
		}
	}()

	if name == "" {
		name = "oteldb.yml"
		if _, err := os.Stat(name); err != nil {
			return cfg, nil
		}
	}

	data, err := os.ReadFile(filepath.Clean(name))
	if err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// Config is the oteldb config.
type Config struct {
	DSN         string            `json:"dsn" yaml:"dsn"`
	TTL         time.Duration     `json:"ttl" yaml:"ttl"`
	Tempo       TempoConfig       `json:"tempo" yaml:"tempo"`
	Prometheus  PrometheusConfig  `json:"prometheus" yaml:"prometheus"`
	LokiConfig  LokiConfig        `json:"loki" yaml:"loki"`
	HealthCheck HealthCheckConfig `json:"health_check" yaml:"health_check"`

	// Collector is an otelcol config.
	Collector map[string]any `json:"otelcol" yaml:"otelcol"`
}

func (cfg *Config) setDefaults() {
	if cfg.DSN == "" {
		cfg.DSN = "clickhouse://localhost:9000"
	}
	if cfg.Collector == nil {
		cfg.Collector = map[string]any{
			"receivers": map[string]any{
				"otlp": map[string]any{
					"protocols": map[string]any{
						"grpc": nil,
						"http": nil,
					},
				},
				"prometheusremotewrite": map[string]any{},
			},
			"exporters": map[string]any{
				"oteldbexporter": map[string]any{
					"dsn": cfg.DSN,
				},
			},
			"service": map[string]any{
				"pipelines": map[string]any{
					"traces": map[string]any{
						"receivers": []string{"otlp"},
						"exporters": []string{"oteldbexporter"},
					},
					"metrics": map[string]any{
						"receivers": []string{"otlp", "prometheusremotewrite"},
						"exporters": []string{"oteldbexporter"},
					},
					"logs": map[string]any{
						"receivers": []string{"otlp"},
						"exporters": []string{"oteldbexporter"},
					},
				},
			},
		}
	}
}

// TempoConfig is Tempo API config.
type TempoConfig struct {
	Bind string `json:"bind" yaml:"bind"`
}

func (cfg *TempoConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":3200"
	}
}

// PrometheusConfig is Prometheus API config.
type PrometheusConfig struct {
	Bind                 string        `json:"bind" yaml:"bind"`
	MaxSamples           int           `json:"max_samples" yaml:"max_samples"`
	Timeout              time.Duration `json:"timeout" yaml:"timeout"`
	LookbackDelta        time.Duration `json:"lookback_delta" yaml:"lookback_delta"`
	EnableAtModifier     bool          `json:"enable_at_modifier" yaml:"enable_at_modifier"`
	EnableNegativeOffset *bool         `json:"enable_negative_offset" yaml:"enable_negative_offset"`
	EnablePerStepStats   bool          `json:"enable_per_step_stats" yaml:"enable_per_step_stats"`
}

func (cfg *PrometheusConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":9090"
	}
	if cfg.MaxSamples == 0 {
		cfg.MaxSamples = 1_000_000
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Minute
	}
	setBool := func(p **bool, defaultValue bool) {
		if *p == nil {
			*p = &defaultValue
		}
	}
	setBool(&cfg.EnableNegativeOffset, true)
}

// LokiConfig is Loki API config.
type LokiConfig struct {
	Bind          string        `json:"bind" yaml:"bind"`
	LookbackDelta time.Duration `json:"lookback_delta" yaml:"lookback_delta"`
}

func (cfg *LokiConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":3100"
	}
}

// HealthCheckConfig is health check config.
type HealthCheckConfig struct {
	Bind string `json:"bind" yaml:"bind"`
}

func (cfg *HealthCheckConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":13133"
	}
}
