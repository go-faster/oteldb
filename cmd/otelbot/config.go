package main

import (
	"os"
	"path/filepath"

	"github.com/go-faster/yaml"
)

// HealthCheckConfig is health check config.
type HealthCheckConfig struct {
	Bind string `json:"bind" yaml:"bind"`
}

func (cfg *HealthCheckConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":13133"
	}
}

type APIConfig struct {
	Bind string `json:"bind" yaml:"bind"`
}

func (cfg *APIConfig) setDefaults() {
	if cfg.Bind == "" {
		cfg.Bind = ":8080"
	}
}

// Config is the oteldb config.
type Config struct {
	HealthCheck HealthCheckConfig `json:"health_check" yaml:"health_check"`
	API         APIConfig         `json:"api" yaml:"api"`
}

func (cfg *Config) setDefaults() {
	cfg.HealthCheck.setDefaults()
	cfg.API.setDefaults()
}

func loadConfig(name string) (cfg Config, _ error) {
	if name == "" {
		name = "otelbot.yml"
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
