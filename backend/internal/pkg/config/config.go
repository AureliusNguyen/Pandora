// Package config loads per-service configuration from environment variables.
// Defaults match the M1 contracts table.
package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

// DefaultPGDSN is the local docker-compose Postgres.
const DefaultPGDSN = "postgres://pandora:pandora@localhost:5432/pandora?sslmode=disable"

// Common holds settings every service reads.
type Common struct {
	MetricsAddr  string `env:"METRICS_ADDR" envDefault:":9090"`
	LogLevel     string `env:"LOG_LEVEL" envDefault:"info"`
	OTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
}

// Kafka holds broker settings for services that talk to Redpanda.
type Kafka struct {
	Brokers []string `env:"KAFKA_BROKERS" envDefault:"localhost:19092" envSeparator:","`
}

// Gateway is the REST edge configuration. CORSOrigins is the browser
// origin allowlist (local `pnpm dev` on :3000, the kind web NodePort :30300).
type Gateway struct {
	Common
	HTTPAddr    string   `env:"HTTP_ADDR" envDefault:":8080"`
	MatcherAddr string   `env:"MATCHER_ADDR" envDefault:"http://localhost:9001"`
	CORSOrigins []string `env:"CORS_ORIGINS" envDefault:"http://localhost:3000,http://localhost:30300" envSeparator:","`
}

// Matcher is the sequencer + engine configuration.
type Matcher struct {
	Common
	Kafka
	HTTPAddr         string `env:"HTTP_ADDR" envDefault:":9001"`
	EnginePartitions uint32 `env:"ENGINE_PARTITIONS" envDefault:"1"`
	DemoMarket       bool   `env:"DEMO_MARKET" envDefault:"true"`
}

// Ledger is the money consumer configuration.
type Ledger struct {
	Common
	Kafka
	HTTPAddr string `env:"HTTP_ADDR" envDefault:":9002"`
	PGDSN    string `env:"PG_DSN" envDefault:"postgres://pandora:pandora@localhost:5432/pandora?sslmode=disable"`
}

// Fanout is the WebSocket edge configuration.
type Fanout struct {
	Common
	Kafka
	HTTPAddr    string `env:"HTTP_ADDR" envDefault:":8081"`
	MatcherAddr string `env:"MATCHER_ADDR" envDefault:"http://localhost:9001"`
}

// Load parses T from the process environment.
func Load[T any]() (T, error) {
	var cfg T
	if err := env.Parse(&cfg); err != nil {
		return cfg, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

// LoadFrom parses T from the given environment only (used by tests).
func LoadFrom[T any](environ map[string]string) (T, error) {
	var cfg T
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: environ}); err != nil {
		return cfg, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}
