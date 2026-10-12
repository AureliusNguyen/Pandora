package config_test

import (
	"slices"
	"testing"
	"time"

	"github.com/aureliusnguyen/pandora/backend/internal/pkg/config"
)

func TestDefaults(t *testing.T) {
	empty := map[string]string{}

	m, err := config.LoadFrom[config.Matcher](empty)
	if err != nil {
		t.Fatal(err)
	}
	if m.HTTPAddr != ":9001" || m.MetricsAddr != ":9090" || m.LogLevel != "info" || m.OTLPEndpoint != "" {
		t.Fatalf("matcher common defaults wrong: %+v", m)
	}
	if !slices.Equal(m.Brokers, []string{"localhost:19092"}) || m.EnginePartitions != 1 || !m.DemoMarket || m.ShutdownGrace != 10*time.Second {
		t.Fatalf("matcher defaults wrong: %+v", m)
	}

	g, err := config.LoadFrom[config.Gateway](empty)
	if err != nil {
		t.Fatal(err)
	}
	if g.HTTPAddr != ":8080" || g.MatcherAddr != "http://localhost:9001" ||
		!slices.Equal(g.CORSOrigins, []string{"http://localhost:3000", "http://localhost:30300"}) {
		t.Fatalf("gateway defaults wrong: %+v", g)
	}

	l, err := config.LoadFrom[config.Ledger](empty)
	if err != nil {
		t.Fatal(err)
	}
	if l.HTTPAddr != ":9002" || l.PGDSN != config.DefaultPGDSN {
		t.Fatalf("ledger defaults wrong: %+v", l)
	}
	if config.DefaultPGDSN != "postgres://pandora:pandora@localhost:5432/pandora?sslmode=disable" {
		t.Fatalf("DefaultPGDSN wrong: %q", config.DefaultPGDSN)
	}

	f, err := config.LoadFrom[config.Fanout](empty)
	if err != nil {
		t.Fatal(err)
	}
	if f.HTTPAddr != ":8081" || f.MatcherAddr != "http://localhost:9001" ||
		!slices.Equal(f.WSOrigins, []string{"localhost:*", "127.0.0.1:*"}) {
		t.Fatalf("fanout defaults wrong: %+v", f)
	}
}

func TestOverrides(t *testing.T) {
	m, err := config.LoadFrom[config.Matcher](map[string]string{
		"HTTP_ADDR":                   ":7000",
		"KAFKA_BROKERS":               "k1:9092,k2:9092",
		"DEMO_MARKET":                 "false",
		"ENGINE_PARTITIONS":           "4",
		"LOG_LEVEL":                   "debug",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
		"SHUTDOWN_GRACE":              "1m30s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if m.HTTPAddr != ":7000" || m.DemoMarket || m.EnginePartitions != 4 || m.LogLevel != "debug" ||
		m.OTLPEndpoint != "http://collector:4318" || m.ShutdownGrace != 90*time.Second || !slices.Equal(m.Brokers, []string{"k1:9092", "k2:9092"}) {
		t.Fatalf("overrides not applied: %+v", m)
	}
	g, err := config.LoadFrom[config.Gateway](map[string]string{"CORS_ORIGINS": "http://a.test,http://b.test"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(g.CORSOrigins, []string{"http://a.test", "http://b.test"}) {
		t.Fatalf("CORS_ORIGINS override not applied: %+v", g)
	}
	f, err := config.LoadFrom[config.Fanout](map[string]string{
		"WS_ORIGINS":    " pandora.example.com ,*.pandora.test:*, ",
		"KAFKA_BROKERS": "k1:9092, k2:9092",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.WSOrigins, []string{"pandora.example.com", "*.pandora.test:*"}) ||
		!slices.Equal(f.Brokers, []string{"k1:9092", "k2:9092"}) {
		t.Fatalf("WS_ORIGINS override not applied (or brokers not normalized): %+v", f)
	}
}

func TestListsAreTrimmed(t *testing.T) {
	m, err := config.LoadFrom[config.Matcher](map[string]string{"KAFKA_BROKERS": "k1:9092, k2:9092,"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(m.Brokers, []string{"k1:9092", "k2:9092"}) {
		t.Fatalf("KAFKA_BROKERS not normalized: %q", m.Brokers)
	}
	g, err := config.LoadFrom[config.Gateway](map[string]string{"CORS_ORIGINS": " http://a.test ,http://b.test, "})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(g.CORSOrigins, []string{"http://a.test", "http://b.test"}) {
		t.Fatalf("CORS_ORIGINS not normalized: %q", g.CORSOrigins)
	}
}

func TestInvalidValueIsError(t *testing.T) {
	if _, err := config.LoadFrom[config.Matcher](map[string]string{"DEMO_MARKET": "notabool"}); err == nil {
		t.Fatal("want error for DEMO_MARKET=notabool")
	}
	if _, err := config.LoadFrom[config.Matcher](map[string]string{"ENGINE_PARTITIONS": "-1"}); err == nil {
		t.Fatal("want error for ENGINE_PARTITIONS=-1")
	}
	if _, err := config.LoadFrom[config.Matcher](map[string]string{"SHUTDOWN_GRACE": "ten"}); err == nil {
		t.Fatal("want error for SHUTDOWN_GRACE=ten")
	}
}
