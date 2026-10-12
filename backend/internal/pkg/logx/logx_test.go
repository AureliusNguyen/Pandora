package logx_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/aureliusnguyen/pandora/backend/internal/pkg/logx"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":        slog.LevelInfo,
		"info":    slog.LevelInfo,
		"INFO":    slog.LevelInfo,
		" debug ": slog.LevelDebug, //nolint:gocritic // whitespace is intentional: ParseLevel must trim it
		"warn":    slog.LevelWarn,
		"Warning": slog.LevelWarn,
		"error":   slog.LevelError,
	}
	for in, want := range cases {
		got, err := logx.ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := logx.ParseLevel("loud"); err == nil {
		t.Error("ParseLevel(loud) want error")
	}
}

func TestNewWritesJSONWithServiceAndRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	lg, err := logx.New(&buf, "matcher", "warn")
	if err != nil {
		t.Fatal(err)
	}
	lg.Info("hidden")
	lg.Warn("shown", "k", 1)
	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("want exactly one JSON line, got %q: %v", buf.String(), err)
	}
	if rec["msg"] != "shown" || rec["service"] != "matcher" || rec["level"] != "WARN" {
		t.Fatalf("unexpected record %v", rec)
	}
}
