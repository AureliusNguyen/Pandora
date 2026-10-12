// Package logx configures structured JSON logging with log/slog.
package logx

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// ParseLevel maps LOG_LEVEL to a slog level. Case-insensitive; blank means
// info; "warning" is accepted as "warn".
func ParseLevel(s string) (slog.Level, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "":
		s = "info"
	case "warning":
		s = "warn"
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("logx: invalid LOG_LEVEL %q (want debug|info|warn|error)", s)
	}
	return l, nil
}

// New returns a JSON logger writing to w, tagged with service.
func New(w io.Writer, service, level string) (*slog.Logger, error) {
	l, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: l})
	return slog.New(h).With(slog.String("service", service)), nil
}

// Setup builds a stdout logger and installs it as the slog default.
func Setup(service, level string) (*slog.Logger, error) {
	lg, err := New(os.Stdout, service, level)
	if err != nil {
		return nil, err
	}
	slog.SetDefault(lg)
	return lg, nil
}
