package pgx_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aureliusnguyen/pandora/backend/internal/pkg/pgx"
)

func TestNewPoolRejectsBadDSN(t *testing.T) {
	_, err := pgx.NewPool(context.Background(), "this is not a dsn")
	if err == nil || !strings.Contains(err.Error(), "pgx: parse dsn") {
		t.Fatalf("NewPool(bad) = %v, want parse error", err)
	}
}

func TestNewPoolFailsFastWhenUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	_, err := pgx.NewPool(ctx, "postgres://u:secret@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err == nil || !strings.Contains(err.Error(), "pgx: ping") {
		t.Fatalf("NewPool(unreachable) = %v, want ping error", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("error leaks password: %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("NewPool did not respect ctx")
	}
}
