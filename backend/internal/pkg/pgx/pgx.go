// Package pgx builds a pgxpool.Pool from a DSN and verifies connectivity.
package pgx

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PingTimeout bounds the startup connectivity check.
var PingTimeout = 5 * time.Second

// NewPool parses dsn, creates a pool and pings it once.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx: parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgx: new pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, PingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgx: ping %s:%d: %w", cfg.ConnConfig.Host, cfg.ConnConfig.Port, err)
	}
	return pool, nil
}
