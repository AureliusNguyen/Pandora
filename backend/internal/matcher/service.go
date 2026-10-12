package matcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/ids"
)

const (
	// DemoMarketID is the market created on start when DEMO_MARKET is set.
	DemoMarketID = "mkt_demo"
	// DemoMarketTitle is the demo market's title.
	DemoMarketTitle = "Demo: BTC above strike"
)

// Options wires a Service. NewID assigns order ids when the request has
// none; nil means ids.New.
type Options struct {
	Partition  uint32
	WAL        WAL
	Metrics    *Metrics
	Log        *slog.Logger
	NewID      func() string // optional, default ids.New
	DemoMarket bool
	Now        func() int64        // optional
	Fatal      func(string, error) // optional (tests)
}

// Service is the MatcherService implementation over one partition runner
// (M1: partition 0 only; ENGINE_PARTITIONS is fixed at 1).
type Service struct {
	o      Options
	runner *Runner
	ready  atomic.Bool
	cancel context.CancelFunc
}

// NewService builds the service and its runner; Start replays and runs it.
func NewService(o Options) *Service {
	if o.NewID == nil {
		o.NewID = ids.New
	}
	r := NewRunner(o.Partition, engine.New(o.Partition), o.WAL, o.Metrics, o.Log)
	if o.Now != nil {
		r.Now = o.Now
	}
	if o.Fatal != nil {
		r.Fatal = o.Fatal
	}
	return &Service{o: o, runner: r}
}

// Start replays the WAL into the engine, starts the runner, creates the
// demo market when configured and absent, then marks the service ready.
// A WAL gap returns an error wrapping ErrGap and the service stays not
// ready. ctx bounds the replay and the demo creation only: the runner is
// detached from it so that only Stop ends the loop.
func (s *Service) Start(ctx context.Context) error {
	n, err := ReplayInto(ctx, s.o.WAL, s.o.Partition, s.runner.eng, s.o.Log)
	if err != nil {
		return fmt.Errorf("replay partition %d: %w", s.o.Partition, err)
	}
	s.o.Log.Info("replay complete", "partition", s.o.Partition, "applied", n, "last_seq", s.runner.eng.LastSeq())
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel
	s.runner.Start(rctx)
	if s.o.DemoMarket && !s.runner.HasMarket(DemoMarketID) {
		cmd := &enginev1.Command{Body: &enginev1.Command_CreateMarket{CreateMarket: &enginev1.CreateMarket{
			MarketId: DemoMarketID, Title: DemoMarketTitle, OutcomeLabels: []string{"YES", "NO"},
		}}}
		res, err := s.runner.Submit(ctx, cmd)
		if err != nil {
			cancel()
			return fmt.Errorf("create demo market: %w", err)
		}
		s.o.Log.Info("demo market created", "seq", res.Seq)
	}
	s.ready.Store(true)
	return nil
}

// Ready reports whether Start completed and Stop has not been called.
func (s *Service) Ready() bool { return s.ready.Load() }

// Runner exposes the partition runner (tests and main).
func (s *Service) Runner() *Runner { return s.runner }

// Stop marks not-ready, stops the runner and waits for it. It is safe
// after a failed Start.
func (s *Service) Stop() {
	s.ready.Store(false)
	if s.cancel != nil {
		s.cancel()
		<-s.runner.Done()
	}
}
