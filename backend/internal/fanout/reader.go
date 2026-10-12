package fanout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
	"github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1/matcherv1connect"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
)

const (
	eventsTopic = kafkax.TopicEngineEvents
	// idleFlush is how long a held command waits for more of its events
	// before it is sent; a later tail becomes a second delta (same market_seq).
	idleFlush = 5 * time.Millisecond
	// attemptTimeout bounds one bootstrap attempt so retries keep logging.
	attemptTimeout = 5 * time.Second
)

var malformed = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "pandora", Subsystem: "fanout", Name: "malformed_events_total",
	Help: "engine.events records that could not be decoded and were skipped.",
})

func retry[T any](ctx context.Context, log *slog.Logger, what string, fn func(context.Context) (T, error)) (T, error) {
	for {
		actx, cancel := context.WithTimeout(ctx, attemptTimeout)
		v, err := fn(actx)
		cancel()
		if err == nil {
			return v, nil
		}
		if ctx.Err() != nil {
			var zero T
			return zero, ctx.Err()
		}
		log.Info("waiting", "for", what, "err", err)
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Bootstrap records the end offsets of engine.events FIRST, then loads matcher
// snapshots, then returns a group-less client consuming from those offsets.
// Events between the two steps are replayed and dropped by market_seq, so no
// update is lost. It retries until ctx ends (returning ctx.Err()).
func Bootstrap(ctx context.Context, brokers []string, m matcherv1connect.MatcherServiceClient, hub *Hub, markets []string, log *slog.Logger) (*kgo.Client, error) {
	admCl, err := kafkax.NewClient(brokers)
	if err != nil {
		return nil, err
	}
	defer admCl.Close()
	adm := kadm.NewClient(admCl)
	offsets, err := retry(ctx, log, "engine.events end offsets", func(actx context.Context) (map[int32]kgo.Offset, error) {
		ends, err := adm.ListEndOffsets(actx, eventsTopic)
		if err != nil {
			return nil, err
		}
		if err := ends.Error(); err != nil {
			return nil, err
		}
		out := map[int32]kgo.Offset{}
		ends.Each(func(o kadm.ListedOffset) { out[o.Partition] = kgo.NewOffset().At(o.Offset) })
		if len(out) == 0 {
			return nil, errors.New("topic has no partitions yet")
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	for _, id := range markets {
		resp, err := retry(ctx, log, "matcher GetBook "+id, func(actx context.Context) (*matcherv1.GetBookResponse, error) {
			r, err := m.GetBook(actx, connect.NewRequest(&matcherv1.GetBookRequest{MarketId: id}))
			if err != nil {
				return nil, err
			}
			return r.Msg, nil
		})
		if err != nil {
			return nil, fmt.Errorf("bootstrap %s: %w", id, err)
		}
		hub.LoadSnapshot(resp)
	}
	return kafkax.NewClient(brokers,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{eventsTopic: offsets}),
		kgo.FetchMaxWait(50*time.Millisecond))
}

// Reader feeds engine.events into the Hub.
type Reader struct {
	cl  *kgo.Client
	hub *Hub
	log *slog.Logger
}

// NewReader returns a reader over a client built by Bootstrap.
func NewReader(cl *kgo.Client, hub *Hub, log *slog.Logger) *Reader {
	return &Reader{cl: cl, hub: hub, log: log}
}

// Run feeds the hub one whole command at a time. While a command is held, polls
// use an idleFlush deadline; an empty poll means the command is complete and is
// flushed. If more of that command arrives later, the BookView still applies it
// and the hub sends it as a second delta with the same market_seq. Run returns
// ctx.Err() when ctx ends.
func (r *Reader) Run(ctx context.Context) error {
	var g CommandGrouper
	for {
		pctx, cancel := ctx, context.CancelFunc(func() {})
		if g.Pending() {
			pctx, cancel = context.WithTimeout(ctx, idleFlush)
		}
		fetches := r.cl.PollRecords(pctx, 1000)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				r.log.Warn("fetch error", "topic", topic, "partition", p, "err", err)
			}
		})
		if errors.Is(fetches.Err0(), kgo.ErrClientClosed) {
			return kgo.ErrClientClosed
		}
		recs := fetches.Records()
		if len(recs) == 0 {
			if g.Pending() {
				r.hub.ApplyCommand(g.Flush())
			}
			continue
		}
		for _, rec := range recs {
			ev := &enginev1.Event{}
			if err := kafkax.Decode(rec, ev); err != nil {
				malformed.Inc()
				r.log.Error("undecodable event", "partition", rec.Partition, "offset", rec.Offset, "err", err)
				continue
			}
			if done := g.Add(ev); done != nil {
				r.hub.ApplyCommand(done)
			}
		}
	}
}
