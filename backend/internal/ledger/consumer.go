package ledger

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

const eventsTopic = "engine.events"

var (
	batchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pandora", Subsystem: "ledger", Name: "batch_size", Buckets: prometheus.ExponentialBuckets(1, 2, 11),
		Help: "Records per applied batch.",
	})
	applySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "pandora", Subsystem: "ledger", Name: "apply_seconds", Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		Help: "Duration of a successful batch transaction.",
	})
	decodeErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "pandora", Subsystem: "ledger", Name: "decode_errors_total",
		Help: "Records that could not be decoded as enginev1.Event.",
	})
	batchRetries = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "pandora", Subsystem: "ledger", Name: "batch_retries_total",
		Help: "Failed batch transactions that are being retried.",
	})
)

// Decode unmarshals records. An undecodable record is logged, counted and
// kept as an Envelope with a nil Event: the applier skips it but still
// advances the stored consumer offset past it, so it cannot wedge the
// partition.
func Decode(recs []*kgo.Record, log *slog.Logger) []Envelope {
	out := make([]Envelope, 0, len(recs))
	for _, r := range recs {
		env := Envelope{KafkaPartition: r.Partition, Offset: r.Offset}
		ev := &enginev1.Event{}
		if err := proto.Unmarshal(r.Value, ev); err != nil {
			decodeErrors.Inc()
			log.Error("undecodable event", "partition", r.Partition, "offset", r.Offset, "err", err)
		} else {
			env.Event = ev
		}
		out = append(out, env)
	}
	return out
}

// NewKafkaClient returns a manual-commit group consumer of engine.events
// that starts from the earliest offset when the group has none.
func NewKafkaClient(brokers []string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(ConsumerName),
		kgo.ConsumeTopics(eventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
}

// Consumer feeds polled engine.events batches to an Applier.
type Consumer struct {
	cl  *kgo.Client
	app *Applier
	log *slog.Logger
}

// NewConsumer returns a Consumer reading from cl and applying through app.
func NewConsumer(cl *kgo.Client, app *Applier, log *slog.Logger) *Consumer {
	return &Consumer{cl: cl, app: app, log: log}
}

// offsetRanges formats the per-partition offset span of recs, e.g. "0:7-8".
func offsetRanges(recs []*kgo.Record) string {
	lo, hi := map[int32]int64{}, map[int32]int64{}
	for _, r := range recs {
		if v, ok := lo[r.Partition]; !ok || r.Offset < v {
			lo[r.Partition] = r.Offset
		}
		hi[r.Partition] = max(hi[r.Partition], r.Offset)
	}
	parts := make([]string, 0, len(lo))
	for _, p := range slices.Sorted(maps.Keys(lo)) {
		parts = append(parts, fmt.Sprintf("%d:%d-%d", p, lo[p], hi[p]))
	}
	return strings.Join(parts, ",")
}

// Run polls up to 1000 records, applies them in one PG txn, then commits
// Kafka offsets. It returns only when ctx ends.
//
// Stall, not corrupt: a batch that fails in Postgres for any reason other
// than decoding (malformed events are skipped by the applier) is retried
// forever with backoff from 100 ms doubling to 5 s, logging the partition
// and offset range and incrementing pandora_ledger_batch_retries_total, with
// rebalances blocked meanwhile. Money events are never skipped; isolating a
// poison record into a dead_events table is an M3/M4 follow-up.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollRecords(ctx, 1000)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(topic string, p int32, err error) {
			c.log.Warn("fetch error", "topic", topic, "partition", p, "err", err)
		})
		recs := fetches.Records()
		if len(recs) == 0 {
			c.cl.AllowRebalance()
			continue
		}
		envs := Decode(recs, c.log)
		batchSize.Observe(float64(len(envs)))
		backoff := 100 * time.Millisecond
		for {
			start := time.Now()
			err := c.app.ApplyBatch(ctx, envs)
			if err == nil {
				applySeconds.Observe(time.Since(start).Seconds())
				break
			}
			batchRetries.Inc()
			c.log.Error("apply batch failed, retrying", "offsets", offsetRanges(recs), "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 5*time.Second)
		}
		if err := c.cl.CommitRecords(ctx, recs...); err != nil {
			// Safe: PG already holds the batch; a redelivery is deduped by event_log.
			c.log.Warn("kafka commit failed", "err", err)
		}
		c.cl.AllowRebalance()
	}
}
