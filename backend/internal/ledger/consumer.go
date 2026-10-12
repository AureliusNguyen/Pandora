package ledger

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
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

// consumerOpts are the group-consumer options for engine.events: manual
// commits, earliest offset when the group has none, and rebalances blocked
// while a polled batch is processed (see Run for why that is not relied on).
func consumerOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(ConsumerName),
		kgo.ConsumeTopics(eventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(100 * time.Millisecond),
	}
}

// Consumer feeds polled engine.events batches to an Applier.
type Consumer struct {
	cl  *kgo.Client
	app *Applier
	log *slog.Logger

	mu   sync.Mutex
	gone map[int32]bool // engine.events partitions lost or revoked since the last poll
}

// NewConsumer creates the pandora-ledger group client for brokers and returns
// a Consumer applying through app. Close releases the client.
func NewConsumer(brokers []string, app *Applier, log *slog.Logger) (*Consumer, error) {
	c := &Consumer{app: app, log: log, gone: map[int32]bool{}}
	opts := append(consumerOpts(brokers), kgo.OnPartitionsLost(c.onGone), kgo.OnPartitionsRevoked(c.onGone))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

// Close leaves the group and closes the Kafka client.
func (c *Consumer) Close() { c.cl.Close() }

// onGone records partitions this member no longer owns (OnPartitionsLost and
// OnPartitionsRevoked), so Run stops retrying their in-flight records instead
// of racing the new owner.
func (c *Consumer) onGone(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range m[eventsTopic] {
		c.gone[p] = true
	}
}

// takeGone returns and clears the partitions recorded by onGone.
func (c *Consumer) takeGone() map[int32]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.gone
	c.gone = map[int32]bool{}
	return g
}

// withoutPartitions drops the envelopes and records of gone partitions.
func withoutPartitions(envs []Envelope, recs []*kgo.Record, gone map[int32]bool) ([]Envelope, []*kgo.Record) {
	keptEnvs := envs[:0:0]
	for _, e := range envs {
		if !gone[e.KafkaPartition] {
			keptEnvs = append(keptEnvs, e)
		}
	}
	keptRecs := recs[:0:0]
	for _, r := range recs {
		if !gone[r.Partition] {
			keptRecs = append(keptRecs, r)
		}
	}
	return keptEnvs, keptRecs
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
// Exactly-once effect comes from claim-first dedup in ApplyBatch, not from
// the group protocol: BlockRebalanceOnPoll holds a rebalance only up to the
// rebalance timeout, after which another member can own the partition and
// apply the same records. Both transactions then race on event_log's unique
// index and the loser applies nothing, so the ledger is never double-applied.
//
// Stall, not corrupt: a batch that fails in Postgres for any reason other
// than decoding (malformed events are skipped by the applier) is retried
// forever with backoff from 100 ms doubling to 5 s, logging the partition
// and offset range and incrementing pandora_ledger_batch_retries_total.
// Money events are never skipped; isolating a poison record into a
// dead_events table is an M3/M4 follow-up. Between retries rebalances are
// allowed, so OnPartitionsLost/OnPartitionsRevoked can run; records of
// partitions this member no longer owns are dropped from the retry (the new
// owner re-reads them from the committed offset).
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
		// Polled records belong to partitions owned now: callbacks cannot run
		// between this poll and the next AllowRebalance.
		c.takeGone()
		if len(recs) == 0 {
			c.cl.AllowRebalance()
			continue
		}
		envs := Decode(recs, c.log)
		batchSize.Observe(float64(len(envs)))
		recs, err := c.applyWithRetry(ctx, envs, recs)
		if err != nil {
			return err
		}
		if len(recs) > 0 {
			if err := c.cl.CommitRecords(ctx, recs...); err != nil {
				// Safe: PG already holds the batch; a redelivery is deduped by event_log.
				c.log.Warn("kafka commit failed", "err", err)
			}
		}
		c.cl.AllowRebalance()
	}
}

// applyWithRetry applies envs until it commits, ctx ends, or every partition
// in the batch is gone. It returns the records still owned (to commit).
func (c *Consumer) applyWithRetry(ctx context.Context, envs []Envelope, recs []*kgo.Record) ([]*kgo.Record, error) {
	backoff := 100 * time.Millisecond
	for {
		start := time.Now()
		err := c.app.ApplyBatch(ctx, envs)
		if err == nil {
			applySeconds.Observe(time.Since(start).Seconds())
			return recs, nil
		}
		batchRetries.Inc()
		c.log.Error("apply batch failed, retrying", "offsets", offsetRanges(recs), "err", err, "backoff", backoff)
		c.cl.AllowRebalance() // let a pending rebalance and its Lost/Revoked callbacks run
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if gone := c.takeGone(); len(gone) > 0 {
			envs, recs = withoutPartitions(envs, recs, gone)
			c.log.Warn("partitions lost or revoked, dropping their in-flight records",
				"partitions", slices.Sorted(maps.Keys(gone)), "remaining", offsetRanges(recs))
			if len(recs) == 0 {
				return nil, nil
			}
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}
