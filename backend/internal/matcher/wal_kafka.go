package matcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
)

// closeFlushTimeout bounds the producer flush in Close.
const closeFlushTimeout = 10 * time.Second

// KafkaWAL implements WAL on franz-go. One idempotent producer (acks=all,
// linger 1 ms, manual partitioner so record.Partition is authoritative)
// serves both topics; Replay opens a short-lived consumer client.
type KafkaWAL struct {
	cl      *kgo.Client
	brokers []string
	log     *slog.Logger
}

// NewKafkaWAL builds the producer via kafkax.NewProducer (seed brokers,
// acks=all, 1 ms linger, idempotent by franz-go default) plus the
// matcher-only options, and pings the cluster once.
func NewKafkaWAL(ctx context.Context, brokers []string, log *slog.Logger) (*KafkaWAL, error) {
	cl, err := kafkax.NewProducer(brokers,
		kgo.ClientID("pandora-matcher"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.MaxBufferedRecords(1<<16),
		kgo.RecordRetries(10),
		kgo.RecordDeliveryTimeout(10*time.Second),
	)
	if err != nil {
		return nil, err
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka ping: %w", err)
	}
	return &KafkaWAL{cl: cl, brokers: brokers, log: log}, nil
}

// Append produces values as one batch to engine.wal and waits for the acks.
func (w *KafkaWAL) Append(ctx context.Context, partition uint32, values [][]byte) error {
	key := []byte(kafkax.PartitionKey(partition))
	recs := make([]*kgo.Record, len(values))
	for i, v := range values {
		recs[i] = &kgo.Record{Topic: TopicWAL, Key: key, Value: v, Partition: int32(partition)}
	}
	// ProduceSync returns after every record is acked or failed; with the
	// idempotent producer a failed record fails all later records of the
	// same partition, so a non-nil FirstErr means the batch is not safe.
	return w.cl.ProduceSync(ctx, recs...).FirstErr()
}

// Publish produces events to engine.events and waits for the acks so the
// runner can retry a failed batch (ruling X3: events are never dropped).
func (w *KafkaWAL) Publish(ctx context.Context, partition uint32, keys []string, values [][]byte) error {
	recs := make([]*kgo.Record, len(values))
	for i, v := range values {
		recs[i] = &kgo.Record{Topic: TopicEvents, Key: []byte(keys[i]), Value: v, Partition: int32(partition)}
	}
	return w.cl.ProduceSync(ctx, recs...).FirstErr()
}

// Replay consumes engine.wal partition from offset 0 to the high watermark
// observed when it is called.
func (w *KafkaWAL) Replay(ctx context.Context, partition uint32, fn func(offset int64, value []byte) error) error {
	ends, err := kadm.NewClient(w.cl).ListEndOffsets(ctx, TopicWAL)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	lo, ok := ends.Lookup(TopicWAL, int32(partition))
	if !ok {
		return fmt.Errorf("partition %d of %s not found", partition, TopicWAL)
	}
	if lo.Err != nil {
		return fmt.Errorf("end offset: %w", lo.Err)
	}
	end := lo.Offset
	if end == 0 {
		return nil
	}
	rc, err := kafkax.NewClient(w.brokers,
		kgo.ClientID("pandora-matcher-replay"),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{TopicWAL: {int32(partition): kgo.NewOffset().AtStart()}}),
	)
	if err != nil {
		return fmt.Errorf("replay client: %w", err)
	}
	defer rc.Close()
	next := int64(0)
	for next < end {
		fs := rc.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, fe := range fs.Errors() {
			if !errors.Is(fe.Err, context.Canceled) {
				return fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
			}
		}
		var cbErr error
		fs.EachRecord(func(r *kgo.Record) {
			if cbErr != nil || r.Offset >= end {
				return
			}
			cbErr = fn(r.Offset, r.Value)
			next = r.Offset + 1
		})
		if cbErr != nil {
			return cbErr
		}
	}
	return nil
}

// Close flushes buffered records (bounded by closeFlushTimeout) so the last
// events of a rollout are not lost, then closes the producer.
func (w *KafkaWAL) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), closeFlushTimeout)
	defer cancel()
	if err := w.cl.Flush(ctx); err != nil {
		w.log.Error("kafka flush on close", "err", err)
	}
	w.cl.Close()
}

var (
	_ WAL = (*KafkaWAL)(nil)
	_ WAL = (*FakeWAL)(nil)
)
