// Package matcher sequences commands through the engine.wal Kafka topic,
// applies them to the deterministic engine and serves MatcherService.
//
// Every RPC validates its input (identifier lengths, ULID order ids, price
// and outcome ranges) before anything is journaled, so the WAL only holds
// well-formed commands. AdminCommand is unauthenticated in M1: the matcher
// listens on the cluster network only and the gateway does not forward it;
// authentication lands in M4.
package matcher

import (
	"context"

	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
)

const (
	// TopicWAL holds enginev1.Command records keyed by partition number.
	TopicWAL = kafkax.TopicEngineWAL
	// TopicEvents holds enginev1.Event records keyed by market_id.
	TopicEvents = kafkax.TopicEngineEvents
	// Epoch is the fencing epoch stamped on every record. M1 has a single
	// owner, so it is constant; M3 replaces it with the group generation.
	Epoch uint64 = 1
	// MaxBatch is the most requests one WAL produce carries.
	MaxBatch = 256
)

// WAL is the journal the runner writes to and replays from. It deals in
// marshaled bytes so the fake needs no proto knowledge.
type WAL interface {
	// Append produces values as one batch to engine.wal partition and
	// returns once every record is acked (acks=all) or the batch failed.
	Append(ctx context.Context, partition uint32, values [][]byte) error
	// Publish produces events to engine.events (same partition) keyed by
	// keys[i] and returns once every record is acked or the batch failed.
	// Events are derived from the WAL and at-least-once: a non-nil error
	// means the runner must retry the same batch (ruling X3), so the Kafka
	// implementation waits for the acks instead of logging failures.
	Publish(ctx context.Context, partition uint32, keys []string, values [][]byte) error
	// Replay calls fn for every record on engine.wal partition from offset
	// 0 up to the high watermark observed at call time, in offset order.
	// It stops at the first error fn returns.
	Replay(ctx context.Context, partition uint32, fn func(offset int64, value []byte) error) error
	// Close flushes buffered records and releases the clients.
	Close()
}
