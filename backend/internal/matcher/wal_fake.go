package matcher

import (
	"context"
	"sync"
)

// FakeEvent is one published engine.events record in the fake.
type FakeEvent struct {
	Partition uint32
	Key       string
	Value     []byte
}

// FakeWAL is an in-memory WAL for unit tests (no Docker on the dev box).
// Exported fields are read directly by tests once the runner is idle; the
// accessor methods are race-free for reads concurrent with the runner.
type FakeWAL struct {
	mu          sync.Mutex
	Records     map[uint32][][]byte
	Events      []FakeEvent
	Appends     int
	FailAppend  error // when non-nil, Append stores nothing and returns it
	batchSizes  []int // record count of every Append call, failed ones included
	publishes   int
	failPublish error
}

// NewFakeWAL returns an empty fake with no partitions.
func NewFakeWAL() *FakeWAL { return &FakeWAL{Records: make(map[uint32][][]byte)} }

// Append stores copies of values on partition, or returns FailAppend.
func (w *FakeWAL) Append(_ context.Context, partition uint32, values [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.Appends++
	w.batchSizes = append(w.batchSizes, len(values))
	if w.FailAppend != nil {
		return w.FailAppend
	}
	for _, v := range values {
		cp := make([]byte, len(v))
		copy(cp, v)
		w.Records[partition] = append(w.Records[partition], cp)
	}
	return nil
}

// Publish records every event, or stores nothing and returns the error set
// by SetFailPublish.
func (w *FakeWAL) Publish(_ context.Context, partition uint32, keys []string, values [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.publishes++
	if w.failPublish != nil {
		return w.failPublish
	}
	for i := range values {
		w.Events = append(w.Events, FakeEvent{Partition: partition, Key: keys[i], Value: values[i]})
	}
	return nil
}

// Replay calls fn with every stored record of partition in order.
func (w *FakeWAL) Replay(_ context.Context, partition uint32, fn func(offset int64, value []byte) error) error {
	w.mu.Lock()
	recs := append([][]byte(nil), w.Records[partition]...)
	w.mu.Unlock()
	for i, v := range recs {
		if err := fn(int64(i), v); err != nil {
			return err
		}
	}
	return nil
}

// Close is a no-op.
func (w *FakeWAL) Close() {}

// EventCount is a race-free read for tests.
func (w *FakeWAL) EventCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.Events)
}

// PublishCount returns how many Publish calls were made, failed ones
// included.
func (w *FakeWAL) PublishCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.publishes
}

// SetFailPublish makes every later Publish store nothing and return err
// until it is called again with nil. Safe to call while the runner runs.
func (w *FakeWAL) SetFailPublish(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failPublish = err
}

// AppendCount returns how many Append calls were made, failed ones
// included; race-free for reads concurrent with the runner.
func (w *FakeWAL) AppendCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Appends
}

// BatchSizes returns the record count of every Append call in order.
func (w *FakeWAL) BatchSizes() []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]int(nil), w.batchSizes...)
}
