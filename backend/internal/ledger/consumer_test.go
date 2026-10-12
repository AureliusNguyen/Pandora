package ledger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func TestDecodeSkipsGarbageKeepsOffsets(t *testing.T) {
	good, err := proto.Marshal(mintEvents()[0])
	if err != nil {
		t.Fatal(err)
	}
	recs := []*kgo.Record{
		{Partition: 0, Offset: 7, Value: good},
		{Partition: 0, Offset: 8, Value: []byte{0xff, 0xff, 0xff}},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	before := testutil.ToFloat64(decodeErrors)
	envs := Decode(recs, log)
	if len(envs) != 2 || envs[0].Offset != 7 || envs[0].Event.GetSeq() != 1 || envs[1].Offset != 8 || envs[1].Event != nil {
		t.Fatalf("envs = %+v", envs)
	}
	if got := testutil.ToFloat64(decodeErrors) - before; got != 1 {
		t.Fatalf("decode error counter grew by %v, want 1", got)
	}
	// The garbage record is last in the batch; the stored offset must still pass it.
	r := newFakeRepo()
	if err := NewApplier(r, log).ApplyBatch(context.Background(), envs); err != nil {
		t.Fatal(err)
	}
	if r.offsets["pandora-ledger/0"] != 9 || !r.applied[EventKey{0, 1, 0}] {
		t.Fatalf("offsets = %v applied = %v", r.offsets, r.applied)
	}
}

func TestLostAndRevokedPartitionsAreDroppedFromInFlightBatch(t *testing.T) {
	c := &Consumer{gone: map[int32]bool{}}
	c.onGone(context.Background(), nil, map[string][]int32{eventsTopic: {1}, "other.topic": {0}})
	gone := c.takeGone()
	if len(gone) != 1 || !gone[1] || len(c.takeGone()) != 0 {
		t.Fatalf("gone = %v", gone)
	}
	recs := []*kgo.Record{{Partition: 0, Offset: 3}, {Partition: 1, Offset: 5}, {Partition: 0, Offset: 4}}
	envs := []Envelope{{KafkaPartition: 0, Offset: 3}, {KafkaPartition: 1, Offset: 5}, {KafkaPartition: 0, Offset: 4}}
	envs, recs = withoutPartitions(envs, recs, gone)
	if len(envs) != 2 || len(recs) != 2 || envs[1].Offset != 4 || recs[1].Offset != 4 {
		t.Fatalf("envs = %+v recs = %d", envs, len(recs))
	}
	envs, recs = withoutPartitions(envs, recs, map[int32]bool{0: true})
	if len(envs) != 0 || len(recs) != 0 {
		t.Fatal("all partitions gone must leave an empty batch")
	}
}

// lockedRepo serializes access to a fakeRepo shared with a running Consumer.
type lockedRepo struct {
	mu sync.Mutex
	r  *fakeRepo
}

func (l *lockedRepo) InTx(ctx context.Context, fn func(context.Context, Tx) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.InTx(ctx, fn)
}

func TestConsumerRunAppliesAndStoresOffsets(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, eventsTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	prod, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, ev := range mintEvents() {
		v, err := proto.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: eventsTopic, Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	repo := &lockedRepo{r: newFakeRepo()}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c, err := NewConsumer(cluster.ListenAddrs(), NewApplier(repo, log), log)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	for {
		repo.mu.Lock()
		off, txns := repo.r.offsets["pandora-ledger/0"], len(repo.r.txns)
		repo.mu.Unlock()
		if off == 4 && txns == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("consumer did not apply the batch: offset=%d txns=%d", off, txns)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}
