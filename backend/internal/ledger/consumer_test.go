package ledger

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
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
