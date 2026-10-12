package kafkax_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	"github.com/aureliusnguyen/pandora/backend/internal/pkg/kafkax"
)

func TestRecordDecodeRoundTrip(t *testing.T) {
	in := &enginev1.Command{
		Seq: 7, Partition: 0, Epoch: 1, EngineTimeUnixNanos: 123,
		Body: &enginev1.Command_SubmitOrder{SubmitOrder: &enginev1.SubmitOrder{
			OrderId: "o1", MarketId: "mkt_demo", UserId: "u_alice", ClientOrderId: "c1",
			Outcome: 0, Side: commonv1.Side_SIDE_BUY, Price: 55, Qty: 10,
			Tif: commonv1.TimeInForce_TIME_IN_FORCE_GTC,
		}},
	}
	rec, err := kafkax.Record(kafkax.TopicEngineWAL, kafkax.PartitionKey(0), in)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Topic != "engine.wal" || string(rec.Key) != "0" {
		t.Fatalf("topic/key = %q/%q", rec.Topic, rec.Key)
	}
	out := &enginev1.Command{}
	if err := kafkax.Decode(rec, out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round trip mismatch:\n in=%v\nout=%v", in, out)
	}
}

func TestDecodeGarbageIsError(t *testing.T) {
	rec := &kgo.Record{Topic: "engine.events", Partition: 0, Offset: 42, Value: []byte{0xff, 0xff, 0xff}}
	err := kafkax.Decode(rec, &enginev1.Event{})
	if err == nil || !strings.Contains(err.Error(), "engine.events/0@42") {
		t.Fatalf("Decode(garbage) = %v, want error naming engine.events/0@42", err)
	}
}

func TestCleanBrokers(t *testing.T) {
	got, err := kafkax.CleanBrokers([]string{" a:1", " ", "b:2 ", ""})
	if err != nil || !slices.Equal(got, []string{"a:1", "b:2"}) {
		t.Fatalf("CleanBrokers = %v, %v", got, err)
	}
	if _, err := kafkax.CleanBrokers([]string{"", "  "}); err == nil {
		t.Fatal("want error for all-empty brokers")
	}
}

func TestM1Topics(t *testing.T) {
	want := []kafkax.TopicSpec{
		{Name: "engine.wal", Partitions: 1, RetentionMs: 604800000},
		{Name: "engine.events", Partitions: 1, RetentionMs: 604800000},
	}
	if got := kafkax.M1Topics(); !slices.Equal(got, want) {
		t.Fatalf("M1Topics = %v", got)
	}
}

func TestEnsureTopicsValidatesBeforeDialing(t *testing.T) {
	ctx := context.Background()
	if err := kafkax.EnsureTopics(ctx, nil, kafkax.M1Topics()); err == nil {
		t.Error("want error for no brokers")
	}
	bad := [][]kafkax.TopicSpec{
		{{Name: "", Partitions: 1, RetentionMs: 1}},
		{{Name: "t", Partitions: 0, RetentionMs: 1}},
		{{Name: "t", Partitions: 1, RetentionMs: 0}},
		{{Name: "t", Partitions: 1, RetentionMs: 1}, {Name: "t", Partitions: 1, RetentionMs: 1}},
	}
	for _, specs := range bad {
		if err := kafkax.EnsureTopics(ctx, []string{"127.0.0.1:1"}, specs); err == nil ||
			!strings.Contains(err.Error(), "invalid topic spec") {
			t.Errorf("EnsureTopics(%v) = %v, want invalid topic spec", specs, err)
		}
	}
}

func TestNewProducerBuildsWithoutBroker(t *testing.T) {
	cl, err := kafkax.NewProducer([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
}
