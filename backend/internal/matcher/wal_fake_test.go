package matcher

import (
	"context"
	"errors"
	"testing"
)

func TestFakeWALAppendReplayAndFailure(t *testing.T) {
	ctx := context.Background()
	w := NewFakeWAL()
	if err := w.Append(ctx, 0, [][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(ctx, 0, [][]byte{[]byte("c")}); err != nil {
		t.Fatal(err)
	}
	if w.Appends != 2 || len(w.Records[0]) != 3 {
		t.Fatalf("appends=%d records=%d", w.Appends, len(w.Records[0]))
	}
	var got []string
	var offsets []int64
	err := w.Replay(ctx, 0, func(off int64, v []byte) error {
		offsets = append(offsets, off)
		got = append(got, string(v))
		return nil
	})
	if err != nil || len(got) != 3 || got[2] != "c" || offsets[2] != 2 {
		t.Fatalf("replay: err=%v got=%v offsets=%v", err, got, offsets)
	}
	boom := errors.New("boom")
	w.FailAppend = boom
	if err := w.Append(ctx, 0, [][]byte{[]byte("d")}); !errors.Is(err, boom) || len(w.Records[0]) != 3 {
		t.Fatalf("failed append must not store: err=%v n=%d", err, len(w.Records[0]))
	}
	if err := w.Publish(ctx, 0, []string{"mkt_demo"}, [][]byte{[]byte("e")}); err != nil || len(w.Events) != 1 || w.Events[0].Key != "mkt_demo" {
		t.Fatalf("publish: err=%v events=%+v", err, w.Events)
	}
	stop := errors.New("stop")
	if err := w.Replay(ctx, 0, func(int64, []byte) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("replay must surface the callback error, got %v", err)
	}
	if err := w.Replay(ctx, 7, func(int64, []byte) error { t.Fatal("empty partition must not call back"); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestFakeWALPublishFailure(t *testing.T) {
	ctx := context.Background()
	w := NewFakeWAL()
	boom := errors.New("events down")
	w.SetFailPublish(boom)
	err := w.Publish(ctx, 0, []string{"mkt_demo"}, [][]byte{[]byte("e")})
	if !errors.Is(err, boom) || w.EventCount() != 0 || w.PublishCount() != 1 {
		t.Fatalf("failed publish must store nothing: err=%v events=%d publishes=%d", err, w.EventCount(), w.PublishCount())
	}
	w.SetFailPublish(nil)
	if err := w.Publish(ctx, 0, []string{"mkt_demo"}, [][]byte{[]byte("e")}); err != nil || w.EventCount() != 1 || w.PublishCount() != 2 {
		t.Fatalf("publish after clearing: err=%v events=%d", err, w.EventCount())
	}
}
