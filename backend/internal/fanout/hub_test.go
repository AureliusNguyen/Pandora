package fanout

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/coder/websocket"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

type closed struct{ code websocket.StatusCode }

func newTestClient() (*Client, chan closed) {
	ch := make(chan closed, 1)
	return NewClient(func(c websocket.StatusCode, _ string) { ch <- closed{c} }), ch
}

func recv(t *testing.T, c *Client) map[string]any {
	t.Helper()
	select {
	case b := <-c.Send():
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	default:
		t.Fatal("queue empty")
		return nil
	}
}

func TestSubscribeSnapshotThenDelta(t *testing.T) {
	h := NewHub()
	h.LoadSnapshot(snap())
	c, _ := newTestClient()
	h.Register(c)
	h.Subscribe(c, []string{"mkt_demo", "mkt_nope"})
	if m := recv(t, c); m["type"] != "snapshot" || m["market_seq"].(float64) != 42 {
		t.Fatalf("first = %v", m)
	}
	if m := recv(t, c); m["type"] != "error" {
		t.Fatalf("unknown market = %v", m)
	}
	h.ApplyCommand([]*enginev1.Event{lvl(100, 43, 0, 0, 55, 3)})
	if m := recv(t, c); m["type"] != "delta" || m["market_seq"].(float64) != 43 {
		t.Fatalf("delta = %v", m)
	}
	h.Unsubscribe(c, []string{"mkt_demo"})
	h.ApplyCommand([]*enginev1.Event{lvl(101, 44, 0, 0, 55, 4)})
	if len(c.Send()) != 0 {
		t.Fatal("unsubscribed client received a delta")
	}
}

func TestOverflowEvictsOnlySlowClient(t *testing.T) {
	h := NewHub()
	h.LoadSnapshot(snap())
	slow, slowClosed := newTestClient()
	fast, fastClosed := newTestClient()
	h.Register(slow)
	h.Register(fast)
	h.Subscribe(slow, []string{"mkt_demo"})
	h.Subscribe(fast, []string{"mkt_demo"})
	<-fast.Send() // fast reader drains its snapshot
	for i := uint64(0); i < SendQueueSize; i++ {
		h.ApplyCommand([]*enginev1.Event{lvl(200+i, 43+i, 0, 0, 55, i+1)})
		<-fast.Send()
	}
	select {
	case cl := <-slowClosed: // closeFn runs in its own goroutine
		if cl.code != websocket.StatusTryAgainLater {
			t.Fatalf("close code = %d, want 1013", cl.code)
		}
	case <-time.After(time.Second):
		t.Fatal("slow client not evicted on overflow")
	}
	h.ApplyCommand([]*enginev1.Event{lvl(999, 999, 0, 0, 55, 1)})
	if m := recv(t, fast); m["type"] != "delta" {
		t.Fatalf("fast client starved: %v", m)
	}
	select {
	case <-fastClosed:
		t.Fatal("fast client evicted")
	default:
	}
}

// An evicted client whose read loop still delivers a late subscribe must not
// be re-added to the subscription maps (it would leak and never be removed).
func TestEvictedClientLateSubscribeNotReAdded(t *testing.T) {
	h := NewHub()
	h.LoadSnapshot(snap())
	slow, slowClosed := newTestClient()
	h.Register(slow)
	h.Subscribe(slow, []string{"mkt_demo"})
	for i := uint64(0); i < SendQueueSize; i++ {
		h.ApplyCommand([]*enginev1.Event{lvl(200+i, 43+i, 0, 0, 55, i+1)})
	}
	select {
	case <-slowClosed:
	case <-time.After(time.Second):
		t.Fatal("slow client not evicted on overflow")
	}
	h.Subscribe(slow, []string{"mkt_demo"})
	h.mu.Lock()
	_, inSubs := h.subs["mkt_demo"][slow]
	_, inClients := h.clients[slow]
	h.mu.Unlock()
	if inSubs || inClients || len(slow.subs) != 0 {
		t.Fatalf("evicted client re-added: subs=%v clients=%v own=%v", inSubs, inClients, slow.subs)
	}
}

func TestMarketCreatedAddsView(t *testing.T) {
	h := NewHub()
	h.ApplyCommand([]*enginev1.Event{{Seq: 1, MarketId: "mkt_new", MarketSeq: 1, Body: &enginev1.Event_MarketCreated{
		MarketCreated: &enginev1.MarketCreated{Market: &enginev1.CreateMarket{MarketId: "mkt_new"}},
	}}})
	c, _ := newTestClient()
	h.Register(c)
	h.Subscribe(c, []string{"mkt_new"})
	if m := recv(t, c); m["type"] != "snapshot" || m["status"] != "open" {
		t.Fatalf("snapshot = %v", m)
	}
}
