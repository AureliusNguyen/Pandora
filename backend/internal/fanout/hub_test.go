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

// The tail of a split command keeps the head's market_seq, so it is broadcast
// as a full snapshot (applied unconditionally) rather than a delta that
// clients would drop; the tail's fills ride in its trades field. A stale
// command after it is still dropped silently.
func TestSplitTailBroadcastsSnapshot(t *testing.T) {
	h := NewHub()
	h.LoadSnapshot(snap())
	c, _ := newTestClient()
	h.Register(c)
	h.Subscribe(c, []string{"mkt_demo"})
	if m := recv(t, c); m["type"] != "snapshot" || m["trades"] != nil {
		t.Fatalf("subscribe snapshot = %v, want no trades field", m)
	}
	h.ApplyCommand([]*enginev1.Event{lvl(100, 43, 0, 1, 40, 0)})
	if m := recv(t, c); m["type"] != "delta" || m["market_seq"].(float64) != 43 {
		t.Fatalf("head = %v", m)
	}
	h.ApplyCommand([]*enginev1.Event{mintFill(100, 43, 1), lvl(100, 43, 2, 0, 55, 5)})
	m := recv(t, c)
	ls, _ := m["levels"].([]any)
	if m["type"] != "snapshot" || m["market_seq"].(float64) != 43 || m["status"] != "open" || len(ls) != 1 {
		t.Fatalf("tail = %v, want a snapshot at 43 with one level", m)
	}
	if l := ls[0].(map[string]any); l["outcome"].(float64) != 0 || l["side"] != "BUY" || l["price"].(float64) != 55 || l["qty"].(float64) != 5 {
		t.Fatalf("tail snapshot level = %v", l)
	}
	trs, _ := m["trades"].([]any)
	if len(trs) != 1 {
		t.Fatalf("tail snapshot trades = %v, want the tail's one fill", m["trades"])
	}
	if tr := trs[0].(map[string]any); tr["fill_id"] != "f1" || tr["kind"] != "MINT" || tr["price"].(float64) != 45 ||
		tr["qty"].(float64) != 5 || tr["outcome"].(float64) != 1 || tr["taker_side"] != "BUY" {
		t.Fatalf("tail snapshot trade = %v", tr)
	}
	h.ApplyCommand([]*enginev1.Event{lvl(99, 43, 0, 0, 55, 999)})
	if len(c.Send()) != 0 {
		t.Fatal("stale command after a tail must not be broadcast")
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
