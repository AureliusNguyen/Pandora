package fanout

import (
	"encoding/json"
	"sync"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
)

// SendQueueSize bounds each connection's outbound queue; overflow evicts the
// connection with close code 1013 (try again later).
const SendQueueSize = 256

var (
	connections = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "pandora", Subsystem: "fanout", Name: "connections",
		Help: "Registered WebSocket connections.",
	})
	messages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "pandora", Subsystem: "fanout", Name: "messages_total",
		Help: "Messages enqueued to clients, by message type.",
	}, []string{"type"})
	evictions = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "pandora", Subsystem: "fanout", Name: "evictions_total",
		Help: "Connections closed with 1013 because their send queue overflowed.",
	})
)

// Client is one WebSocket connection as seen by the Hub. Its fields other
// than send are guarded by the Hub mutex.
type Client struct {
	send    chan []byte
	closeFn func(websocket.StatusCode, string)
	subs    map[string]struct{}
	evicted bool
}

// NewClient returns a client with a SendQueueSize queue. closeFn is called
// (in its own goroutine) when the hub evicts the client.
func NewClient(closeFn func(websocket.StatusCode, string)) *Client {
	return &Client{send: make(chan []byte, SendQueueSize), closeFn: closeFn, subs: map[string]struct{}{}}
}

// Send is the client's outbound queue; the connection's writer drains it.
func (c *Client) Send() <-chan []byte { return c.send }

// Hub owns all book views and subscriptions under one mutex, so a snapshot
// enqueued on subscribe is always followed by exactly the deltas after it.
type Hub struct {
	mu      sync.Mutex
	views   map[string]*BookView
	subs    map[string]map[*Client]struct{}
	clients map[*Client]struct{}
}

// NewHub returns an empty hub.
func NewHub() *Hub {
	return &Hub{views: map[string]*BookView{}, subs: map[string]map[*Client]struct{}{}, clients: map[*Client]struct{}{}}
}

// LoadSnapshot installs a matcher snapshot unless the current view is newer.
func (h *Hub) LoadSnapshot(resp *matcherv1.GetBookResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if v := h.views[resp.GetMarketId()]; v != nil && v.Seq() > resp.GetMarketSeq() {
		return
	}
	h.views[resp.GetMarketId()] = NewBookView(resp)
}

// Register adds a connected client.
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
	connections.Inc()
}

// Remove drops a client and all its subscriptions; safe to call twice and
// after eviction.
func (h *Hub) Remove(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeLocked(c)
}

func (h *Hub) removeLocked(c *Client) {
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	for m := range c.subs {
		delete(h.subs[m], c)
	}
	clear(c.subs)
	connections.Dec()
}

// enqueueLocked never blocks; a full queue evicts the client with 1013.
func (h *Hub) enqueueLocked(c *Client, typ string, b []byte) {
	if c.evicted {
		return
	}
	select {
	case c.send <- b:
		messages.WithLabelValues(typ).Inc()
	default:
		c.evicted = true
		evictions.Inc()
		h.removeLocked(c)
		go c.closeFn(websocket.StatusTryAgainLater, "send queue overflow")
	}
}

func (h *Hub) replyLocked(c *Client, typ string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.enqueueLocked(c, typ, b)
}

// Reply enqueues one message for c alone (pong, error).
func (h *Hub) Reply(c *Client, typ string, v any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.replyLocked(c, typ, v)
}

// Subscribe adds c to each known market and enqueues its snapshot; an
// unknown market gets an error message. A client that is not registered
// (removed or evicted) is ignored, so a late subscribe cannot leak it.
func (h *Hub) Subscribe(c *Client, markets []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; !ok || c.evicted {
		return
	}
	for _, m := range markets {
		v := h.views[m]
		if v == nil {
			h.replyLocked(c, "error", SimpleMsg{Type: "error", Message: "unknown market " + m})
			continue
		}
		if h.subs[m] == nil {
			h.subs[m] = map[*Client]struct{}{}
		}
		h.subs[m][c] = struct{}{}
		c.subs[m] = struct{}{}
		h.replyLocked(c, "snapshot", v.Snapshot())
		if c.evicted { // the snapshot overflowed the queue and removed c
			return
		}
	}
}

// Unsubscribe removes c from the given markets.
func (h *Hub) Unsubscribe(c *Client, markets []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range markets {
		delete(h.subs[m], c)
		delete(c.subs, m)
	}
}

func (h *Hub) broadcastLocked(market, typ string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	for c := range h.subs[market] {
		h.enqueueLocked(c, typ, b)
	}
}

// ApplyCommand takes the events of one engine command (or, after an idle
// flush split, part of one), splits them by market, updates each view and
// broadcasts one delta (plus a market message) per market. The tail of a split
// command is broadcast as a snapshot of the market instead of a delta.
func (h *Hub) ApplyCommand(events []*enginev1.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	byMarket := map[string][]*enginev1.Event{}
	var order []string
	for _, ev := range events {
		m := ev.GetMarketId()
		if m == "" {
			continue
		}
		if _, ok := byMarket[m]; !ok {
			order = append(order, m)
		}
		byMarket[m] = append(byMarket[m], ev)
	}
	for _, m := range order {
		evs := byMarket[m]
		v := h.views[m]
		if v == nil {
			if !hasMarketCreated(evs) {
				continue
			}
			v = NewEmptyBookView(m)
			h.views[m] = v
		}
		delta, mm, tail, ok := v.Apply(evs)
		if !ok {
			continue
		}
		if tail {
			// A split command's tail keeps the head's market_seq, which
			// clients would drop as a delta; a snapshot is applied as is.
			h.broadcastLocked(m, "snapshot", v.Snapshot())
		} else {
			h.broadcastLocked(m, "delta", delta)
		}
		if mm != nil {
			h.broadcastLocked(m, "market", mm)
		}
	}
}

func hasMarketCreated(evs []*enginev1.Event) bool {
	for _, ev := range evs {
		if ev.GetMarketCreated() != nil {
			return true
		}
	}
	return false
}
