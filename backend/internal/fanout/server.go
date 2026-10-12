package fanout

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

const (
	// writeTimeout bounds one frame write; a stuck peer is dropped.
	writeTimeout = 5 * time.Second
	// readLimit bounds one client frame; client messages are tiny.
	readLimit = 4096
	// pingInterval and pingTimeout reap half-open clients.
	pingInterval = 30 * time.Second
	pingTimeout  = 10 * time.Second
)

// WSServer serves the fanout WebSocket protocol on /ws.
type WSServer struct {
	hub *Hub
	log *slog.Logger
}

// NewWSServer returns a server that registers each connection with hub.
func NewWSServer(hub *Hub, log *slog.Logger) *WSServer { return &WSServer{hub: hub, log: log} }

// Handler returns the HTTP handler serving GET /ws.
func (s *WSServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.serveWS)
	return mux
}

func (s *WSServer) serveWS(w http.ResponseWriter, r *http.Request) {
	// The library always allows same-origin requests, and non-browser clients
	// send no Origin header and pass. Cross-origin browsers are allowed only from
	// localhost (pnpm dev, the kind NodePort); a deployed hostname needs a config
	// knob like the gateway's CORS_ORIGINS (future).
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"localhost:*", "127.0.0.1:*"}})
	if err != nil {
		s.log.Warn("ws accept", "err", err)
		return
	}
	conn.SetReadLimit(readLimit)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewClient(func(code websocket.StatusCode, reason string) {
		_ = conn.Close(code, reason)
		cancel()
	})
	s.hub.Register(c)
	defer s.hub.Remove(c)

	go func() { // writer: the only goroutine that writes data frames, in queue order
		for {
			select {
			case <-ctx.Done():
				return
			case b := <-c.Send():
				wctx, wcancel := context.WithTimeout(ctx, writeTimeout)
				err := conn.Write(wctx, websocket.MessageText, b)
				wcancel()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	go func() { // liveness: Ping needs the read loop below to receive the pong
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(ctx, pingTimeout)
				err := conn.Ping(pctx)
				pcancel()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// conn.Read (not wsjson.Read): wsjson closes the socket with 1007 on bad
	// JSON, while the contract answers malformed frames with an error frame.
	for {
		typ, b, err := conn.Read(ctx)
		if err != nil {
			break
		}
		if typ != websocket.MessageText {
			_ = conn.Close(websocket.StatusUnsupportedData, "text frames only")
			break
		}
		var m ClientMsg
		if err := json.Unmarshal(b, &m); err != nil {
			s.hub.Reply(c, "error", SimpleMsg{Type: "error", Message: "malformed message"})
			continue
		}
		switch m.Type {
		case "subscribe":
			s.hub.Subscribe(c, m.Markets)
		case "unsubscribe":
			s.hub.Unsubscribe(c, m.Markets)
		case "ping":
			s.hub.Reply(c, "pong", SimpleMsg{Type: "pong"})
		default:
			s.hub.Reply(c, "error", SimpleMsg{Type: "error", Message: "unknown message type " + m.Type})
		}
	}
	cancel()
	_ = conn.CloseNow()
}
