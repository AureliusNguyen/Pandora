package fanout

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// writeTimeout bounds one frame write; a stuck peer is dropped.
const writeTimeout = 5 * time.Second

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
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"localhost:*", "127.0.0.1:*"}})
	if err != nil {
		s.log.Warn("ws accept", "err", err)
		return
	}
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

	for {
		var m ClientMsg
		if err := wsjson.Read(ctx, conn, &m); err != nil {
			break
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
