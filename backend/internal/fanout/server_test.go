package fanout

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
)

func TestWebSocketRoundTrip(t *testing.T) {
	hub := NewHub()
	hub.LoadSnapshot(snap())
	ts := httptest.NewServer(NewWSServer(hub, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.CloseNow() }()

	read := func() map[string]any {
		var m map[string]any
		if err := wsjson.Read(ctx, conn, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	if err := wsjson.Write(ctx, conn, ClientMsg{Type: "subscribe", Markets: []string{"mkt_demo"}}); err != nil {
		t.Fatal(err)
	}
	if m := read(); m["type"] != "snapshot" || len(m["levels"].([]any)) != 2 {
		t.Fatalf("snapshot = %v", m)
	}
	hub.ApplyCommand([]*enginev1.Event{lvl(100, 43, 0, 0, 55, 3), mintFill(100, 43, 1)})
	if m := read(); m["type"] != "delta" || len(m["trades"].([]any)) != 1 {
		t.Fatalf("delta = %v", m)
	}
	if err := wsjson.Write(ctx, conn, ClientMsg{Type: "ping"}); err != nil {
		t.Fatal(err)
	}
	if m := read(); m["type"] != "pong" {
		t.Fatalf("pong = %v", m)
	}
	if err := wsjson.Write(ctx, conn, ClientMsg{Type: "dance"}); err != nil {
		t.Fatal(err)
	}
	if m := read(); m["type"] != "error" {
		t.Fatalf("error = %v", m)
	}
}
