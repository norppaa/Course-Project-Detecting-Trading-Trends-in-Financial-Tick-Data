package server

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"trading-trends/gateway/model"
	"trading-trends/gateway/store"
)

// startReader reads frames in the background (a gorilla connection cannot
// be read again after a read timeout). The write pump may pack several
// messages into one frame separated by '\n'.
func startReader(t *testing.T, conn *websocket.Conn) <-chan model.WSServerMessage {
	ch := make(chan model.WSServerMessage, 256)
	go func() {
		defer close(ch)
		for {
			_, frame, err := conn.ReadMessage()
			if err != nil {
				return
			}
			for _, line := range bytes.Split(frame, []byte{'\n'}) {
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				var m model.WSServerMessage
				if err := json.Unmarshal(line, &m); err != nil {
					t.Errorf("bad message %q: %v", line, err)
					return
				}
				ch <- m
			}
		}
	}()
	return ch
}

// collect returns every message that arrives within d.
func collect(ch <-chan model.WSServerMessage, d time.Duration) []model.WSServerMessage {
	var out []model.WSServerMessage
	timeout := time.After(d)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, m)
		case <-timeout:
			return out
		}
	}
}

func TestWebSocketSubscriptionAndBroadcast(t *testing.T) {
	st := store.NewMemoryStore(100, 50)
	hub := NewHub(st)
	go hub.Run()

	s := NewServer(hub, st)
	server := httptest.NewServer(s.Handler())
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	defer conn.Close()

	ch := startReader(t, conn)

	// 1. Welcome message
	msgs := collect(ch, 200*time.Millisecond)
	if len(msgs) != 1 || msgs[0].Type != "info" {
		t.Fatalf("expected one info message, got %+v", msgs)
	}

	// 2. Subscribe -> history
	subBytes, _ := json.Marshal(model.WSClientMessage{Action: "subscribe", Symbols: []string{"BMW.ETR"}})
	if err := conn.WriteMessage(websocket.TextMessage, subBytes); err != nil {
		t.Fatalf("Failed to send subscribe message: %v", err)
	}
	msgs = collect(ch, 200*time.Millisecond)
	if len(msgs) != 1 || msgs[0].Type != "history" || msgs[0].Symbol != "BMW.ETR" {
		t.Fatalf("expected history for BMW.ETR, got %+v", msgs)
	}

	end := time.Date(2021, 11, 8, 10, 0, 0, 0, time.UTC)

	// 3. Indicator for an unsubscribed symbol: no chart update.
	hub.Broadcast(model.SignalEvent{Kind: model.KindIndicator, Symbol: "RDSA.NL", Close: 20, EMA38: 19.9, EMA100: 19.8, WindowEnd: end, Timestamp: end})

	// 4. Crossover on the subscribed symbol, reported twice like the
	//    analyzer does (indicator with signal field + signal message).
	ind := model.SignalEvent{Kind: model.KindIndicator, Symbol: "BMW.ETR", Close: 88.5, EMA38: 88.2, EMA100: 88.0, Signal: "BUY", WindowEnd: end, Timestamp: end}
	sig := ind
	sig.Kind = model.KindSignal
	hub.Broadcast(ind)
	hub.Broadcast(sig)

	// 5. Warm-up BUY of another symbol: stored and pushed with FirstWindow=true.
	hub.Broadcast(model.SignalEvent{Kind: model.KindSignal, Symbol: "X.FR", Close: 1, Signal: "BUY", FirstWindow: true, WindowEnd: end, Timestamp: end})

	msgs = collect(ch, 200*time.Millisecond)
	var signals, realAlerts, warmupAlerts int
	for _, m := range msgs {
		switch m.Type {
		case "signal":
			signals++
			if m.Symbol != "BMW.ETR" {
				t.Fatalf("received chart update for unsubscribed %s", m.Symbol)
			}
		case "alert":
			switch m.Symbol {
			case "BMW.ETR":
				realAlerts++
			case "X.FR":
				warmupAlerts++
			default:
				t.Fatalf("unexpected alert for %s", m.Symbol)
			}
		}
	}
	if signals != 1 || realAlerts != 1 || warmupAlerts != 1 {
		t.Fatalf("expected 1 signal, 1 real alert, 1 warm-up alert; got %d, %d, %d: %+v", signals, realAlerts, warmupAlerts, msgs)
	}

	stats := st.GetStats()
	if stats.TotalSignals != 2 || stats.TotalBuyAlerts != 1 || stats.WarmupAlerts != 1 {
		t.Fatalf("unexpected store stats: %+v", stats)
	}
	if h := st.GetHistory("BMW.ETR", 0); len(h) != 1 {
		t.Fatalf("signal message must not add a chart point, history=%d", len(h))
	}
}
