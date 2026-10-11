package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"trading-trends/gateway/model"
	"trading-trends/gateway/store"
)

func TestWebSocketSubscriptionAndBroadcast(t *testing.T) {
	st := store.NewMemoryStore(100, 50)
	hub := NewHub(st)
	go hub.Run()

	s := NewServer(hub, st)
	server := httptest.NewServer(s.Handler())
	defer server.Close()

	// Convert http:// to ws://
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"

	// Connect client
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket: %v", err)
	}
	defer conn.Close()

	// 1. First message should be the welcome / info message
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read welcome message: %v", err)
	}
	var welcome model.WSServerMessage
	if err := json.Unmarshal(msg, &welcome); err != nil || welcome.Type != "info" {
		t.Fatalf("Expected welcome info message, got: %s", string(msg))
	}

	// 2. Send subscription message for "BMW.ETR"
	subMsg := model.WSClientMessage{
		Action:  "subscribe",
		Symbols: []string{"BMW.ETR"},
	}
	subBytes, _ := json.Marshal(subMsg)
	if err := conn.WriteMessage(websocket.TextMessage, subBytes); err != nil {
		t.Fatalf("Failed to send subscribe message: %v", err)
	}

	// Read history response for BMW.ETR
	_, msg, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read history response: %v", err)
	}
	var histMsg model.WSServerMessage
	if err := json.Unmarshal(msg, &histMsg); err != nil || histMsg.Type != "history" {
		t.Fatalf("Expected history message, got: %s", string(msg))
	}

	// 3. Broadcast an event for an unsubscribed symbol ("RDSA.NL") -> should NOT be received as a signal
	hub.Broadcast(model.SignalEvent{
		Symbol:    "RDSA.NL",
		Close:     20.0,
		EMA38:     19.9,
		EMA100:    19.8,
		Timestamp: time.Now().UTC(),
	})

	// 4. Broadcast an event for the subscribed symbol ("BMW.ETR")
	now := time.Now().UTC()
	hub.Broadcast(model.SignalEvent{
		Symbol:    "BMW.ETR",
		Close:     88.5,
		EMA38:     88.2,
		EMA100:    88.0,
		Signal:    "BUY",
		Timestamp: now,
	})

	// Read signal message
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read broadcast message: %v", err)
	}
	var broadcastMsg model.WSServerMessage
	if err := json.Unmarshal(msg, &broadcastMsg); err != nil {
		t.Fatalf("Failed to parse broadcast message: %v", err)
	}
	if broadcastMsg.Symbol != "BMW.ETR" {
		t.Fatalf("Expected BMW.ETR, got %s", broadcastMsg.Symbol)
	}
	if broadcastMsg.Type != "signal" {
		t.Fatalf("Expected signal type, got %s", broadcastMsg.Type)
	}
}

