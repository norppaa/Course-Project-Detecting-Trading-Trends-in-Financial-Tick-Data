package server

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"trading-trends/gateway/model"
	"trading-trends/gateway/store"
)

// Hub maintains the set of active clients and broadcasts filtered messages.
//
// Routing rules:
//   - indicator (one per symbol per closed window): stored as a chart point
//     and sent as type "signal" only to clients subscribed to that symbol.
//   - BUY/SELL advisory (from a "signal" message, or the signal field of an
//     indicator): stored once (deduplicated) and sent as type "alert" to
//     every client. Warm-up advisories (first window, EMAs start at 0) are
//     stored and counted but not pushed live.
type Hub struct {
	store *store.MemoryStore

	// Registered clients
	clients map[*Client]bool

	// Inbound messages from Kafka
	broadcast chan model.SignalEvent

	// Register / Unregister requests
	register   chan *Client
	unregister chan *Client

	mu sync.RWMutex

	indicators atomic.Int64 // indicator messages processed
	advisories atomic.Int64 // distinct advisories processed
	dropped    atomic.Int64 // client messages dropped because a client was too slow
}

// HubStats are counters for the debug panel.
type HubStats struct {
	Indicators      int64 `json:"indicators"`
	Advisories      int64 `json:"advisories"`
	DroppedMessages int64 `json:"dropped_client_messages"`
	QueueLength     int   `json:"queue_length"`
	QueueCapacity   int   `json:"queue_capacity"`
}

// NewHub initializes a new Hub.
func NewHub(st *store.MemoryStore) *Hub {
	return &Hub{
		store:      st,
		clients:    make(map[*Client]bool),
		broadcast:  make(chan model.SignalEvent, 4096),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run starts the Hub event loop.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			n := len(h.clients)
			h.mu.Unlock()

			log.Printf("[Hub] Client connected: %s (Total: %d)", client.id, n)
			h.sendWelcome(client)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("[Hub] Client disconnected: %s (Remaining: %d)", client.id, len(h.clients))
			}
			h.mu.Unlock()

		case event := <-h.broadcast:
			h.process(event)
		}
	}
}

func (h *Hub) process(event model.SignalEvent) {
	if event.Symbol == "" {
		return
	}

	if event.Kind != model.KindSignal {
		// Indicator (or a legacy message without kind): one chart point.
		h.indicators.Add(1)
		h.store.AddIndicator(event)
		h.sendToSubscribers(event.Symbol, h.encode("signal", event))
	}

	if event.IsAlert() && h.store.AddAlert(event) {
		h.advisories.Add(1)
		h.sendToAll(h.encode("alert", event))
	}
}

func (h *Hub) encode(msgType string, event model.SignalEvent) []byte {
	b, err := json.Marshal(model.WSServerMessage{
		Type:      msgType,
		Symbol:    event.Symbol,
		Data:      event,
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		log.Printf("[Hub] encode error: %v", err)
		return nil
	}
	return b
}

func (h *Hub) sendToSubscribers(symbol string, msg []byte) {
	if msg == nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for client := range h.clients {
		if client.IsSubscribed(symbol) {
			h.trySend(client, msg)
		}
	}
}

func (h *Hub) sendToAll(msg []byte) {
	if msg == nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for client := range h.clients {
		h.trySend(client, msg)
	}
}

func (h *Hub) trySend(client *Client, msg []byte) {
	select {
	case client.send <- msg:
	default:
		h.dropped.Add(1) // slow client: drop rather than stall the pipeline
	}
}

// Broadcast hands an event to the Hub. It blocks while the hub queue is
// full, which applies back-pressure to the Kafka consumer instead of
// silently losing chart points.
func (h *Hub) Broadcast(event model.SignalEvent) {
	h.broadcast <- event
}

// ClientCount returns the number of connected clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Stats returns hub counters.
func (h *Hub) Stats() HubStats {
	return HubStats{
		Indicators:      h.indicators.Load(),
		Advisories:      h.advisories.Load(),
		DroppedMessages: h.dropped.Load(),
		QueueLength:     len(h.broadcast),
		QueueCapacity:   cap(h.broadcast),
	}
}

func (h *Hub) sendWelcome(client *Client) {
	welcomePayload := map[string]interface{}{
		"symbols":       h.store.GetSymbols(),
		"markets":       h.store.GetMarkets(250),
		"stats":         h.store.GetStats(),
		"recent_alerts": h.store.GetAlerts(50, true),
		"client_id":     client.id,
	}

	msg, err := json.Marshal(model.WSServerMessage{
		Type:      "info",
		Data:      welcomePayload,
		Timestamp: time.Now().UTC(),
	})
	if err == nil {
		client.Send(msg)
	}
}
