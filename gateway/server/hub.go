package server

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"trading-trends/gateway/model"
	"trading-trends/gateway/store"
)

// Hub maintains the set of active clients and broadcasts filtered messages.
type Hub struct {
	store *store.MemoryStore

	// Registered clients
	clients map[*Client]bool

	// Inbound messages from Kafka / Mock
	broadcast chan model.SignalEvent

	// Register / Unregister requests
	register   chan *Client
	unregister chan *Client

	mu sync.RWMutex
}

// NewHub initializes a new Hub.
func NewHub(st *store.MemoryStore) *Hub {
	return &Hub{
		store:      st,
		clients:    make(map[*Client]bool),
		broadcast:  make(chan model.SignalEvent, 2048),
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
			h.mu.Unlock()

			log.Printf("[Hub] Client connected: %s (Total: %d)", client.id, len(h.clients))

			// Send welcome state to client: available symbols & stats & recent alerts
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
			// Save in memory store
			h.store.AddSignal(event)

			// Prepare JSON payloads
			signalMsg, _ := json.Marshal(model.WSServerMessage{
				Type:      "signal",
				Symbol:    event.Symbol,
				Data:      event,
				Timestamp: time.Now().UTC(),
			})

			var alertMsg []byte
			if event.Signal == "BUY" || event.Signal == "SELL" {
				alertMsg, _ = json.Marshal(model.WSServerMessage{
					Type:      "alert",
					Symbol:    event.Symbol,
					Data:      event,
					Timestamp: time.Now().UTC(),
				})
			}

			// Broadcast according to client subscriptions
			h.mu.RLock()
			for client := range h.clients {
				isSubscribed := client.IsSubscribed(event.Symbol)

				if isSubscribed {
					select {
					case client.send <- signalMsg:
					default:
						// Slow client, drop or disconnect
					}
				}

				// If there's a BUY/SELL alert, deliver to all clients for global alert feed
				if alertMsg != nil && !isSubscribed {
					select {
					case client.send <- alertMsg:
					default:
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Broadcast sends a SignalEvent to the Hub for distribution.
func (h *Hub) Broadcast(event model.SignalEvent) {
	select {
	case h.broadcast <- event:
	default:
		log.Printf("[Hub] Warning: broadcast queue full, dropping event for %s", event.Symbol)
	}
}

// ClientCount returns the number of connected clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) sendWelcome(client *Client) {
	symbols := h.store.GetSymbols()
	stats := h.store.GetStats()
	recentAlerts := h.store.GetLatestAlerts(20)

	welcomePayload := map[string]interface{}{
		"symbols":       symbols,
		"stats":         stats,
		"recent_alerts": recentAlerts,
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

