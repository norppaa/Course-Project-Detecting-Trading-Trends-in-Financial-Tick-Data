package server

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"trading-trends/gateway/model"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024
)

// Client is a middleman between the WebSocket connection and the Hub.
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
	id   string

	subMu         sync.RWMutex
	subscriptions map[string]bool
	subscribeAll  bool
}

// NewClient initializes a new client instance.
func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:           hub,
		conn:          conn,
		send:          make(chan []byte, 512),
		id:            fmt.Sprintf("client-%d", time.Now().UnixNano()),
		subscriptions: make(map[string]bool),
		subscribeAll:  false,
	}
}

// IsSubscribed checks if the client is subscribed to a given symbol.
func (c *Client) IsSubscribed(symbol string) bool {
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	if c.subscribeAll {
		return true
	}
	return c.subscriptions[symbol]
}

// Subscribe adds symbols to the client's subscription set.
func (c *Client) Subscribe(symbols []string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	for _, s := range symbols {
		if s != "" {
			c.subscriptions[s] = true
		}
	}
}

// Unsubscribe removes symbols from the client's subscription set.
func (c *Client) Unsubscribe(symbols []string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	for _, s := range symbols {
		delete(c.subscriptions, s)
	}
}

// SetSubscribeAll updates the global subscription flag.
func (c *Client) SetSubscribeAll(val bool) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.subscribeAll = val
}

// Send enqueues a message to be written to the WebSocket.
func (c *Client) Send(msg []byte) {
	select {
	case c.send <- msg:
	default:
		// Queue full
	}
}

// ReadPump pumps messages from the WebSocket connection to the hub.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[Client %s] Read error: %v", c.id, err)
			}
			break
		}

		c.handleIncomingMessage(message)
	}
}

func (c *Client) handleIncomingMessage(raw []byte) {
	var req model.WSClientMessage
	if err := json.Unmarshal(raw, &req); err != nil {
		log.Printf("[Client %s] Invalid message JSON: %v", c.id, err)
		return
	}

	switch req.Action {
	case "subscribe":
		symbols := req.Symbols
		if req.Symbol != "" {
			symbols = append(symbols, req.Symbol)
		}
		c.Subscribe(symbols)
		log.Printf("[Client %s] Subscribed to %v", c.id, symbols)

		// Send initial history for newly subscribed symbols
		for _, sym := range symbols {
			c.sendSymbolHistory(sym)
		}

	case "unsubscribe":
		symbols := req.Symbols
		if req.Symbol != "" {
			symbols = append(symbols, req.Symbol)
		}
		c.Unsubscribe(symbols)
		log.Printf("[Client %s] Unsubscribed from %v", c.id, symbols)

	case "subscribe_all":
		c.SetSubscribeAll(true)
		log.Printf("[Client %s] Subscribed to ALL symbols", c.id)

	case "unsubscribe_all":
		c.SetSubscribeAll(false)
		c.subMu.Lock()
		c.subscriptions = make(map[string]bool)
		c.subMu.Unlock()

	case "get_history":
		if req.Symbol != "" {
			c.sendSymbolHistory(req.Symbol)
		}

	case "get_symbols":
		symbols := c.hub.store.GetSymbols()
		msg, _ := json.Marshal(model.WSServerMessage{
			Type:      "symbols",
			Data:      symbols,
			Timestamp: time.Now().UTC(),
		})
		c.Send(msg)

	case "ping":
		msg, _ := json.Marshal(model.WSServerMessage{
			Type:      "pong",
			Timestamp: time.Now().UTC(),
		})
		c.Send(msg)

	default:
		log.Printf("[Client %s] Unknown action: %s", c.id, req.Action)
	}
}

func (c *Client) sendSymbolHistory(symbol string) {
	history := c.hub.store.GetHistory(symbol, 200)
	msg, err := json.Marshal(model.WSServerMessage{
		Type:      "history",
		Symbol:    symbol,
		Data:      history,
		Timestamp: time.Now().UTC(),
	})
	if err == nil {
		c.Send(msg)
	}
}

// WritePump pumps messages from the hub to the WebSocket connection.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued messages to the current websocket frame
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
