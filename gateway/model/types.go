package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SignalEvent represents the computed metrics and trading advice for a 5-minute window.
// It maps directly to Query 1 (EMA38 & EMA100) and Query 2 (Buy/Sell Breakouts).
type SignalEvent struct {
	Symbol      string    `json:"symbol"`
	SecType     string    `json:"sec_type,omitempty"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	WindowIndex int64     `json:"window_index"`
	Close       float64   `json:"close"`
	EMA38       float64   `json:"ema38"`
	EMA100      float64   `json:"ema100"`
	Signal      string    `json:"signal,omitempty"` // "BUY", "SELL", or empty / "HOLD"
	TradeCount  int       `json:"trade_count,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

// UnmarshalJSON provides flexible decoding of SignalEvent from JSON,
// tolerating multiple naming styles (e.g. ID, Symbol, Close, Last, price, ema_38, etc.)
// and various timestamp representations.
func (s *SignalEvent) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// 1. Symbol / ID
	s.Symbol = getString(raw, "symbol", "Symbol", "id", "ID", "Instrument", "instrument")

	// 2. SecType
	s.SecType = getString(raw, "sec_type", "SecType", "sectype")

	// 3. Close / Last price
	s.Close = getFloat(raw, "close", "Close", "last", "Last", "price", "Price")

	// 4. EMA38
	s.EMA38 = getFloat(raw, "ema38", "EMA38", "ema_38", "Ema38", "ema_j1")

	// 5. EMA100
	s.EMA100 = getFloat(raw, "ema100", "EMA100", "ema_100", "Ema100", "ema_j2")

	// 6. Signal / Advisory
	sig := strings.ToUpper(getString(raw, "signal", "Signal", "advisory", "Advisory", "action", "Action", "type", "Type"))
	if sig == "BUY" || sig == "BULLISH" {
		s.Signal = "BUY"
	} else if sig == "SELL" || sig == "BEARISH" {
		s.Signal = "SELL"
	} else if sig == "HOLD" {
		s.Signal = "HOLD"
	} else {
		s.Signal = sig
	}

	// 7. Window Index
	s.WindowIndex = int64(getFloat(raw, "window_index", "WindowIndex", "window", "Window", "w_i", "index"))

	// 8. Trade Count
	s.TradeCount = int(getFloat(raw, "trade_count", "TradeCount", "trades", "count"))

	// 9. Timestamps
	s.WindowStart = parseTimeField(raw, "window_start", "WindowStart", "start_time", "start")
	s.WindowEnd = parseTimeField(raw, "window_end", "WindowEnd", "end_time", "end")
	s.Timestamp = parseTimeField(raw, "timestamp", "Timestamp", "time", "Time", "created_at")

	if s.Timestamp.IsZero() {
		if !s.WindowEnd.IsZero() {
			s.Timestamp = s.WindowEnd
		} else {
			s.Timestamp = time.Now().UTC()
		}
	}
	if s.WindowEnd.IsZero() && !s.Timestamp.IsZero() {
		s.WindowEnd = s.Timestamp
	}

	return nil
}

// WSClientMessage represents an incoming command from WebSocket clients.
type WSClientMessage struct {
	Action  string   `json:"action"` // "subscribe", "unsubscribe", "subscribe_all", "get_history", "ping"
	Symbols []string `json:"symbols,omitempty"`
	Symbol  string   `json:"symbol,omitempty"`
}

// WSServerMessage represents an outgoing payload to WebSocket clients.
type WSServerMessage struct {
	Type      string      `json:"type"` // "signal", "alert", "history", "symbols", "snapshot", "pong", "info", "error"
	Symbol    string      `json:"symbol,omitempty"`
	Data      interface{} `json:"data,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// Helper parsing functions
func getString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if val, ok := m[k]; ok && val != nil {
			if str, ok := val.(string); ok {
				return strings.TrimSpace(str)
			}
			return fmt.Sprintf("%v", val)
		}
	}
	return ""
}

func getFloat(m map[string]interface{}, keys ...string) float64 {
	for _, k := range keys {
		if val, ok := m[k]; ok && val != nil {
			switch v := val.(type) {
			case float64:
				return v
			case float32:
				return float64(v)
			case int:
				return float64(v)
			case int64:
				return float64(v)
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
					return f
				}
			}
		}
	}
	return 0.0
}

func parseTimeField(m map[string]interface{}, keys ...string) time.Time {
	for _, k := range keys {
		if val, ok := m[k]; ok && val != nil {
			switch v := val.(type) {
			case float64:
				// Assume epoch milliseconds or seconds
				if v > 1e11 {
					return time.UnixMilli(int64(v)).UTC()
				}
				return time.Unix(int64(v), 0).UTC()
			case int64:
				if v > 1e11 {
					return time.UnixMilli(v).UTC()
				}
				return time.Unix(v, 0).UTC()
			case string:
				v = strings.TrimSpace(v)
				if v == "" {
					continue
				}
				// Try RFC3339
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					return t.UTC()
				}
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					return t.UTC()
				}
				// Try "2006-01-02 15:04:05"
				if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
					return t.UTC()
				}
				// Try DEBS format "08-11-2021 09:05:00"
				if t, err := time.Parse("02-01-2006 15:04:05", v); err == nil {
					return t.UTC()
				}
				// Try epoch string
				if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
					if ms > 1e11 {
						return time.UnixMilli(ms).UTC()
					}
					return time.Unix(ms, 0).UTC()
				}
			}
		}
	}
	return time.Time{}
}

