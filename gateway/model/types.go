package model

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Message kinds published by the stream analyzer on "trading-signals"
// (see pkg/models/tick.go). The kind is sent both as the Kafka header
// "kind" and as a "kind" field in the JSON body.
const (
	KindIndicator = "indicator" // Query 1: one message per (symbol, closed 5-min window)
	KindSignal    = "signal"    // Query 2: one message per BUY/SELL crossover
)

// WindowSize is the tumbling window length used by the analyzer.
const WindowSize = 5 * time.Minute

// EMA smoothing factors (alpha_j = 2 / (1 + j)), identical to analyzer/engine.
const (
	alpha38  = 2.0 / (1.0 + 38.0)
	alpha100 = 2.0 / (1.0 + 100.0)
)

// SignalEvent is the gateway's normalized view of one analyzer output message.
// Indicators (Query 1) and crossover advisories (Query 2) both decode into it;
// Kind tells them apart.
type SignalEvent struct {
	Kind        string    `json:"kind,omitempty"` // "indicator" or "signal" ("" for legacy producers)
	Symbol      string    `json:"symbol"`
	SecType     string    `json:"sec_type,omitempty"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	WindowIndex int64     `json:"window_index"` // window number since the Unix epoch (window_start / 5 min)
	Close       float64   `json:"close"`
	HasTick     bool      `json:"has_tick"` // false if the close was carried over from an earlier window
	EMA38       float64   `json:"ema38"`
	EMA100      float64   `json:"ema100"`
	Signal      string    `json:"signal,omitempty"` // "BUY", "SELL" or empty
	// FirstWindow is true for the symbol's first evaluated window. Because
	// both EMAs start from 0 (DEBS 2022 spec), that window always satisfies
	// the bullish crossover rule, so its BUY is a warm-up artefact.
	FirstWindow bool      `json:"first_window,omitempty"`
	TradeCount  int       `json:"trade_count,omitempty"`
	Timestamp   time.Time `json:"timestamp"` // event time of the result (window end)

	// Wall-clock timing copied from the analyzer output, used for latency.
	ProducedAt time.Time `json:"produced_at,omitzero"`
	EmittedAt  time.Time `json:"emitted_at,omitzero"`
}

// IsAlert reports whether the event carries a BUY or SELL advisory.
func (s *SignalEvent) IsAlert() bool {
	return s.Signal == "BUY" || s.Signal == "SELL"
}

// UnmarshalJSON provides flexible decoding of SignalEvent from JSON,
// tolerating multiple naming styles (e.g. ID, Symbol, Close, Last, price, ema_38, etc.)
// and various timestamp representations.
func (s *SignalEvent) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	s.Kind = strings.ToLower(getString(raw, "kind", "Kind"))
	s.Symbol = getString(raw, "symbol", "Symbol", "id", "ID", "Instrument", "instrument")
	s.SecType = getString(raw, "sec_type", "SecType", "sectype")
	s.Close = getFloat(raw, "close", "Close", "last", "Last", "price", "Price", "last_price", "LastPrice")
	s.EMA38 = getFloat(raw, "ema38", "EMA38", "ema_38", "Ema38", "ema_j1")
	s.EMA100 = getFloat(raw, "ema100", "EMA100", "ema_100", "Ema100", "ema_j2")

	s.HasTick = true
	if v, ok := raw["has_tick"].(bool); ok {
		s.HasTick = v
	}

	sig := strings.ToUpper(getString(raw, "signal", "Signal", "advisory", "Advisory", "action", "Action", "type", "Type"))
	switch sig {
	case "BUY", "BULLISH":
		s.Signal = "BUY"
	case "SELL", "BEARISH":
		s.Signal = "SELL"
	case "", "NONE", "HOLD":
		s.Signal = ""
	default:
		s.Signal = sig
	}

	s.WindowIndex = int64(getFloat(raw, "window_index", "WindowIndex", "window", "Window", "w_i", "index"))
	s.TradeCount = int(getFloat(raw, "trade_count", "TradeCount", "trades", "count"))

	s.WindowStart = parseTimeField(raw, "window_start", "WindowStart", "start_time", "start")
	s.WindowEnd = parseTimeField(raw, "window_end", "WindowEnd", "end_time", "end")
	s.Timestamp = parseTimeField(raw, "timestamp", "Timestamp", "time", "Time", "created_at")
	s.ProducedAt = parseTimeField(raw, "produced_at")
	s.EmittedAt = parseTimeField(raw, "emitted_at")

	// The analyzer's TradeSignal has window_start + timestamp (= window end);
	// WindowIndicator has window_start + window_end. Fill in the gaps.
	if s.WindowEnd.IsZero() {
		switch {
		case !s.Timestamp.IsZero():
			s.WindowEnd = s.Timestamp
		case !s.WindowStart.IsZero():
			s.WindowEnd = s.WindowStart.Add(WindowSize)
		}
	}
	if s.WindowStart.IsZero() && !s.WindowEnd.IsZero() {
		s.WindowStart = s.WindowEnd.Add(-WindowSize)
	}
	if s.Timestamp.IsZero() {
		if !s.WindowEnd.IsZero() {
			s.Timestamp = s.WindowEnd
		} else {
			s.Timestamp = time.Now().UTC()
		}
	}
	if s.WindowEnd.IsZero() {
		s.WindowEnd = s.Timestamp
	}
	if s.WindowIndex == 0 && !s.WindowStart.IsZero() {
		s.WindowIndex = s.WindowStart.Unix() / int64(WindowSize/time.Second)
	}

	// Detect the warm-up window (see FirstWindow).
	if _, ok := raw["prev_ema_38"]; ok {
		s.FirstWindow = getFloat(raw, "prev_ema_38") == 0 && getFloat(raw, "prev_ema_100") == 0
	} else {
		s.FirstWindow = isFirstWindow(s.Close, s.EMA38, s.EMA100)
	}
	if v, ok := raw["first_window"].(bool); ok {
		s.FirstWindow = v
	}

	return nil
}

// isFirstWindow reports whether the EMAs equal the first-window values
// close*alpha, i.e. the previous EMAs were 0.
func isFirstWindow(close, ema38, ema100 float64) bool {
	if close <= 0 {
		return false
	}
	tol := 1e-9 * close
	return math.Abs(ema38-close*alpha38) <= tol && math.Abs(ema100-close*alpha100) <= tol
}

// WSClientMessage represents an incoming command from WebSocket clients.
type WSClientMessage struct {
	Action  string   `json:"action"` // "subscribe", "unsubscribe", "subscribe_all", "get_history", "get_symbols", "ping"
	Symbols []string `json:"symbols,omitempty"`
	Symbol  string   `json:"symbol,omitempty"`
}

// WSServerMessage represents an outgoing payload to WebSocket clients.
type WSServerMessage struct {
	Type      string      `json:"type"` // "signal", "alert", "history", "symbols", "pong", "info", "error"
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
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					return t.UTC()
				}
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					return t.UTC()
				}
				if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
					return t.UTC()
				}
				// DEBS format "08-11-2021 09:05:00"
				if t, err := time.Parse("02-01-2006 15:04:05", v); err == nil {
					return t.UTC()
				}
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
