package models

import "time"

// MarketTick represents a clean financial tick event published to the "market-ticks" topic.
type MarketTick struct {
	ID          string    `json:"id"`           // Symbol code with exchange suffix (e.g. "RDSA.NL")
	SecType     string    `json:"sec_type"`     // Security type: "E" (Equity) or "I" (Index)
	Timestamp   time.Time `json:"timestamp"`    // Parsed event-time timestamp
	Price       float64   `json:"price"`        // Last traded price
	TradingDate string    `json:"trading_date"` // Original DD-MM-YYYY date string
	TradingTime string    `json:"trading_time"` // Original HH:MM:SS.sss time string
}

// ---------------------------------------------------------------------------
// Control messages on "market-ticks"
//
// A message whose Kafka header HeaderKind is missing or "tick" carries a
// MarketTick. The producer can also send control messages, which must be
// written to EVERY partition (explicit partition, not key-hashed):
//
//   - KindHeartbeat, value = Heartbeat JSON. Promises that the producer has
//     already sent every event with event time < EventTime (up to the
//     analyzer's grace period). Lets windows close on quiet partitions.
//   - KindEndOfStream, empty value. The replay is over: the analyzer closes
//     all remaining windows.
//
// Optional header HeaderProducedAt (RFC 3339 nano, wall clock) on any message
// is copied into the analyzer's output for end-to-end latency measurement.
// ---------------------------------------------------------------------------

const (
	HeaderKind       = "kind"
	HeaderProducedAt = "produced_at"

	KindTick        = "tick"
	KindHeartbeat   = "heartbeat"
	KindEndOfStream = "eos"

	// Output kinds (also set as the HeaderKind header on output messages).
	KindIndicator = "indicator"
	KindSignal    = "signal"
)

// Heartbeat is the value of a KindHeartbeat control message.
type Heartbeat struct {
	EventTime time.Time `json:"event_time"` // producer's latest event time sent so far
}

// Timing carries wall-clock timestamps for latency measurement. They refer to
// the input message that made the window closable (the "trigger").
type Timing struct {
	ProducedAt time.Time `json:"produced_at,omitzero"` // producer send time of the trigger (if the producer sets HeaderProducedAt)
	IngestedAt time.Time `json:"ingested_at"`          // analyzer received the trigger
	EmittedAt  time.Time `json:"emitted_at"`           // analyzer handed the result to the broker client
}

// WindowIndicator represents Query 1 output: 5-minute tumbling window metrics.
type WindowIndicator struct {
	Kind        string    `json:"kind"`             // always KindIndicator
	Symbol      string    `json:"symbol"`           // e.g. "RDSA.NL"
	SecType     string    `json:"sec_type"`         // "E" or "I"
	WindowStart time.Time `json:"window_start"`     // Start of the 5-minute tumbling window
	WindowEnd   time.Time `json:"window_end"`       // End of the 5-minute tumbling window
	LastPrice   float64   `json:"last_price"`       // Closing price observed in this window
	HasTick     bool      `json:"has_tick"`         // false if LastPrice was carried over from an earlier window
	EMA38       float64   `json:"ema_38"`           // Exponential moving average (j=38)
	EMA100      float64   `json:"ema_100"`          // Exponential moving average (j=100)
	Signal      string    `json:"signal,omitempty"` // "BUY"/"SELL" if this window produced an advisory
	Timing
}

// SignalType defines whether an advisory is BUY (bullish) or SELL (bearish).
type SignalType string

const (
	SignalBuy  SignalType = "BUY"
	SignalSell SignalType = "SELL"
)

// TradeSignal represents Query 2 output: Buy/Sell advisory produced on crossover.
type TradeSignal struct {
	Kind        string     `json:"kind"`         // always KindSignal
	Symbol      string     `json:"symbol"`       // e.g. "RDSA.NL"
	SecType     string     `json:"sec_type"`     // "E" or "I"
	Type        SignalType `json:"type"`         // BUY or SELL
	WindowStart time.Time  `json:"window_start"` // Start of the window in which the crossover occurred
	Timestamp   time.Time  `json:"timestamp"`    // Time when crossover occurred (window end)
	Price       float64    `json:"price"`        // Last price triggering the signal
	EMA38       float64    `json:"ema_38"`       // Current window EMA38
	EMA100      float64    `json:"ema_100"`      // Current window EMA100
	PrevEMA38   float64    `json:"prev_ema_38"`  // Previous window EMA38
	PrevEMA100  float64    `json:"prev_ema_100"` // Previous window EMA100
	Timing
}
