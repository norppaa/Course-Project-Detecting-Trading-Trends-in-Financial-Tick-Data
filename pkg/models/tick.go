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

// WindowIndicator represents Query 1 output: 5-minute tumbling window metrics.
type WindowIndicator struct {
	Symbol      string    `json:"symbol"`       // e.g. "RDSA.NL"
	WindowStart time.Time `json:"window_start"` // Start of the 5-minute tumbling window
	WindowEnd   time.Time `json:"window_end"`   // End of the 5-minute tumbling window
	LastPrice   float64   `json:"last_price"`   // Closing price observed in this window
	EMA38       float64   `json:"ema_38"`       // Exponential moving average (j=38)
	EMA100      float64   `json:"ema_100"`      // Exponential moving average (j=100)
}

// SignalType defines whether an advisory is BUY (bullish) or SELL (bearish).
type SignalType string

const (
	SignalBuy  SignalType = "BUY"
	SignalSell SignalType = "SELL"
)

// TradeSignal represents Query 2 output: Buy/Sell advisory produced on crossover.
type TradeSignal struct {
	Symbol     string     `json:"symbol"`       // e.g. "RDSA.NL"
	Type       SignalType `json:"type"`         // BUY or SELL
	Timestamp  time.Time  `json:"timestamp"`    // Time when crossover occurred (window end)
	Price      float64    `json:"price"`        // Last price triggering the signal
	EMA38      float64    `json:"ema_38"`       // Current window EMA38
	EMA100     float64    `json:"ema_100"`      // Current window EMA100
	PrevEMA38  float64    `json:"prev_ema_38"`  // Previous window EMA38
	PrevEMA100 float64    `json:"prev_ema_100"` // Previous window EMA100
}
