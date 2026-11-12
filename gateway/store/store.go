package store

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"trading-trends/gateway/model"
)

// StoreStats summarizes the in-memory store metrics.
type StoreStats struct {
	TotalSignals    int64     `json:"total_signals"`     // window indicators received (chart points)
	TotalBuyAlerts  int64     `json:"total_buy_alerts"`  // distinct BUY advisories (warm-up excluded)
	TotalSellAlerts int64     `json:"total_sell_alerts"` // distinct SELL advisories
	WarmupAlerts    int64     `json:"warmup_alerts"`     // first-window BUYs caused by EMA_0 = 0
	ActiveSymbols   int       `json:"active_symbols"`
	LastEventTime   time.Time `json:"last_event_time"` // latest event time (window end) seen
	LastUpdate      time.Time `json:"last_update"`     // wall clock of the latest message
}

// MarketSummary is one row of the market overview.
type MarketSummary struct {
	Symbol     string    `json:"symbol"`
	SecType    string    `json:"sec_type,omitempty"`
	Windows    int64     `json:"windows"` // windows received for this symbol
	Close      float64   `json:"close"`
	EMA38      float64   `json:"ema38"`
	EMA100     float64   `json:"ema100"`
	WindowEnd  time.Time `json:"window_end"`
	LastSignal string    `json:"last_signal,omitempty"` // latest non-warm-up advisory
	Alerts     int64     `json:"alerts"`                // non-warm-up advisories for this symbol
}

type symbolMeta struct {
	windows    int64
	alerts     int64
	lastSignal string
}

// MemoryStore provides thread-safe in-memory caching of window events and alerts.
type MemoryStore struct {
	mu           sync.RWMutex
	maxPerSymbol int
	maxAlerts    int

	history      map[string][]model.SignalEvent
	latestBySym  map[string]model.SignalEvent
	meta         map[string]*symbolMeta
	recentAlerts []model.SignalEvent

	// Dedupe of advisories: the analyzer reports each crossover twice on
	// the same topic (inside the indicator and as a separate signal).
	alertKeys     map[string]struct{}
	alertKeyOrder []string

	totalSignals    int64
	totalBuyAlerts  int64
	totalSellAlerts int64
	warmupAlerts    int64
	lastEventTime   time.Time
	lastUpdate      time.Time
}

const maxAlertKeys = 20000

// NewMemoryStore initializes a new MemoryStore.
func NewMemoryStore(maxPerSymbol, maxAlerts int) *MemoryStore {
	if maxPerSymbol <= 0 {
		maxPerSymbol = 500
	}
	if maxAlerts <= 0 {
		maxAlerts = 200
	}
	return &MemoryStore{
		maxPerSymbol: maxPerSymbol,
		maxAlerts:    maxAlerts,
		history:      make(map[string][]model.SignalEvent),
		latestBySym:  make(map[string]model.SignalEvent),
		meta:         make(map[string]*symbolMeta),
		recentAlerts: make([]model.SignalEvent, 0, maxAlerts),
		alertKeys:    make(map[string]struct{}),
	}
}

// AddSignal stores a window event and, if it carries a BUY/SELL, records
// the advisory. Kept for callers that do not distinguish message kinds.
func (s *MemoryStore) AddSignal(event model.SignalEvent) {
	s.AddIndicator(event)
	if event.IsAlert() {
		s.AddAlert(event)
	}
}

// AddIndicator appends a window indicator (one chart point) for its symbol.
// A repeated window (same window end) replaces the stored point.
func (s *MemoryStore) AddIndicator(event model.SignalEvent) {
	if event.Symbol == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalSignals++
	s.touchLocked(event)

	m := s.metaLocked(event.Symbol)
	m.windows++
	s.latestBySym[event.Symbol] = event

	h := s.history[event.Symbol]
	if n := len(h); n > 0 && !event.WindowEnd.IsZero() && h[n-1].WindowEnd.Equal(event.WindowEnd) {
		h[n-1] = event
	} else {
		h = append(h, event)
		if len(h) > s.maxPerSymbol {
			// Copy so the old backing array can be released.
			h = append([]model.SignalEvent(nil), h[len(h)-s.maxPerSymbol:]...)
		}
	}
	s.history[event.Symbol] = h
}

// AddAlert records a BUY/SELL advisory. It returns false if the event is
// not an advisory or the same advisory (symbol, direction, window) was
// already recorded.
func (s *MemoryStore) AddAlert(event model.SignalEvent) bool {
	if event.Symbol == "" || !event.IsAlert() {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	when := event.WindowEnd
	if when.IsZero() {
		when = event.Timestamp
	}
	key := fmt.Sprintf("%s|%s|%d", event.Symbol, event.Signal, when.UnixNano())
	if _, dup := s.alertKeys[key]; dup {
		return false
	}
	s.alertKeys[key] = struct{}{}
	s.alertKeyOrder = append(s.alertKeyOrder, key)
	if len(s.alertKeyOrder) > maxAlertKeys {
		drop := len(s.alertKeyOrder) - maxAlertKeys
		for _, k := range s.alertKeyOrder[:drop] {
			delete(s.alertKeys, k)
		}
		s.alertKeyOrder = append([]string(nil), s.alertKeyOrder[drop:]...)
	}

	s.touchLocked(event)
	if event.FirstWindow {
		s.warmupAlerts++
	} else {
		if event.Signal == "BUY" {
			s.totalBuyAlerts++
		} else {
			s.totalSellAlerts++
		}
		m := s.metaLocked(event.Symbol)
		m.alerts++
		m.lastSignal = event.Signal
	}

	s.recentAlerts = append(s.recentAlerts, event)
	if len(s.recentAlerts) > s.maxAlerts {
		s.recentAlerts = append([]model.SignalEvent(nil), s.recentAlerts[len(s.recentAlerts)-s.maxAlerts:]...)
	}
	return true
}

func (s *MemoryStore) touchLocked(event model.SignalEvent) {
	if event.Timestamp.After(s.lastEventTime) {
		s.lastEventTime = event.Timestamp
	}
	s.lastUpdate = time.Now().UTC()
}

func (s *MemoryStore) metaLocked(symbol string) *symbolMeta {
	m := s.meta[symbol]
	if m == nil {
		m = &symbolMeta{}
		s.meta[symbol] = m
	}
	return m
}

// GetSymbols returns an alphabetically sorted slice of all tracked symbols.
func (s *MemoryStore) GetSymbols() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	symbols := make([]string, 0, len(s.meta))
	for sym := range s.meta {
		symbols = append(symbols, sym)
	}
	sort.Strings(symbols)
	return symbols
}

// GetHistory returns up to limit historical signal events for a symbol.
func (s *MemoryStore) GetHistory(symbol string, limit int) []model.SignalEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h, ok := s.history[symbol]
	if !ok || len(h) == 0 {
		return []model.SignalEvent{}
	}

	if limit <= 0 || limit >= len(h) {
		result := make([]model.SignalEvent, len(h))
		copy(result, h)
		return result
	}

	result := make([]model.SignalEvent, limit)
	copy(result, h[len(h)-limit:])
	return result
}

// GetLatest returns the latest event for a specific symbol.
func (s *MemoryStore) GetLatest(symbol string) (model.SignalEvent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	event, ok := s.latestBySym[symbol]
	return event, ok
}

// GetLatestAlerts returns the most recent alerts, newest first, including
// warm-up alerts.
func (s *MemoryStore) GetLatestAlerts(limit int) []model.SignalEvent {
	return s.GetAlerts(limit, true)
}

// GetAlerts returns up to limit recent alerts, newest first. Warm-up
// (first-window) alerts are skipped unless includeWarmup is set.
func (s *MemoryStore) GetAlerts(limit int, includeWarmup bool) []model.SignalEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 {
		limit = len(s.recentAlerts)
	}
	result := make([]model.SignalEvent, 0, min(limit, len(s.recentAlerts)))
	for i := len(s.recentAlerts) - 1; i >= 0 && len(result) < limit; i-- {
		a := s.recentAlerts[i]
		if a.FirstWindow && !includeWarmup {
			continue
		}
		result = append(result, a)
	}
	return result
}

// GetSnapshot returns the latest event for every known symbol.
func (s *MemoryStore) GetSnapshot() []model.SignalEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make([]model.SignalEvent, 0, len(s.latestBySym))
	for _, event := range s.latestBySym {
		snapshot = append(snapshot, event)
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].Symbol < snapshot[j].Symbol
	})
	return snapshot
}

// GetMarkets returns one summary per symbol that has chart data, most
// active (most windows) first. limit <= 0 returns all.
func (s *MemoryStore) GetMarkets(limit int) []MarketSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]MarketSummary, 0, len(s.latestBySym))
	for sym, ev := range s.latestBySym {
		m := s.meta[sym]
		out = append(out, MarketSummary{
			Symbol:     sym,
			SecType:    ev.SecType,
			Windows:    m.windows,
			Close:      ev.Close,
			EMA38:      ev.EMA38,
			EMA100:     ev.EMA100,
			WindowEnd:  ev.WindowEnd,
			LastSignal: m.lastSignal,
			Alerts:     m.alerts,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Windows != out[j].Windows {
			return out[i].Windows > out[j].Windows
		}
		return out[i].Symbol < out[j].Symbol
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// GetStats returns current store statistics.
func (s *MemoryStore) GetStats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return StoreStats{
		TotalSignals:    s.totalSignals,
		TotalBuyAlerts:  s.totalBuyAlerts,
		TotalSellAlerts: s.totalSellAlerts,
		WarmupAlerts:    s.warmupAlerts,
		ActiveSymbols:   len(s.meta),
		LastEventTime:   s.lastEventTime,
		LastUpdate:      s.lastUpdate,
	}
}
