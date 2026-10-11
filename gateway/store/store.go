package store

import (
	"sort"
	"sync"
	"time"

	"trading-trends/gateway/model"
)

// StoreStats summarizes the in-memory store metrics.
type StoreStats struct {
	TotalSignals    int64     `json:"total_signals"`
	TotalBuyAlerts  int64     `json:"total_buy_alerts"`
	TotalSellAlerts int64     `json:"total_sell_alerts"`
	ActiveSymbols   int       `json:"active_symbols"`
	LastEventTime   time.Time `json:"last_event_time"`
}

// MemoryStore provides thread-safe in-memory caching of window events and alerts.
type MemoryStore struct {
	mu           sync.RWMutex
	maxPerSymbol int
	maxAlerts    int

	history      map[string][]model.SignalEvent
	latestBySym  map[string]model.SignalEvent
	recentAlerts []model.SignalEvent

	totalSignals    int64
	totalBuyAlerts  int64
	totalSellAlerts int64
	lastEventTime   time.Time
}

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
		recentAlerts: make([]model.SignalEvent, 0, maxAlerts),
	}
}

// AddSignal appends a signal event to the in-memory cache and updates stats.
func (s *MemoryStore) AddSignal(event model.SignalEvent) {
	if event.Symbol == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalSignals++
	if event.Timestamp.After(s.lastEventTime) {
		s.lastEventTime = event.Timestamp
	}

	// Update latest per symbol
	s.latestBySym[event.Symbol] = event

	// Append to symbol history with ring buffer trimming
	h := s.history[event.Symbol]
	h = append(h, event)
	if len(h) > s.maxPerSymbol {
		// Keep the newest maxPerSymbol entries
		h = h[len(h)-s.maxPerSymbol:]
	}
	s.history[event.Symbol] = h

	// Record breakout advisory alerts (BUY / SELL)
	if event.Signal == "BUY" {
		s.totalBuyAlerts++
		s.recordAlertLocked(event)
	} else if event.Signal == "SELL" {
		s.totalSellAlerts++
		s.recordAlertLocked(event)
	}
}

func (s *MemoryStore) recordAlertLocked(event model.SignalEvent) {
	s.recentAlerts = append(s.recentAlerts, event)
	if len(s.recentAlerts) > s.maxAlerts {
		s.recentAlerts = s.recentAlerts[len(s.recentAlerts)-s.maxAlerts:]
	}
}

// GetSymbols returns an alphabetically sorted slice of all tracked symbols.
func (s *MemoryStore) GetSymbols() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	symbols := make([]string, 0, len(s.latestBySym))
	for sym := range s.latestBySym {
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

// GetLatestAlerts returns the most recent alerts (newest first).
func (s *MemoryStore) GetLatestAlerts(limit int) []model.SignalEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n := len(s.recentAlerts)
	if n == 0 {
		return []model.SignalEvent{}
	}

	if limit <= 0 || limit > n {
		limit = n
	}

	result := make([]model.SignalEvent, limit)
	for i := 0; i < limit; i++ {
		result[i] = s.recentAlerts[n-1-i] // reverse order: newest first
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

// GetStats returns current store statistics.
func (s *MemoryStore) GetStats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return StoreStats{
		TotalSignals:    s.totalSignals,
		TotalBuyAlerts:  s.totalBuyAlerts,
		TotalSellAlerts: s.totalSellAlerts,
		ActiveSymbols:   len(s.latestBySym),
		LastEventTime:   s.lastEventTime,
	}
}

