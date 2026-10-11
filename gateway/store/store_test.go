package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"trading-trends/gateway/model"
)

func TestMemoryStore(t *testing.T) {
	s := NewMemoryStore(5, 5)

	now := time.Now().UTC()
	for i := 1; i <= 10; i++ {
		sig := ""
		if i%3 == 0 {
			sig = "BUY"
		} else if i%5 == 0 {
			sig = "SELL"
		}

		s.AddSignal(model.SignalEvent{
			Symbol:    "RDSA.NL",
			Close:     10.0 + float64(i),
			EMA38:     10.0,
			EMA100:    9.0,
			Signal:    sig,
			Timestamp: now.Add(time.Duration(i) * time.Minute),
		})
	}

	history := s.GetHistory("RDSA.NL", 10)
	if len(history) != 5 {
		t.Fatalf("Expected history trimmed to maxPerSymbol 5, got %d", len(history))
	}
	if history[4].Close != 20.0 {
		t.Errorf("Expected latest close to be 20.0, got %f", history[4].Close)
	}

	stats := s.GetStats()
	if stats.TotalSignals != 10 {
		t.Errorf("Expected 10 total signals, got %d", stats.TotalSignals)
	}
	if stats.ActiveSymbols != 1 {
		t.Errorf("Expected 1 active symbol, got %d", stats.ActiveSymbols)
	}
	if stats.TotalBuyAlerts == 0 {
		t.Errorf("Expected buy alerts > 0")
	}

	alerts := s.GetLatestAlerts(10)
	if len(alerts) == 0 {
		t.Errorf("Expected alerts to be recorded")
	}
}

func TestConcurrentStore(t *testing.T) {
	s := NewMemoryStore(100, 50)
	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			sym := fmt.Sprintf("SYM%d.EX", workerID%3)
			for i := 0; i < iterations; i++ {
				s.AddSignal(model.SignalEvent{
					Symbol:    sym,
					Close:     float64(i),
					EMA38:     float64(i),
					EMA100:    float64(i),
					Signal:    "BUY",
					Timestamp: time.Now(),
				})
				_ = s.GetHistory(sym, 10)
				_ = s.GetSymbols()
				_ = s.GetLatestAlerts(5)
			}
		}(w)
	}

	wg.Wait()
	stats := s.GetStats()
	if stats.TotalSignals != int64(workers*iterations) {
		t.Errorf("Expected %d signals, got %d", workers*iterations, stats.TotalSignals)
	}
}

