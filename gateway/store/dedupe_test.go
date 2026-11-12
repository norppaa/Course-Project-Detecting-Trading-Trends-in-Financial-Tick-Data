package store

import (
	"testing"
	"time"

	"trading-trends/gateway/model"
)

func TestAlertDedupeAndWarmup(t *testing.T) {
	s := NewMemoryStore(100, 50)
	end := time.Date(2021, 11, 8, 9, 5, 0, 0, time.UTC)

	ind := model.SignalEvent{Kind: model.KindIndicator, Symbol: "A.FR", Close: 10, EMA38: 10.1, EMA100: 10, Signal: "BUY", WindowEnd: end, Timestamp: end}
	sig := ind
	sig.Kind = model.KindSignal

	s.AddIndicator(ind)
	if !s.AddAlert(ind) {
		t.Fatal("first alert should be new")
	}
	if s.AddAlert(sig) {
		t.Fatal("the signal message for the same window must be deduplicated")
	}

	warm := model.SignalEvent{Symbol: "B.FR", Close: 5, Signal: "BUY", FirstWindow: true, WindowEnd: end, Timestamp: end}
	if !s.AddAlert(warm) {
		t.Fatal("warm-up alert should be recorded")
	}

	st := s.GetStats()
	if st.TotalBuyAlerts != 1 || st.WarmupAlerts != 1 || st.TotalSignals != 1 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	if got := s.GetAlerts(10, false); len(got) != 1 || got[0].Symbol != "A.FR" {
		t.Fatalf("expected only the real alert, got %+v", got)
	}
	if got := s.GetAlerts(10, true); len(got) != 2 {
		t.Fatalf("expected 2 alerts including warm-up, got %d", len(got))
	}

	// Repeated window replaces the stored point instead of duplicating it.
	ind.Close = 11
	s.AddIndicator(ind)
	if h := s.GetHistory("A.FR", 0); len(h) != 1 || h[0].Close != 11 {
		t.Fatalf("expected 1 replaced point, got %+v", h)
	}

	m := s.GetMarkets(0)
	if len(m) != 1 || m[0].Symbol != "A.FR" || m[0].LastSignal != "BUY" || m[0].Alerts != 1 {
		t.Fatalf("unexpected markets: %+v", m)
	}
}
