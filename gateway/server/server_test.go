package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"trading-trends/gateway/model"
	"trading-trends/gateway/store"
)

func TestServerEndpoints(t *testing.T) {
	st := store.NewMemoryStore(100, 50)
	hub := NewHub(st)
	go hub.Run()

	s := NewServer(hub, st)
	handler := s.Handler()

	// Seed some test data
	st.AddSignal(model.SignalEvent{
		Symbol:    "RDSA.NL",
		Close:     19.85,
		EMA38:     19.80,
		EMA100:    19.70,
		Signal:    "BUY",
		Timestamp: time.Now().UTC(),
	})

	// 1. Test /health
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /health, got %d", w.Code)
	}

	// 2. Test /api/symbols
	req = httptest.NewRequest(http.MethodGet, "/api/symbols", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /api/symbols, got %d", w.Code)
	}
	var symbols []string
	if err := json.Unmarshal(w.Body.Bytes(), &symbols); err != nil {
		t.Fatalf("Failed to parse symbols JSON: %v", err)
	}
	if len(symbols) != 1 || symbols[0] != "RDSA.NL" {
		t.Fatalf("Expected ['RDSA.NL'], got %v", symbols)
	}

	// 3. Test /api/history?symbol=RDSA.NL
	req = httptest.NewRequest(http.MethodGet, "/api/history?symbol=RDSA.NL", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /api/history, got %d", w.Code)
	}
	var history []model.SignalEvent
	if err := json.Unmarshal(w.Body.Bytes(), &history); err != nil {
		t.Fatalf("Failed to parse history JSON: %v", err)
	}
	if len(history) != 1 || history[0].Close != 19.85 {
		t.Fatalf("Expected history with close 19.85, got %v", history)
	}

	// 4. Test /api/alerts
	req = httptest.NewRequest(http.MethodGet, "/api/alerts", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /api/alerts, got %d", w.Code)
	}
	var alerts []model.SignalEvent
	if err := json.Unmarshal(w.Body.Bytes(), &alerts); err != nil {
		t.Fatalf("Failed to parse alerts JSON: %v", err)
	}
	if len(alerts) != 1 || alerts[0].Signal != "BUY" {
		t.Fatalf("Expected 1 BUY alert, got %v", alerts)
	}

	// 5. Test /api/stats
	req = httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK for /api/stats, got %d", w.Code)
	}
}

