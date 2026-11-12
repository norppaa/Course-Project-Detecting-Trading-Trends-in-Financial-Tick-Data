package model

import (
	"encoding/json"
	"testing"
	"time"

	"trading-trends/analyzer/engine"
	"trading-trends/pkg/models"
)

// These tests encode messages with the analyzer's own output types so the
// gateway is checked against the exact wire contract of "trading-signals".

func TestDecodeAnalyzerIndicator(t *testing.T) {
	start := time.Date(2021, 11, 8, 9, 0, 0, 0, time.UTC)
	produced := time.Now().UTC().Add(-50 * time.Millisecond)
	in := models.WindowIndicator{
		Kind:        models.KindIndicator,
		Symbol:      "ALVG.ETR",
		SecType:     "E",
		WindowStart: start,
		WindowEnd:   start.Add(5 * time.Minute),
		LastPrice:   241.35,
		HasTick:     true,
		EMA38:       240.9,
		EMA100:      239.7,
		Timing:      models.Timing{ProducedAt: produced, IngestedAt: produced, EmittedAt: produced},
	}
	b, _ := json.Marshal(in)

	var ev SignalEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Kind != KindIndicator || ev.Symbol != "ALVG.ETR" || ev.Close != 241.35 ||
		ev.EMA38 != 240.9 || ev.EMA100 != 239.7 || ev.Signal != "" || !ev.HasTick {
		t.Fatalf("unexpected decode: %+v", ev)
	}
	if !ev.WindowEnd.Equal(in.WindowEnd) || !ev.Timestamp.Equal(in.WindowEnd) {
		t.Fatalf("window end / timestamp mismatch: %+v", ev)
	}
	if want := start.Unix() / 300; ev.WindowIndex != want {
		t.Fatalf("window index = %d, want %d", ev.WindowIndex, want)
	}
	if ev.FirstWindow {
		t.Fatal("converged EMAs must not be flagged as first window")
	}
	if !ev.ProducedAt.Equal(produced) {
		t.Fatalf("produced_at not decoded: %v", ev.ProducedAt)
	}
}

func TestDecodeFirstWindowIndicatorAndSignal(t *testing.T) {
	start := time.Date(2021, 11, 8, 7, 0, 0, 0, time.UTC)
	price := 52.69
	// First evaluated window: previous EMAs are 0, exactly as in the engine.
	ema38 := price*engine.Alpha38 + 0*(1-engine.Alpha38)
	ema100 := price*engine.Alpha100 + 0*(1-engine.Alpha100)

	ind, _ := json.Marshal(models.WindowIndicator{
		Kind: models.KindIndicator, Symbol: "IEBBB.FR", SecType: "I",
		WindowStart: start, WindowEnd: start.Add(5 * time.Minute),
		LastPrice: price, HasTick: true, EMA38: ema38, EMA100: ema100, Signal: "BUY",
	})
	sig, _ := json.Marshal(models.TradeSignal{
		Kind: models.KindSignal, Symbol: "IEBBB.FR", SecType: "I", Type: models.SignalBuy,
		WindowStart: start, Timestamp: start.Add(5 * time.Minute),
		Price: price, EMA38: ema38, EMA100: ema100,
	})

	var a, b SignalEvent
	if err := json.Unmarshal(ind, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(sig, &b); err != nil {
		t.Fatal(err)
	}
	if !a.FirstWindow || !b.FirstWindow {
		t.Fatalf("first window not detected: indicator=%v signal=%v", a.FirstWindow, b.FirstWindow)
	}
	if a.Signal != "BUY" || b.Signal != "BUY" || b.Kind != KindSignal || b.Close != price {
		t.Fatalf("unexpected decode: %+v / %+v", a, b)
	}
	// Both messages describe the same window -> same dedupe key.
	if !a.WindowEnd.Equal(b.WindowEnd) || !a.WindowStart.Equal(b.WindowStart) {
		t.Fatalf("indicator and signal windows differ: %v-%v vs %v-%v",
			a.WindowStart, a.WindowEnd, b.WindowStart, b.WindowEnd)
	}
}

func TestDecodeRealCrossoverSignal(t *testing.T) {
	start := time.Date(2021, 11, 8, 12, 0, 0, 0, time.UTC)
	b, _ := json.Marshal(models.TradeSignal{
		Kind: models.KindSignal, Symbol: "AIR.FR", SecType: "E", Type: models.SignalSell,
		WindowStart: start, Timestamp: start.Add(5 * time.Minute),
		Price: 112.4, EMA38: 111.9, EMA100: 113.1, PrevEMA38: 113.5, PrevEMA100: 113.0,
	})
	var ev SignalEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.FirstWindow || ev.Signal != "SELL" || ev.Kind != KindSignal {
		t.Fatalf("unexpected decode: %+v", ev)
	}
}
