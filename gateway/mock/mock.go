package mock

import (
	"context"
	"log"
	"math"
	"math/rand"
	"time"

	"trading-trends/gateway/model"
	"trading-trends/gateway/server"
)

type instrumentState struct {
	symbol      string
	secType     string
	price       float64
	volatility  float64
	trend       float64
	ema38       float64
	ema100      float64
	prevEMA38   float64
	prevEMA100  float64
	windowIndex int64
}

// StartMockGenerator launches a background generator producing realistic window EMA events.
// It cycles through realistic market prices and triggers genuine EMA crossovers.
func StartMockGenerator(ctx context.Context, hub *server.Hub, interval time.Duration) {
	if interval <= 0 {
		interval = 1200 * time.Millisecond // fast simulated tumbling windows
	}

	instruments := []*instrumentState{
		{symbol: "RDSA.NL", secType: "E", price: 19.85, volatility: 0.15, trend: 0.05, ema38: 19.80, ema100: 19.75},
		{symbol: "AIR.FR", secType: "E", price: 112.50, volatility: 0.60, trend: -0.10, ema38: 112.00, ema100: 112.20},
		{symbol: "BMW.ETR", secType: "E", price: 89.20, volatility: 0.45, trend: 0.08, ema38: 88.90, ema100: 88.70},
		{symbol: "MC.FR", secType: "E", price: 715.00, volatility: 2.50, trend: -0.20, ema38: 718.00, ema100: 716.50},
		{symbol: "ASML.NL", secType: "E", price: 680.00, volatility: 3.00, trend: 0.30, ema38: 678.00, ema100: 675.00},
		{symbol: "SAN.FR", secType: "E", price: 88.40, volatility: 0.35, trend: -0.05, ema38: 88.60, ema100: 88.50},
	}

	// Initialize previous EMA values
	for _, inst := range instruments {
		inst.prevEMA38 = inst.ema38
		inst.prevEMA100 = inst.ema100
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("[Mock Generator] Started simulating market windows (interval: %v for %d symbols)",
		interval, len(instruments))

	// Pre-fill some history for each symbol
	baseTime := time.Now().UTC().Add(-time.Duration(len(instruments)*50) * 5 * time.Minute)
	for step := 0; step < 40; step++ {
		for _, inst := range instruments {
			inst.windowIndex++
			wStart := baseTime.Add(time.Duration(inst.windowIndex*5) * time.Minute)
			wEnd := wStart.Add(5 * time.Minute)

			simulateWindowStep(inst)
			event := computeEvent(inst, wStart, wEnd)
			hub.Broadcast(event)
		}
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("[Mock Generator] Stopping...")
			return
		case now := <-ticker.C:
			// Pick 1 or 2 instruments per tick to simulate asynchronous tumbling window completion
			for i := 0; i < 2; i++ {
				idx := rand.Intn(len(instruments))
				inst := instruments[idx]
				inst.windowIndex++

				wEnd := now.UTC()
				wStart := wEnd.Add(-5 * time.Minute)

				simulateWindowStep(inst)
				event := computeEvent(inst, wStart, wEnd)
				hub.Broadcast(event)
			}
		}
	}
}

func simulateWindowStep(inst *instrumentState) {
	// Reverse trend periodically to create smooth cycles and crossovers
	if inst.windowIndex%25 == 0 {
		inst.trend = -inst.trend
	}

	// Geometric random walk with mean-reverting trend
	delta := inst.trend + (rand.Float64()*2-1)*inst.volatility
	inst.price += delta
	if inst.price < 1.0 {
		inst.price = 1.0
	}
	inst.price = math.Round(inst.price*1000) / 1000

	// Save previous EMAs
	inst.prevEMA38 = inst.ema38
	inst.prevEMA100 = inst.ema100

	// EMA smoothing formulas according to Query 1
	// EMA_j = Close * (2 / (1 + j)) + EMA_prev * (1 - (2 / (1 + j)))
	alpha38 := 2.0 / (1.0 + 38.0)
	alpha100 := 2.0 / (1.0 + 100.0)

	inst.ema38 = inst.price*alpha38 + inst.prevEMA38*(1.0-alpha38)
	inst.ema100 = inst.price*alpha100 + inst.prevEMA100*(1.0-alpha100)

	inst.ema38 = math.Round(inst.ema38*1000) / 1000
	inst.ema100 = math.Round(inst.ema100*1000) / 1000
}

func computeEvent(inst *instrumentState, wStart, wEnd time.Time) model.SignalEvent {
	// Query 2 Breakout Crossover Rules
	signal := ""

	// Bullish breakout: EMA38 > EMA100 and prevEMA38 <= prevEMA100
	if inst.ema38 > inst.ema100 && inst.prevEMA38 <= inst.prevEMA100 {
		signal = "BUY"
	} else if inst.ema38 < inst.ema100 && inst.prevEMA38 >= inst.prevEMA100 {
		// Bearish breakout: EMA38 < EMA100 and prevEMA38 >= prevEMA100
		signal = "SELL"
	}

	return model.SignalEvent{
		Symbol:      inst.symbol,
		SecType:     inst.secType,
		WindowStart: wStart,
		WindowEnd:   wEnd,
		WindowIndex: inst.windowIndex,
		Close:       inst.price,
		EMA38:       inst.ema38,
		EMA100:      inst.ema100,
		Signal:      signal,
		TradeCount:  rand.Intn(300) + 50,
		Timestamp:   wEnd,
	}
}
