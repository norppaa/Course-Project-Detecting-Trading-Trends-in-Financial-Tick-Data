// Package engine implements Query 1 (EMA38/EMA100 over 5-minute event-time
// tumbling windows) and Query 2 (bullish/bearish crossover advisories) as
// pure logic with no I/O. The Redpanda consumer feeds it ticks and
// watermarks; it hands back closed-window results through a callback.
//
// One Engine instance per partition: a symbol always lives in exactly one
// partition, so an Engine never needs locks.
package engine

import (
	"sort"
)

// EMA smoothing factors from the spec: alpha_j = 2 / (1 + j).
const (
	Alpha38  = 2.0 / (1.0 + 38.0)
	Alpha100 = 2.0 / (1.0 + 100.0)
)

// Signal is the advisory produced for a window by Query 2.
type Signal int8

const (
	None Signal = iota
	Buy         // bullish breakout, Eq. 1 & 2
	Sell        // bearish breakout, Eq. 3 & 4
)

func (s Signal) String() string {
	switch s {
	case Buy:
		return "BUY"
	case Sell:
		return "SELL"
	default:
		return "NONE"
	}
}

// EmptyWindowPolicy decides what happens for a symbol in a window with no
// price event. The team must pick one and the ground truth must match it.
type EmptyWindowPolicy int8

const (
	// SkipEmpty evaluates a symbol only in windows where it had a price
	// event. "Previous window" in Eq. 2 and 4 means the symbol's previous
	// evaluated window. EMAs are effectively frozen across gaps.
	SkipEmpty EmptyWindowPolicy = iota
	// CarryLastClose evaluates every window after a symbol's first price,
	// reusing the last known close when a window is empty. EMAs keep
	// converging toward the last price across gaps (nights, weekend), and a
	// crossover can fire in a window with no trades.
	CarryLastClose
)

// Config controls the engine.
type Config struct {
	WindowSize int64 // microseconds; default 5 minutes
	Grace      int64 // allowed out-of-orderness, microseconds; default 0
	Policy     EmptyWindowPolicy
	// SuppressFirstSignal drops the advisory in a symbol's first evaluated
	// window. With EMA_{s,w0} = 0 for both EMAs, the first window always
	// satisfies Eq. 1 & 2 (EMA38 = 0.051p > EMA100 = 0.020p, previous
	// 0 <= 0), so every symbol would otherwise get a BUY on its first trade.
	SuppressFirstSignal bool
}

// Tick is one price event (a row with non-null Last).
type Tick struct {
	Symbol    string
	SecType   byte
	Price     float64
	EventTime int64 // µs, see timeparse.go
}

// WindowResult is emitted for each (symbol, window) that is evaluated.
type WindowResult struct {
	Symbol      string
	SecType     byte
	WindowStart int64 // µs, inclusive
	WindowEnd   int64 // µs, exclusive
	Close       float64
	HasTick     bool // false if Close was carried forward (CarryLastClose)
	EMA38       float64
	EMA100      float64
	Signal      Signal
}

// Stats are counters for the report and for debugging.
type Stats struct {
	TicksAccepted   int64
	TicksLate       int64 // dropped: their window had already closed
	WindowsEmitted  int64
	BuySignals      int64
	SellSignals     int64
	Watermark       int64
	ClosedBeforeIdx int64 // all windows with index < this are closed
}

// Detect applies the crossover rules (Eq. 1-4) to the EMAs of the previous
// and current window.
func Detect(prev38, prev100, cur38, cur100 float64) Signal {
	switch {
	case cur38 > cur100 && prev38 <= prev100:
		return Buy
	case cur38 < cur100 && prev38 >= prev100:
		return Sell
	default:
		return None
	}
}

type pendingWin struct {
	idx   int64
	ts    int64 // event time of the price currently held as Close
	price float64
}

type symbolState struct {
	symbol    string
	secType   byte
	pending   []pendingWin // open windows, sorted by idx ascending
	evaluated bool         // has at least one evaluated window
	lastIdx   int64
	lastClose float64
	ema38     float64
	ema100    float64
}

// Engine holds per-symbol state for one partition.
type Engine struct {
	cfg     Config
	emit    func(WindowResult)
	symbols map[string]*symbolState
	dirty   map[string]*symbolState // symbols with pending windows

	maxEventTime int64
	watermark    int64
	closedBefore int64 // window indices < closedBefore are closed

	buf   []WindowResult
	stats Stats
}

// New creates an engine. emit is called for every evaluated window, in
// order of (WindowStart, Symbol) within each batch of closed windows.
func New(cfg Config, emit func(WindowResult)) *Engine {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 5 * Minute
	}
	if cfg.Grace < 0 {
		cfg.Grace = 0
	}
	if emit == nil {
		emit = func(WindowResult) {}
	}
	return &Engine{
		cfg:     cfg,
		emit:    emit,
		symbols: make(map[string]*symbolState),
		dirty:   make(map[string]*symbolState),
	}
}

func (e *Engine) windowIndex(ts int64) int64 { return ts / e.cfg.WindowSize }

// Process ingests one tick. It may close windows (and call emit) if the
// tick advances the watermark.
func (e *Engine) Process(t Tick) {
	idx := e.windowIndex(t.EventTime)
	if idx < e.closedBefore {
		e.stats.TicksLate++
		return
	}
	e.stats.TicksAccepted++

	st := e.symbols[t.Symbol]
	if st == nil {
		st = &symbolState{symbol: t.Symbol, secType: t.SecType}
		e.symbols[t.Symbol] = st
	}
	st.addPrice(idx, t.EventTime, t.Price)
	e.dirty[t.Symbol] = st

	if t.EventTime > e.maxEventTime {
		e.maxEventTime = t.EventTime
		e.AdvanceWatermark(t.EventTime - e.cfg.Grace)
	}
}

// AdvanceWatermark tells the engine that no more events with event time
// below wm are expected (beyond those already accepted). Call it for
// heartbeat messages so windows of quiet partitions still close.
// Watermarks never move backwards.
func (e *Engine) AdvanceWatermark(wm int64) {
	if wm <= e.watermark {
		return
	}
	e.watermark = wm
	if limit := e.windowIndex(wm); limit > e.closedBefore {
		e.closeBefore(limit)
	}
}

// Flush closes every window up to and including the one holding the latest
// event seen. Call it at the end of a bounded input (e.g. a replayed day).
func (e *Engine) Flush() {
	if e.maxEventTime == 0 && len(e.dirty) == 0 {
		return
	}
	limit := e.windowIndex(e.maxEventTime) + 1
	if limit > e.closedBefore {
		e.closeBefore(limit)
	}
	if e.watermark < limit*e.cfg.WindowSize {
		e.watermark = limit * e.cfg.WindowSize
	}
}

// Stats returns a snapshot of the counters.
func (e *Engine) Stats() Stats {
	s := e.stats
	s.Watermark = e.watermark
	s.ClosedBeforeIdx = e.closedBefore
	return s
}

func (st *symbolState) addPrice(idx, ts int64, price float64) {
	// Usually the window is the last pending one; scan from the back.
	for i := len(st.pending) - 1; i >= 0; i-- {
		p := &st.pending[i]
		if p.idx == idx {
			// Close = last price by event time. Ties go to the later arrival.
			if ts >= p.ts {
				p.ts, p.price = ts, price
			}
			return
		}
		if p.idx < idx {
			st.pending = append(st.pending, pendingWin{})
			copy(st.pending[i+2:], st.pending[i+1:])
			st.pending[i+1] = pendingWin{idx: idx, ts: ts, price: price}
			return
		}
	}
	st.pending = append(st.pending, pendingWin{})
	copy(st.pending[1:], st.pending)
	st.pending[0] = pendingWin{idx: idx, ts: ts, price: price}
}

// closeBefore evaluates all windows with index < limit and emits results.
func (e *Engine) closeBefore(limit int64) {
	e.buf = e.buf[:0]
	switch e.cfg.Policy {
	case CarryLastClose:
		for _, st := range e.symbols {
			e.closeCarry(st, limit)
		}
	default:
		for _, st := range e.dirty {
			e.closeSkip(st, limit)
		}
	}
	for sym, st := range e.dirty {
		if len(st.pending) == 0 {
			delete(e.dirty, sym)
		}
	}
	e.closedBefore = limit

	sort.Slice(e.buf, func(i, j int) bool {
		a, b := &e.buf[i], &e.buf[j]
		if a.WindowStart != b.WindowStart {
			return a.WindowStart < b.WindowStart
		}
		return a.Symbol < b.Symbol
	})
	for i := range e.buf {
		e.emit(e.buf[i])
	}
	e.stats.WindowsEmitted += int64(len(e.buf))
}

func (e *Engine) closeSkip(st *symbolState, limit int64) {
	n := 0
	for n < len(st.pending) && st.pending[n].idx < limit {
		p := st.pending[n]
		e.evaluate(st, p.idx, p.price, true)
		n++
	}
	st.pending = st.pending[n:]
}

func (e *Engine) closeCarry(st *symbolState, limit int64) {
	var next int64
	switch {
	case st.evaluated:
		next = st.lastIdx + 1
	case len(st.pending) > 0:
		next = st.pending[0].idx
	default:
		return
	}
	n := 0
	for w := next; w < limit; w++ {
		if n < len(st.pending) && st.pending[n].idx == w {
			e.evaluate(st, w, st.pending[n].price, true)
			n++
		} else {
			e.evaluate(st, w, st.lastClose, false)
		}
	}
	st.pending = st.pending[n:]
}

func (e *Engine) evaluate(st *symbolState, idx int64, close float64, hasTick bool) {
	first := !st.evaluated
	prev38, prev100 := st.ema38, st.ema100 // both 0 before the first window

	st.ema38 = close*Alpha38 + prev38*(1-Alpha38)
	st.ema100 = close*Alpha100 + prev100*(1-Alpha100)

	sig := Detect(prev38, prev100, st.ema38, st.ema100)
	if first && e.cfg.SuppressFirstSignal {
		sig = None
	}
	switch sig {
	case Buy:
		e.stats.BuySignals++
	case Sell:
		e.stats.SellSignals++
	}

	st.evaluated = true
	st.lastIdx = idx
	st.lastClose = close

	e.buf = append(e.buf, WindowResult{
		Symbol:      st.symbol,
		SecType:     st.secType,
		WindowStart: idx * e.cfg.WindowSize,
		WindowEnd:   (idx + 1) * e.cfg.WindowSize,
		Close:       close,
		HasTick:     hasTick,
		EMA38:       st.ema38,
		EMA100:      st.ema100,
		Signal:      sig,
	})
}
