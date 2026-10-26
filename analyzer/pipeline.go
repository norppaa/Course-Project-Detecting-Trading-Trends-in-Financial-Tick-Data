package main

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"trading-trends/analyzer/engine"
	"trading-trends/pkg/models"
)

// This file is the broker-independent part of the online analyzer: one
// worker goroutine per input partition, each owning its own engine.Engine.
// A symbol always lives in exactly one partition (the producer keys by
// symbol), so workers share no state and need no locks on the hot path.
// main.go connects workers to Kafka; the tests drive them directly.

// inMsg is one input message handed to a worker.
type inMsg struct {
	kind       string    // models.KindTick / KindHeartbeat / KindEndOfStream, or kindIdleFlush
	value      []byte    // raw JSON payload
	producedAt time.Time // from the produced_at header, zero if absent
	receivedAt time.Time // wall clock when the analyzer read the message
}

// kindIdleFlush is internal: sent by the idle detector, never on the wire.
const kindIdleFlush = "idle-flush"

// outMsg is one result to publish.
type outMsg struct {
	topic string
	key   string
	kind  string // models.KindIndicator or models.KindSignal
	value []byte
}

// Sink publishes results. Implementations must be safe for concurrent use
// by several workers and must not keep the slice after returning.
type Sink interface {
	Send(msgs []outMsg) error
}

// workerStats is a snapshot of one worker's counters.
type workerStats struct {
	Messages     int64
	Ticks        int64
	Heartbeats   int64
	EOS          int64
	IdleFlushes  int64
	BadMessages  int64 // undecodable or invalid payload, unknown kind
	SinkErrors   int64
	Engine       engine.Stats
	LatencySumUs int64 // sum over emitted batches of (emitted - received)
	LatencyMaxUs int64
	LatencyCount int64
}

type worker struct {
	partition int
	cfg       *Config
	sink      Sink
	in        chan inMsg

	eng     *engine.Engine
	results []engine.WindowResult // filled by the engine callback
	out     []outMsg

	st   workerStats // owned by the worker goroutine
	mu   sync.Mutex
	snap workerStats // published copy, read by the metrics logger
}

func newWorker(partition int, cfg *Config, sink Sink) *worker {
	w := &worker{
		partition: partition,
		cfg:       cfg,
		sink:      sink,
		in:        make(chan inMsg, cfg.WorkerBuffer),
	}
	w.eng = engine.New(cfg.Engine, func(r engine.WindowResult) {
		w.results = append(w.results, r)
	})
	return w
}

// run processes messages until w.in is closed.
func (w *worker) run() {
	for m := range w.in {
		w.handle(m)
		if w.st.Messages%1024 == 0 || len(w.in) == 0 {
			w.publish()
		}
	}
	w.publish()
}

func (w *worker) handle(m inMsg) {
	if m.kind != kindIdleFlush {
		w.st.Messages++
	}
	switch m.kind {
	case "", models.KindTick:
		var t models.MarketTick
		if err := json.Unmarshal(m.value, &t); err != nil || t.ID == "" || t.Timestamp.IsZero() || !(t.Price > 0) {
			w.st.BadMessages++
			return
		}
		w.st.Ticks++
		var secType byte
		if t.SecType != "" {
			secType = t.SecType[0]
		}
		w.eng.Process(engine.Tick{
			Symbol:    t.ID,
			SecType:   secType,
			Price:     t.Price,
			EventTime: t.Timestamp.UnixMicro(),
		})
	case models.KindHeartbeat:
		var hb models.Heartbeat
		if err := json.Unmarshal(m.value, &hb); err != nil || hb.EventTime.IsZero() {
			w.st.BadMessages++
			return
		}
		w.st.Heartbeats++
		w.eng.AdvanceWatermark(hb.EventTime.UnixMicro() - w.cfg.Engine.Grace)
	case models.KindEndOfStream:
		w.st.EOS++
		w.eng.Flush()
	case kindIdleFlush:
		w.st.IdleFlushes++
		w.eng.Flush()
	default:
		w.st.BadMessages++
		return
	}
	if len(w.results) > 0 {
		w.emit(m)
	}
}

// emit converts the windows closed by message m into output messages.
func (w *worker) emit(trigger inMsg) {
	now := time.Now().UTC()
	timing := models.Timing{
		ProducedAt: trigger.producedAt,
		IngestedAt: trigger.receivedAt,
		EmittedAt:  now,
	}
	w.out = w.out[:0]
	for i := range w.results {
		r := &w.results[i]
		secType := ""
		if r.SecType != 0 {
			secType = string(r.SecType)
		}
		start := time.UnixMicro(r.WindowStart).UTC()
		end := time.UnixMicro(r.WindowEnd).UTC()

		if w.cfg.IndicatorTopic != "" {
			ind := models.WindowIndicator{
				Kind:        models.KindIndicator,
				Symbol:      r.Symbol,
				SecType:     secType,
				WindowStart: start,
				WindowEnd:   end,
				LastPrice:   r.Close,
				HasTick:     r.HasTick,
				EMA38:       r.EMA38,
				EMA100:      r.EMA100,
				Timing:      timing,
			}
			if r.Signal != engine.None {
				ind.Signal = r.Signal.String()
			}
			w.add(w.cfg.IndicatorTopic, r.Symbol, models.KindIndicator, ind)
		}
		if r.Signal != engine.None {
			sig := models.TradeSignal{
				Kind:        models.KindSignal,
				Symbol:      r.Symbol,
				SecType:     secType,
				Type:        models.SignalType(r.Signal.String()),
				WindowStart: start,
				Timestamp:   end,
				Price:       r.Close,
				EMA38:       r.EMA38,
				EMA100:      r.EMA100,
				PrevEMA38:   r.PrevEMA38,
				PrevEMA100:  r.PrevEMA100,
				Timing:      timing,
			}
			w.add(w.cfg.SignalTopic, r.Symbol, models.KindSignal, sig)
		}
	}
	w.results = w.results[:0]

	if len(w.out) > 0 {
		if err := w.sink.Send(w.out); err != nil {
			w.st.SinkErrors += int64(len(w.out))
		}
	}
	if !trigger.receivedAt.IsZero() {
		lat := now.Sub(trigger.receivedAt).Microseconds()
		w.st.LatencySumUs += lat
		w.st.LatencyCount++
		if lat > w.st.LatencyMaxUs {
			w.st.LatencyMaxUs = lat
		}
	}
}

func (w *worker) add(topic, key, kind string, v any) {
	b, err := json.Marshal(v)
	if err != nil { // cannot happen for these types
		w.st.BadMessages++
		return
	}
	w.out = append(w.out, outMsg{topic: topic, key: key, kind: kind, value: b})
}

func (w *worker) publish() {
	w.st.Engine = w.eng.Stats()
	w.mu.Lock()
	w.snap = w.st
	w.mu.Unlock()
}

func (w *worker) stats() workerStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snap
}

// dispatcher owns the workers and routes messages to them by partition.
type dispatcher struct {
	cfg  *Config
	sink Sink

	mu      sync.Mutex
	workers map[int]*worker
	wg      sync.WaitGroup

	received   atomic.Int64 // messages accepted from the broker
	firstInput atomic.Int64 // unix nanos of the first message
	lastInput  atomic.Int64 // unix nanos of the latest message
}

func newDispatcher(cfg *Config, sink Sink) *dispatcher {
	return &dispatcher{cfg: cfg, sink: sink, workers: make(map[int]*worker)}
}

// worker returns the worker for a partition, starting it on first use.
func (d *dispatcher) worker(partition int) *worker {
	d.mu.Lock()
	defer d.mu.Unlock()
	w := d.workers[partition]
	if w == nil {
		w = newWorker(partition, d.cfg, d.sink)
		d.workers[partition] = w
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			w.run()
		}()
	}
	return w
}

// dispatch hands m to the partition's worker. It blocks if the worker's
// queue is full, which applies back-pressure to the broker reader.
func (d *dispatcher) dispatch(partition int, m inMsg) {
	d.received.Add(1)
	d.firstInput.CompareAndSwap(0, m.receivedAt.UnixNano())
	d.lastInput.Store(m.receivedAt.UnixNano())
	d.worker(partition).in <- m
}

// idleFlush closes all open windows in every worker. Used when the input has
// been silent for a while, i.e. the replay is over and no end-of-stream
// marker arrived.
func (d *dispatcher) idleFlush() {
	for _, w := range d.all() {
		w.in <- inMsg{kind: kindIdleFlush}
	}
}

func (d *dispatcher) all() []*worker {
	d.mu.Lock()
	defer d.mu.Unlock()
	ws := make([]*worker, 0, len(d.workers))
	for _, w := range d.workers {
		ws = append(ws, w)
	}
	return ws
}

// close stops all workers after they have drained their queues.
// No dispatch may happen during or after close.
func (d *dispatcher) close() {
	for _, w := range d.all() {
		close(w.in)
	}
	d.wg.Wait()
}

// idleWatcher calls d.idleFlush once whenever no message has arrived for
// `after`, and again only after new input has arrived.
type idleWatcher struct {
	d          *dispatcher
	after      time.Duration
	flushedAt  int64 // value of d.received at the last flush
	hasFlushed bool
}

func (iw *idleWatcher) check(now time.Time) bool {
	if iw.after <= 0 {
		return false
	}
	n := iw.d.received.Load()
	if n == 0 || (iw.hasFlushed && n == iw.flushedAt) {
		return false
	}
	if now.Sub(time.Unix(0, iw.d.lastInput.Load())) < iw.after {
		return false
	}
	iw.d.idleFlush()
	iw.flushedAt, iw.hasFlushed = n, true
	return true
}
