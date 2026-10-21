package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"trading-trends/analyzer/engine"
	"trading-trends/pkg/models"
)

// memSink records everything the workers publish.
type memSink struct {
	mu   sync.Mutex
	msgs []outMsg
}

func (s *memSink) Send(msgs []outMsg) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, msgs...)
	return nil
}

func (s *memSink) byKind(kind string) []outMsg {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []outMsg
	for _, m := range s.msgs {
		if m.kind == kind {
			out = append(out, m)
		}
	}
	return out
}

func testConfig() *Config {
	return &Config{
		IndicatorTopic: "ind",
		SignalTopic:    "sig",
		WorkerBuffer:   16,
		Engine:         engine.Config{WindowSize: 5 * engine.Minute, Grace: engine.Second},
	}
}

var day = time.Date(2021, 11, 8, 0, 0, 0, 0, time.UTC)

// at returns 08-11-2021 hh:mm:ss as event time.
func at(h, m, s int) time.Time {
	return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second)
}

func tickMsg(t *testing.T, sym string, price float64, ts time.Time) inMsg {
	t.Helper()
	b, err := json.Marshal(models.MarketTick{ID: sym, SecType: "E", Timestamp: ts, Price: price})
	if err != nil {
		t.Fatal(err)
	}
	return inMsg{kind: models.KindTick, value: b, receivedAt: time.Now().UTC()}
}

func heartbeatMsg(t *testing.T, ts time.Time) inMsg {
	b, _ := json.Marshal(models.Heartbeat{EventTime: ts})
	return inMsg{kind: models.KindHeartbeat, value: b, receivedAt: time.Now().UTC()}
}

func decodeIndicators(t *testing.T, msgs []outMsg) []models.WindowIndicator {
	t.Helper()
	out := make([]models.WindowIndicator, len(msgs))
	for i, m := range msgs {
		if m.topic != "ind" {
			t.Errorf("indicator on topic %q", m.topic)
		}
		if err := json.Unmarshal(m.value, &out[i]); err != nil {
			t.Fatal(err)
		}
		if out[i].Symbol != m.key {
			t.Errorf("key %q for symbol %q", m.key, out[i].Symbol)
		}
	}
	return out
}

func TestWorkerEmitsIndicatorsAndSignals(t *testing.T) {
	sink := &memSink{}
	w := newWorker(0, testConfig(), sink)

	w.handle(tickMsg(t, "A.FR", 10, at(9, 0, 10)))
	w.handle(tickMsg(t, "A.FR", 11, at(9, 4, 0))) // close of window 09:00
	if n := len(sink.msgs); n != 0 {
		t.Fatalf("window emitted before the next one started: %d msgs", n)
	}
	// 09:05:02 - 1 s grace passes 09:05 -> window 09:00 closes.
	w.handle(tickMsg(t, "A.FR", 12, at(9, 5, 2)))

	inds := decodeIndicators(t, sink.byKind(models.KindIndicator))
	if len(inds) != 1 {
		t.Fatalf("got %d indicators, want 1", len(inds))
	}
	got := inds[0]
	if got.Kind != models.KindIndicator || got.SecType != "E" || !got.HasTick {
		t.Errorf("bad fields: %+v", got)
	}
	if !got.WindowStart.Equal(at(9, 0, 0)) || !got.WindowEnd.Equal(at(9, 5, 0)) {
		t.Errorf("window %v - %v", got.WindowStart, got.WindowEnd)
	}
	if got.LastPrice != 11 || !near(got.EMA38, 11*2.0/39) || !near(got.EMA100, 11*2.0/101) {
		t.Errorf("close/EMA: %+v", got)
	}
	if got.IngestedAt.IsZero() || got.EmittedAt.Before(got.IngestedAt) {
		t.Errorf("timing: %+v", got.Timing)
	}

	// First window: EMA38 > EMA100 after both were 0 -> BUY (not suppressed).
	sigs := sink.byKind(models.KindSignal)
	if len(sigs) != 1 || sigs[0].topic != "sig" {
		t.Fatalf("signals: %+v", sigs)
	}
	var s models.TradeSignal
	if err := json.Unmarshal(sigs[0].value, &s); err != nil {
		t.Fatal(err)
	}
	if s.Type != models.SignalBuy || s.PrevEMA38 != 0 || s.PrevEMA100 != 0 ||
		!s.Timestamp.Equal(at(9, 5, 0)) || !s.WindowStart.Equal(at(9, 0, 0)) || got.Signal != "BUY" {
		t.Errorf("signal: %+v (indicator signal %q)", s, got.Signal)
	}
}

func TestHeartbeatClosesWindowWithoutNewTicks(t *testing.T) {
	sink := &memSink{}
	w := newWorker(0, testConfig(), sink)
	w.handle(tickMsg(t, "A.FR", 10, at(9, 0, 10)))
	w.handle(heartbeatMsg(t, at(9, 5, 0))) // within grace: still open
	if len(sink.msgs) != 0 {
		t.Fatal("heartbeat ignored the grace period")
	}
	w.handle(heartbeatMsg(t, at(9, 5, 1)))
	if n := len(sink.byKind(models.KindIndicator)); n != 1 {
		t.Fatalf("got %d indicators after heartbeat, want 1", n)
	}
	if w.st.Heartbeats != 2 {
		t.Errorf("heartbeats counted: %d", w.st.Heartbeats)
	}
}

func TestEndOfStreamFlushes(t *testing.T) {
	sink := &memSink{}
	w := newWorker(0, testConfig(), sink)
	w.handle(tickMsg(t, "A.FR", 10, at(9, 0, 10)))
	w.handle(tickMsg(t, "B.NL", 20, at(9, 1, 0)))
	w.handle(inMsg{kind: models.KindEndOfStream})
	inds := decodeIndicators(t, sink.byKind(models.KindIndicator))
	if len(inds) != 2 || inds[0].Symbol != "A.FR" || inds[1].Symbol != "B.NL" {
		t.Fatalf("after EOS: %+v", inds)
	}
}

func TestBadMessagesAreCountedNotFatal(t *testing.T) {
	sink := &memSink{}
	w := newWorker(0, testConfig(), sink)
	w.handle(inMsg{kind: models.KindTick, value: []byte("{not json")})
	w.handle(inMsg{value: []byte(`{"id":"A.FR","price":0,"timestamp":"2021-11-08T09:00:00Z"}`)})
	w.handle(inMsg{value: []byte(`{"id":"","price":5,"timestamp":"2021-11-08T09:00:00Z"}`)})
	w.handle(inMsg{kind: models.KindHeartbeat, value: []byte(`{}`)})
	w.handle(inMsg{kind: "something-else"})
	if w.st.BadMessages != 5 || w.st.Ticks != 0 {
		t.Errorf("bad=%d ticks=%d", w.st.BadMessages, w.st.Ticks)
	}
	// A message without a kind header is a tick.
	w.handle(inMsg{value: []byte(`{"id":"A.FR","sec_type":"I","price":5,"timestamp":"2021-11-08T09:00:00Z"}`)})
	if w.st.Ticks != 1 {
		t.Errorf("tick without kind header not accepted")
	}
}

func TestSignalsOnlyWhenIndicatorsDisabled(t *testing.T) {
	cfg := testConfig()
	cfg.IndicatorTopic = ""
	sink := &memSink{}
	w := newWorker(0, cfg, sink)
	w.handle(tickMsg(t, "A.FR", 10, at(9, 0, 10)))
	w.handle(inMsg{kind: models.KindEndOfStream})
	if len(sink.msgs) != 1 || sink.msgs[0].kind != models.KindSignal {
		t.Fatalf("got %+v", sink.msgs)
	}
}

func TestProducedAtIsPassedThrough(t *testing.T) {
	sink := &memSink{}
	w := newWorker(0, testConfig(), sink)
	w.handle(tickMsg(t, "A.FR", 10, at(9, 0, 10)))
	m := tickMsg(t, "A.FR", 10, at(9, 6, 0))
	m.producedAt = time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	w.handle(m)
	inds := decodeIndicators(t, sink.byKind(models.KindIndicator))
	if len(inds) != 1 || !inds[0].ProducedAt.Equal(m.producedAt) {
		t.Fatalf("produced_at not passed through: %+v", inds)
	}
}

// Partitions have independent engines and watermarks: a busy partition must
// not close the windows of a quiet one.
func TestDispatcherKeepsPartitionsIndependent(t *testing.T) {
	sink := &memSink{}
	d := newDispatcher(testConfig(), sink)
	d.dispatch(0, tickMsg(t, "A.FR", 10, at(9, 0, 10)))
	d.dispatch(1, tickMsg(t, "B.NL", 20, at(9, 0, 20)))
	d.dispatch(0, tickMsg(t, "A.FR", 11, at(9, 30, 0))) // closes A's 09:00 only
	d.close()

	inds := decodeIndicators(t, sink.byKind(models.KindIndicator))
	if len(inds) != 1 || inds[0].Symbol != "A.FR" {
		t.Fatalf("got %+v", inds)
	}
	if len(d.all()) != 2 {
		t.Errorf("%d workers, want 2", len(d.all()))
	}
}

func TestIdleWatcherFlushesOncePerQuietPeriod(t *testing.T) {
	sink := &memSink{}
	d := newDispatcher(testConfig(), sink)
	iw := &idleWatcher{d: d, after: time.Second}

	if iw.check(time.Now().Add(time.Hour)) {
		t.Fatal("flushed before any input")
	}
	d.dispatch(0, tickMsg(t, "A.FR", 10, at(9, 0, 10)))
	now := time.Now()
	if iw.check(now) {
		t.Fatal("flushed while input is fresh")
	}
	if !iw.check(now.Add(2 * time.Second)) {
		t.Fatal("did not flush after idle period")
	}
	if iw.check(now.Add(10 * time.Second)) {
		t.Fatal("flushed twice without new input")
	}
	d.dispatch(0, tickMsg(t, "A.FR", 12, at(9, 20, 0)))
	if !iw.check(time.Now().Add(2 * time.Second)) {
		t.Fatal("did not flush after the second quiet period")
	}
	d.close()
	if n := len(sink.byKind(models.KindIndicator)); n != 2 {
		t.Fatalf("got %d indicators, want 2 (one per flush)", n)
	}
}

// The online worker must give exactly the engine's results for the same
// ticks, i.e. the JSON layer loses nothing.
func TestWorkerMatchesEngine(t *testing.T) {
	cfg := testConfig()
	sink := &memSink{}
	w := newWorker(0, cfg, sink)
	var want []engine.WindowResult
	ref := engine.New(cfg.Engine, func(r engine.WindowResult) { want = append(want, r) })

	syms := []string{"A.FR", "B.NL", "C.ETR"}
	for i := 0; i < 3000; i++ {
		sym := syms[i%3]
		p := 100 + float64((i*37)%23) - float64(i%7)
		ts := at(8, 0, 0).Add(time.Duration(i) * 7 * time.Second)
		w.handle(tickMsg(t, sym, p, ts))
		ref.Process(engine.Tick{Symbol: sym, SecType: 'E', Price: p, EventTime: ts.UnixMicro()})
	}
	w.handle(inMsg{kind: models.KindEndOfStream})
	ref.Flush()

	got := decodeIndicators(t, sink.byKind(models.KindIndicator))
	if len(got) != len(want) {
		t.Fatalf("%d indicators, want %d", len(got), len(want))
	}
	for i := range want {
		g, r := got[i], want[i]
		if g.Symbol != r.Symbol || g.WindowStart.UnixMicro() != r.WindowStart || g.LastPrice != r.Close ||
			g.EMA38 != r.EMA38 || g.EMA100 != r.EMA100 || g.Signal != sigString(r.Signal) {
			t.Fatalf("result %d differs:\n got %+v\nwant %+v", i, g, r)
		}
	}
	if n := len(sink.byKind(models.KindSignal)); int64(n) != ref.Stats().BuySignals+ref.Stats().SellSignals {
		t.Errorf("%d signals, engine had %d", n, ref.Stats().BuySignals+ref.Stats().SellSignals)
	}
}

func sigString(s engine.Signal) string {
	if s == engine.None {
		return ""
	}
	return s.String()
}

func near(a, b float64) bool {
	d := a - b
	return d < 1e-9 && d > -1e-9
}

func TestLoadConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := LoadConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.InputTopic != "market-ticks" || cfg.SignalTopic != "trading-signals" || cfg.GroupID == "" ||
		cfg.Engine.Grace != 5*engine.Second || cfg.Engine.WindowSize != 5*engine.Minute {
		t.Errorf("defaults: %+v", cfg)
	}
	for _, args := range [][]string{
		{"-policy", "bogus"},
		{"-start", "middle"},
		{"-group", "", "-partitions", ""},
		{"-signal-topic", ""},
	} {
		if _, err := LoadConfig(args); err == nil {
			t.Errorf("%v: expected an error", args)
		}
	}
	cfg, err = LoadConfig([]string{"-indicator-topic", "none", "-policy", "carry", "-grace", "30s"})
	if err != nil || cfg.IndicatorTopic != "" || cfg.Engine.Policy != engine.CarryLastClose || cfg.Engine.Grace != 30*engine.Second {
		t.Errorf("overrides: %+v %v", cfg, err)
	}
	if ps, err := parsePartitions("0, 2,3"); err != nil || fmt.Sprint(ps) != "[0 2 3]" {
		t.Errorf("parsePartitions: %v %v", ps, err)
	}
}
