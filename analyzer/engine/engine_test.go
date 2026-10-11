package engine

import (
	"math"
	"testing"
)

// day0 is 08-11-2021 00:00:00 on the naive clock.
var day0 = mustParse("08-11-2021", "00:00:00")

func mustParse(date, clock string) int64 {
	var p TimeParser
	us, err := p.Parse(date, clock)
	if err != nil {
		panic(err)
	}
	return us
}

// at returns day0 + minutes + seconds.
func at(min, sec float64) int64 {
	return day0 + int64(min*float64(Minute)) + int64(sec*float64(Second))
}

func collect(cfg Config) (*Engine, *[]WindowResult) {
	out := &[]WindowResult{}
	e := New(cfg, func(r WindowResult) { *out = append(*out, r) })
	return e, out
}

func approx(a, b float64) bool {
	return math.Abs(a-b) <= 1e-12*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

// ---------------------------------------------------------------- time parsing

func TestParse(t *testing.T) {
	var p TimeParser
	tests := []struct {
		date, clock string
		want        int64
		wantErr     bool
	}{
		{"08-11-2021", "00:00:00", day0, false},
		{"08-11-2021", "00:00:00.0001", day0 + 100, false},
		{"08-11-2021", "12:34:56.7891", day0 + 12*60*Minute + 34*Minute + 56*Second + 789100, false},
		{"08-11-2021", "23:59:59.999999", day0 + Day - 1, false},
		{"09-11-2021", "00:00:00.000", day0 + Day, false},
		{"08-11-2021", "", 0, true},
		{"08-11-2021", "12:34", 0, true},
		{"08-11-2021", "12:34:56.", 0, true},
		{"2021-11-08", "12:34:56", 0, true},
		{"", "12:34:56", 0, true},
	}
	for _, tc := range tests {
		got, err := p.Parse(tc.date, tc.clock)
		if (err != nil) != tc.wantErr {
			t.Errorf("Parse(%q,%q) err=%v, wantErr=%v", tc.date, tc.clock, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && got != tc.want {
			t.Errorf("Parse(%q,%q)=%d, want %d", tc.date, tc.clock, got, tc.want)
		}
	}
}

func TestWindowsAlignToMidnight(t *testing.T) {
	e := New(Config{}, nil)
	if day0%e.cfg.WindowSize != 0 {
		t.Fatal("midnight is not a window boundary")
	}
	w0 := e.windowIndex(day0)
	if got := e.windowIndex(at(4, 59.9999)); got != w0 {
		t.Errorf("00:04:59.9999 in window %d, want %d", got-w0, 0)
	}
	if got := e.windowIndex(at(5, 0)); got != w0+1 {
		t.Errorf("00:05:00 in window %d, want 1", got-w0)
	}
}

// ---------------------------------------------------------------- Query 2 rules

func TestDetect(t *testing.T) {
	tests := []struct {
		name                           string
		prev38, prev100, cur38, cur100 float64
		want                           Signal
	}{
		{"bullish cross from below", 1, 2, 3, 2, Buy},
		{"bullish cross from equal", 2, 2, 3, 2, Buy},
		{"bearish cross from above", 3, 2, 1, 2, Sell},
		{"bearish cross from equal", 2, 2, 1, 2, Sell},
		{"stays above", 3, 2, 4, 2, None},
		{"stays below", 1, 2, 1, 3, None},
		{"touches from below, no cross", 1, 2, 2, 2, None},
		{"touches from above, no cross", 3, 2, 2, 2, None},
		{"equal both windows", 2, 2, 2, 2, None},
		{"first window, both zero before", 0, 0, 5.1, 2.0, Buy},
	}
	for _, tc := range tests {
		if got := Detect(tc.prev38, tc.prev100, tc.cur38, tc.cur100); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------- Query 1

func TestFirstWindowEMAAndSignal(t *testing.T) {
	for _, suppress := range []bool{false, true} {
		e, out := collect(Config{SuppressFirstSignal: suppress})
		e.Process(Tick{Symbol: "A.FR", Price: 100, EventTime: at(1, 0)})
		e.Flush()
		if len(*out) != 1 {
			t.Fatalf("got %d results, want 1", len(*out))
		}
		r := (*out)[0]
		if !approx(r.EMA38, 100*2.0/39) || !approx(r.EMA100, 100*2.0/101) {
			t.Errorf("EMA38=%v EMA100=%v", r.EMA38, r.EMA100)
		}
		want := Buy
		if suppress {
			want = None
		}
		if r.Signal != want {
			t.Errorf("suppress=%v: signal %v, want %v", suppress, r.Signal, want)
		}
	}
}

func TestCloseIsLastByEventTimeNotArrival(t *testing.T) {
	e, out := collect(Config{Grace: 10 * Second})
	e.Process(Tick{Symbol: "A.FR", Price: 1, EventTime: at(1, 10)})
	e.Process(Tick{Symbol: "A.FR", Price: 2, EventTime: at(1, 5)})  // arrives later, older
	e.Process(Tick{Symbol: "A.FR", Price: 3, EventTime: at(1, 10)}) // tie: later arrival wins
	e.Flush()
	if got := (*out)[0].Close; got != 3 {
		t.Errorf("Close=%v, want 3", got)
	}
}

func TestWindowWaitsForGrace(t *testing.T) {
	e, out := collect(Config{Grace: 2 * Second})
	e.Process(Tick{Symbol: "A.FR", Price: 1, EventTime: at(4, 0)})
	e.Process(Tick{Symbol: "A.FR", Price: 2, EventTime: at(5, 1)}) // wm = 00:04:59
	if len(*out) != 0 {
		t.Fatalf("window 0 closed before grace expired")
	}
	e.Process(Tick{Symbol: "A.FR", Price: 3, EventTime: at(5, 2)}) // wm = 00:05:00
	if len(*out) != 1 || (*out)[0].WindowStart != day0 {
		t.Fatalf("window 0 not closed at end+grace: %+v", *out)
	}
}

func TestOutOfOrderWithinGraceIsUsed(t *testing.T) {
	e, out := collect(Config{Grace: 5 * Second})
	e.Process(Tick{Symbol: "A.FR", Price: 1, EventTime: at(4, 50)})
	e.Process(Tick{Symbol: "A.FR", Price: 9, EventTime: at(5, 1)})
	e.Process(Tick{Symbol: "A.FR", Price: 2, EventTime: at(4, 59)}) // late but within grace
	e.Flush()
	if (*out)[0].Close != 2 {
		t.Errorf("Close=%v, want 2", (*out)[0].Close)
	}
	if s := e.Stats(); s.TicksLate != 0 || s.TicksAccepted != 3 {
		t.Errorf("stats %+v", s)
	}
}

func TestLateEventDropped(t *testing.T) {
	e, out := collect(Config{Grace: 1 * Second})
	e.Process(Tick{Symbol: "A.FR", Price: 1, EventTime: at(4, 0)})
	e.Process(Tick{Symbol: "A.FR", Price: 2, EventTime: at(6, 0)}) // closes window 0
	e.Process(Tick{Symbol: "A.FR", Price: 99, EventTime: at(4, 30)})
	e.Flush()
	if (*out)[0].Close != 1 {
		t.Errorf("late tick changed a closed window")
	}
	if e.Stats().TicksLate != 1 {
		t.Errorf("late tick not counted")
	}
}

func TestHeartbeatClosesQuietSymbols(t *testing.T) {
	e, out := collect(Config{})
	e.Process(Tick{Symbol: "A.FR", Price: 1, EventTime: at(1, 0)})
	e.AdvanceWatermark(at(5, 0))
	if len(*out) != 1 {
		t.Fatalf("heartbeat did not close window")
	}
	e.AdvanceWatermark(at(1, 0)) // backwards: ignored
	if e.Stats().Watermark != at(5, 0) {
		t.Errorf("watermark moved backwards")
	}
}

func TestEmptyWindowPolicies(t *testing.T) {
	feed := func(e *Engine) {
		e.Process(Tick{Symbol: "A.FR", Price: 100, EventTime: at(1, 0)})  // w0
		e.Process(Tick{Symbol: "A.FR", Price: 100, EventTime: at(11, 0)}) // w2, w1 empty
		e.Flush()
	}

	e, out := collect(Config{Policy: SkipEmpty})
	feed(e)
	if len(*out) != 2 {
		t.Fatalf("SkipEmpty: %d results, want 2", len(*out))
	}
	// Second evaluated window applies the formula once more.
	want38 := 100*Alpha38 + (100*Alpha38)*(1-Alpha38)
	if !approx((*out)[1].EMA38, want38) {
		t.Errorf("SkipEmpty EMA38=%v want %v", (*out)[1].EMA38, want38)
	}

	e, out = collect(Config{Policy: CarryLastClose})
	feed(e)
	if len(*out) != 3 {
		t.Fatalf("CarryLastClose: %d results, want 3", len(*out))
	}
	if (*out)[1].HasTick || (*out)[1].Close != 100 {
		t.Errorf("w1 should be carried: %+v", (*out)[1])
	}
	e38 := 0.0
	for i := 0; i < 3; i++ {
		e38 = 100*Alpha38 + e38*(1-Alpha38)
	}
	if !approx((*out)[2].EMA38, e38) {
		t.Errorf("CarryLastClose EMA38=%v want %v", (*out)[2].EMA38, e38)
	}
}

func TestSymbolsIndependentAndOrdered(t *testing.T) {
	e, out := collect(Config{})
	e.Process(Tick{Symbol: "B.NL", Price: 10, EventTime: at(1, 0)})
	e.Process(Tick{Symbol: "A.FR", Price: 20, EventTime: at(2, 0)})
	e.Process(Tick{Symbol: "B.NL", Price: 30, EventTime: at(6, 0)})
	e.Flush()
	got := []string{}
	for _, r := range *out {
		got = append(got, r.Symbol)
	}
	want := []string{"A.FR", "B.NL", "B.NL"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order %v, want %v", got, want)
		}
	}
	if (*out)[0].Close != 20 || (*out)[2].Close != 30 {
		t.Errorf("values mixed between symbols: %+v", *out)
	}
}

// TestAgainstReference runs a price path that rises then falls and compares
// every window with a straightforward recomputation of the spec formulas.
func TestAgainstReference(t *testing.T) {
	var prices []float64
	for i := 0; i < 200; i++ {
		prices = append(prices, 100+10*math.Sin(float64(i)/15))
	}

	e, out := collect(Config{Grace: 1 * Second})
	for i, p := range prices {
		// Several ticks per window; the last one (by event time) is the close.
		base := float64(i * 5)
		e.Process(Tick{Symbol: "X.ETR", Price: p - 1, EventTime: at(base+1, 0)})
		e.Process(Tick{Symbol: "X.ETR", Price: p, EventTime: at(base+4, 30)})
	}
	e.Flush()

	if len(*out) != len(prices) {
		t.Fatalf("%d results, want %d", len(*out), len(prices))
	}
	var r38, r100 float64
	buys, sells := 0, 0
	for i, p := range prices {
		p38, p100 := r38, r100
		r38 = p*(2.0/39) + p38*(1-2.0/39)
		r100 = p*(2.0/101) + p100*(1-2.0/101)
		want := None
		if r38 > r100 && p38 <= p100 {
			want = Buy
		} else if r38 < r100 && p38 >= p100 {
			want = Sell
		}
		got := (*out)[i]
		if !approx(got.EMA38, r38) || !approx(got.EMA100, r100) || got.Signal != want {
			t.Fatalf("window %d: got (%v,%v,%v) want (%v,%v,%v)",
				i, got.EMA38, got.EMA100, got.Signal, r38, r100, want)
		}
		if want == Buy {
			buys++
		} else if want == Sell {
			sells++
		}
	}
	if buys < 2 || sells < 1 {
		t.Errorf("price path too tame: %d buys, %d sells", buys, sells)
	}
}
