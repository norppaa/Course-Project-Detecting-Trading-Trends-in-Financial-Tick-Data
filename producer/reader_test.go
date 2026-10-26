package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		dateStr  string
		timeStr  string
		expected time.Time
	}{
		{
			dateStr:  "08-11-2021",
			timeStr:  "07:00:00.021",
			expected: time.Date(2021, 11, 8, 7, 0, 0, 21000000, time.UTC),
		},
		{
			dateStr:  "08-11-2021",
			timeStr:  "01:01:09.564",
			expected: time.Date(2021, 11, 8, 1, 1, 9, 564000000, time.UTC),
		},
		{
			dateStr:  "15-05-2022",
			timeStr:  "12:30:45",
			expected: time.Date(2022, 5, 15, 12, 30, 45, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		got, err := parseTimestamp(tt.dateStr, tt.timeStr)
		if err != nil {
			t.Fatalf("unexpected error parsing %s %s: %v", tt.dateStr, tt.timeStr, err)
		}
		if !got.Equal(tt.expected) {
			t.Errorf("parseTimestamp(%s, %s) = %v, want %v", tt.dateStr, tt.timeStr, got, tt.expected)
		}
	}
}

func TestReaderCleanedCSV(t *testing.T) {
	csvData := `ID,SecType,Date,Time,Last,TradingTime,TradingDate
# comment line
A2ASZ7.ETR,E,08-11-2021,01:01:07.000,,,
IEBBB.FR,I,08-11-2021,07:00:00.000,52.690000,07:00:00.021,08-11-2021
IUSAC.FR,I,08-11-2021,07:00:00.000,419.360000,07:00:00.023,
`
	// Test with onlyTrades = true
	reader := NewTickReaderFromReader(strings.NewReader(csvData), true)

	tick1, err := reader.Next()
	if err != nil {
		t.Fatalf("unexpected error reading tick 1: %v", err)
	}
	if tick1.ID != "IEBBB.FR" || tick1.Price != 52.69 {
		t.Errorf("tick 1 mismatch: got %+v", tick1)
	}
	if tick1.Timestamp.Hour() != 7 || tick1.Timestamp.Nanosecond() != 21000000 {
		t.Errorf("tick 1 timestamp mismatch: %v", tick1.Timestamp)
	}

	tick2, err := reader.Next()
	if err != nil {
		t.Fatalf("unexpected error reading tick 2: %v", err)
	}
	if tick2.ID != "IUSAC.FR" || tick2.Price != 419.36 {
		t.Errorf("tick 2 mismatch: got %+v", tick2)
	}
	if tick2.TradingDate != "08-11-2021" {
		t.Errorf("tick 2 fallback TradingDate failed: got %q", tick2.TradingDate)
	}

	_, err = reader.Next()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReaderGzipSupport(t *testing.T) {
	rawCSV := `ID,SecType,Date,Time,Last,TradingTime,TradingDate
RDSA.NL,E,08-11-2021,09:00:00.000,18.450000,09:00:00.123,08-11-2021
`
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	gzWriter.Write([]byte(rawCSV))
	gzWriter.Close()

	gzReader, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("failed to create gz reader: %v", err)
	}
	defer gzReader.Close()

	reader := NewTickReaderFromReader(gzReader, true)
	tick, err := reader.Next()
	if err != nil {
		t.Fatalf("failed to read from gzip stream: %v", err)
	}
	if tick.ID != "RDSA.NL" || tick.Price != 18.45 {
		t.Errorf("tick mismatch: got %+v", tick)
	}
}

func BenchmarkTickReader(b *testing.B) {
	r, err := NewTickReader("../data/trades_cleaned.csv.gz", true)
	if err != nil {
		b.Skipf("skipping benchmark: %v", err)
	}
	defer r.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := r.Next()
		if err != nil {
			break
		}
	}
}

func TestParseTimestampRejectsInvalid(t *testing.T) {
	for _, tc := range [][2]string{
		{"31-02-2021", "07:00:00.000"}, // impossible date
		{"ab-11-2021", "07:00:00.000"}, // non-digit day
		{"08-11-2021", "25:00:00"},     // hour out of range
		{"08-11-2021", "07:00:00x123"}, // bad fraction separator
	} {
		if got, err := parseTimestamp(tc[0], tc[1]); err == nil {
			t.Errorf("parseTimestamp(%q, %q) = %v, want error", tc[0], tc[1], got)
		}
	}
}

func TestReaderSkipsZeroPrices(t *testing.T) {
	csvData := `A.FR,E,08-11-2021,07:00:00.000,0.00,07:00:00.000,08-11-2021
B.FR,E,08-11-2021,07:00:00.000,0.0,07:00:00.000,08-11-2021
C.FR,E,08-11-2021,07:00:00.000,1.5,07:00:00.000,08-11-2021`
	reader := NewTickReaderFromReader(strings.NewReader(csvData), true)

	tick, err := reader.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tick.ID != "C.FR" {
		t.Errorf("expected zero-price rows to be skipped, got %+v", tick)
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestNewTickReaderDetectsGzipWithoutExtension(t *testing.T) {
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	gzWriter.Write([]byte("RDSA.NL,E,08-11-2021,09:00:00.000,18.450000,09:00:00.123,08-11-2021\n"))
	gzWriter.Close()

	path := filepath.Join(t.TempDir(), "ticks.csv")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	reader, err := NewTickReader(path, true)
	if err != nil {
		t.Fatalf("failed to open reader: %v", err)
	}
	defer reader.Close()

	tick, err := reader.Next()
	if err != nil {
		t.Fatalf("failed to read tick: %v", err)
	}
	if tick.ID != "RDSA.NL" {
		t.Errorf("tick mismatch: got %+v", tick)
	}
}
