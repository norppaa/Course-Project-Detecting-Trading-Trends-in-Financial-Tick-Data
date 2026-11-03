package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSignalEventUnmarshal(t *testing.T) {
	// 1. Standard format
	jsonStandard := []byte(`{
		"symbol": "RDSA.NL",
		"sec_type": "E",
		"window_start": "2021-11-08T09:00:00Z",
		"window_end": "2021-11-08T09:05:00Z",
		"window_index": 108,
		"close": 19.852,
		"ema38": 19.824,
		"ema100": 19.780,
		"signal": "BUY"
	}`)

	var s1 SignalEvent
	if err := json.Unmarshal(jsonStandard, &s1); err != nil {
		t.Fatalf("Failed to unmarshal standard format: %v", err)
	}
	if s1.Symbol != "RDSA.NL" || s1.Close != 19.852 || s1.Signal != "BUY" || s1.EMA38 != 19.824 {
		t.Errorf("Unexpected values in s1: %+v", s1)
	}

	// 2. Alternative naming format (e.g. ID, last, ema_38)
	jsonAlt := []byte(`{
		"ID": "AIR.FR",
		"last": 112.4,
		"ema_38": 111.9,
		"ema_100": 113.1,
		"advisory": "SELL",
		"timestamp": "2021-11-08 09:10:00"
	}`)

	var s2 SignalEvent
	if err := json.Unmarshal(jsonAlt, &s2); err != nil {
		t.Fatalf("Failed to unmarshal alt format: %v", err)
	}
	if s2.Symbol != "AIR.FR" || s2.Close != 112.4 || s2.Signal != "SELL" || s2.EMA38 != 111.9 || s2.EMA100 != 113.1 {
		t.Errorf("Unexpected values in s2: %+v", s2)
	}
	expectedTime, _ := time.Parse("2006-01-02 15:04:05", "2021-11-08 09:10:00")
	if !s2.Timestamp.Equal(expectedTime) {
		t.Errorf("Timestamp mismatch: got %v, expected %v", s2.Timestamp, expectedTime)
	}

	// 3. Exact models.WindowIndicator format from analyzer
	jsonIndicator := []byte(`{
		"kind": "indicator",
		"symbol": "RDSA.NL",
		"sec_type": "E",
		"window_start": "2021-11-08T09:00:00Z",
		"window_end": "2021-11-08T09:05:00Z",
		"last_price": 19.852,
		"has_tick": true,
		"ema_38": 19.824,
		"ema_100": 19.780,
		"signal": "BUY"
	}`)

	var s3 SignalEvent
	if err := json.Unmarshal(jsonIndicator, &s3); err != nil {
		t.Fatalf("Failed to unmarshal WindowIndicator format: %v", err)
	}
	if s3.Symbol != "RDSA.NL" || s3.Close != 19.852 || s3.EMA38 != 19.824 || s3.EMA100 != 19.780 || s3.Signal != "BUY" {
		t.Errorf("Unexpected values in s3 (WindowIndicator): %+v", s3)
	}

	// 4. Exact models.TradeSignal format from analyzer
	jsonSignal := []byte(`{
		"kind": "signal",
		"symbol": "AIR.FR",
		"sec_type": "E",
		"type": "SELL",
		"window_start": "2021-11-08T09:00:00Z",
		"timestamp": "2021-11-08T09:05:00Z",
		"price": 112.4,
		"ema_38": 111.9,
		"ema_100": 113.1,
		"prev_ema_38": 113.5,
		"prev_ema_100": 113.0
	}`)

	var s4 SignalEvent
	if err := json.Unmarshal(jsonSignal, &s4); err != nil {
		t.Fatalf("Failed to unmarshal TradeSignal format: %v", err)
	}
	if s4.Symbol != "AIR.FR" || s4.Close != 112.4 || s4.Signal != "SELL" || s4.EMA38 != 111.9 || s4.EMA100 != 113.1 {
		t.Errorf("Unexpected values in s4 (TradeSignal): %+v", s4)
	}
}

