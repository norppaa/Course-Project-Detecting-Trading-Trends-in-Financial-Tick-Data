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
}

