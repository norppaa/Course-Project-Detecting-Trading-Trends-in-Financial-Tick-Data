package main

import (
	"testing"
)

func TestLoadConfigPrecedence(t *testing.T) {
	t.Setenv("REPLAY_MODE", "rate")
	t.Setenv("RATE_LIMIT", "500")
	t.Setenv("KAFKA_BROKERS", "a:9092, b:9092")

	cfg, err := LoadConfig([]string{"-rate", "750"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ReplayMode != "rate" {
		t.Errorf("mode from env not applied: got %q", cfg.ReplayMode)
	}
	if cfg.Rate != 750 {
		t.Errorf("flag should override env: got rate %d", cfg.Rate)
	}
	if len(cfg.Brokers) != 2 || cfg.Brokers[1] != "b:9092" {
		t.Errorf("brokers not split and trimmed: got %q", cfg.Brokers)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		args []string
	}{
		{name: "non-numeric env", env: map[string]string{"BATCH_SIZE": "5k"}},
		{name: "unknown mode", args: []string{"-mode", "realtme"}},
		{name: "zero rate", args: []string{"-mode", "rate", "-rate", "0"}},
		{name: "negative speed", args: []string{"-mode", "realtime", "-speed", "-1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			if _, err := LoadConfig(tt.args); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}
