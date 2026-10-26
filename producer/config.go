package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Config holds all runtime parameters for the producer service.
type Config struct {
	InputFile    string
	Brokers      []string
	Topic        string
	ReplayMode   string  // "max", "rate", "realtime"
	Rate         int     // events per second (for mode="rate")
	Speed        float64 // speedup multiplier (for mode="realtime")
	BatchSize    int
	BatchTimeout time.Duration
	OnlyTrades   bool
	MaxTicks     int64 // 0 = unlimited
}

// envVars maps each flag to the environment variable that can set it.
var envVars = map[string]string{
	"input":       "INPUT_FILE",
	"brokers":     "KAFKA_BROKERS",
	"topic":       "KAFKA_TOPIC",
	"mode":        "REPLAY_MODE",
	"rate":        "RATE_LIMIT",
	"speed":       "SPEED_FACTOR",
	"batch-size":  "BATCH_SIZE",
	"only-trades": "ONLY_TRADES",
	"max-ticks":   "MAX_TICKS",
}

// LoadConfig parses environment variables and command-line flags (flags take precedence)
// and validates the result.
func LoadConfig(args []string) (*Config, error) {
	cfg := &Config{BatchTimeout: 10 * time.Millisecond}
	var brokers string

	fs := flag.NewFlagSet("producer", flag.ContinueOnError)
	fs.StringVar(&cfg.InputFile, "input", "data/trades_cleaned.csv.gz", "Path to input file (.csv or .csv.gz)")
	fs.StringVar(&brokers, "brokers", "localhost:19092", "Comma-separated Kafka broker addresses")
	fs.StringVar(&cfg.Topic, "topic", "market-ticks", "Destination Kafka topic")
	fs.StringVar(&cfg.ReplayMode, "mode", "max", "Replay mode: max, rate, or realtime")
	fs.IntVar(&cfg.Rate, "rate", 10000, "Target events per second (if mode=rate)")
	fs.Float64Var(&cfg.Speed, "speed", 10.0, "Speedup factor (if mode=realtime)")
	fs.IntVar(&cfg.BatchSize, "batch-size", 5000, "Kafka batch size")
	fs.BoolVar(&cfg.OnlyTrades, "only-trades", true, "Only stream trade events with a positive Last price")
	fs.Int64Var(&cfg.MaxTicks, "max-ticks", 0, "Max ticks to process (0 = unlimited)")

	// Environment variables override defaults; parsing them through the flag set
	// gives the same type checking as command-line flags.
	for name, key := range envVars {
		if val := os.Getenv(key); val != "" {
			if err := fs.Set(name, val); err != nil {
				return nil, fmt.Errorf("invalid %s=%q: %w", key, val, err)
			}
		}
	}
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg.InputFile = resolveInputFile(cfg.InputFile)
	for _, b := range strings.Split(brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.Brokers = append(cfg.Brokers, b)
		}
	}
	cfg.ReplayMode = strings.ToLower(strings.TrimSpace(cfg.ReplayMode))

	return cfg, cfg.validate()
}

func (cfg *Config) validate() error {
	switch {
	case len(cfg.Brokers) == 0:
		return fmt.Errorf("at least one broker is required")
	case cfg.ReplayMode != "max" && cfg.ReplayMode != "rate" && cfg.ReplayMode != "realtime":
		return fmt.Errorf("unknown replay mode %q (expected max, rate, or realtime)", cfg.ReplayMode)
	case cfg.ReplayMode == "rate" && cfg.Rate <= 0:
		return fmt.Errorf("rate must be positive, got %d", cfg.Rate)
	case cfg.ReplayMode == "realtime" && cfg.Speed <= 0:
		return fmt.Errorf("speed must be positive, got %g", cfg.Speed)
	case cfg.BatchSize <= 0:
		return fmt.Errorf("batch size must be positive, got %d", cfg.BatchSize)
	case cfg.MaxTicks < 0:
		return fmt.Errorf("max ticks must not be negative, got %d", cfg.MaxTicks)
	}
	return nil
}

// resolveInputFile falls back to the compressed or uncompressed variant of path
// (adding or removing ".gz") when path itself does not exist.
func resolveInputFile(path string) string {
	if _, err := os.Stat(path); err == nil {
		return path
	}
	alt := path + ".gz"
	if strings.HasSuffix(path, ".gz") {
		alt = strings.TrimSuffix(path, ".gz")
	}
	if _, err := os.Stat(alt); err == nil {
		return alt
	}
	return path
}
