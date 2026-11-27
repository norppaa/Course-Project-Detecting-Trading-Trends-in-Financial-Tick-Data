package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"trading-trends/analyzer/engine"
)

// The analyzer can be run in two modes: either it joins a consumer group (GroupID) 
// and Kafka assigns partitions, or it reads exactly the partitions given in
// Partitions. The latter is deterministic and handy for benchmarks.
type Config struct {
	Brokers        []string
	InputTopic     string
	IndicatorTopic string 	// "" = do not publish Query 1 indicators
	SignalTopic    string
	GroupID    	   string 	// consumer group id (where to read with a group)
	Partitions 	   string	// "all" or e.g. "0,1" (where to read without a group)
	StartAt    	   string 	// "first" or "last" (where to begin without committed offsets)

	Engine engine.Config

	IdleFlush       time.Duration	// close all windows after this long with no input; 0 = never
	MetricsInterval time.Duration
	WorkerBuffer    int 			// per-partition channel capacity
	BatchSize       int 			// output writer batch size
	BatchTimeout    time.Duration
}

func LoadConfig(args []string) (*Config, error) {
	fs := flag.NewFlagSet("analyzer", flag.ContinueOnError)

	brokers := fs.String("brokers", env("KAFKA_BROKERS", "localhost:19092"), "comma-separated broker addresses")
	inTopic := fs.String("input-topic", env("INPUT_TOPIC", "market-ticks"), "topic to read ticks from")
	indTopic := fs.String("indicator-topic", env("INDICATOR_TOPIC", "trading-signals"), "topic for Query 1 window indicators ('' or 'none' = don't publish)")
	sigTopic := fs.String("signal-topic", env("SIGNAL_TOPIC", "trading-signals"), "topic for Query 2 BUY/SELL advisories")

	group := fs.String("group", env("GROUP_ID", "stream-analyzer"), "consumer group id (group mode)")
	parts := fs.String("partitions", env("PARTITIONS", ""), "read these partitions without a group: 'all' or e.g. '0,1'")
	start := fs.String("start", env("START_OFFSET", "first"), "where to start without committed offsets: first | last")

	grace := fs.Duration("grace", envDuration("GRACE", 5*time.Second), "allowed out-of-orderness of event time")
	policy := fs.String("policy", env("EMPTY_WINDOW_POLICY", "skip"), "empty-window policy: skip | carry")
	suppress := fs.Bool("suppress-first", envBool("SUPPRESS_FIRST_SIGNAL", false), "suppress the advisory in a symbol's first window")

	idle := fs.Duration("idle-flush", envDuration("IDLE_FLUSH", 5*time.Second), "close all windows after this long without input (0 = never)")
	metrics := fs.Duration("metrics-interval", envDuration("METRICS_INTERVAL", 5*time.Second), "how often to log throughput")
	buf := fs.Int("worker-buffer", envInt("WORKER_BUFFER", 4096), "per-partition queue length")
	batch := fs.Int("batch-size", envInt("OUTPUT_BATCH_SIZE", 1000), "output writer batch size")
	batchTimeout := fs.Duration("batch-timeout", envDuration("OUTPUT_BATCH_TIMEOUT", 5*time.Millisecond), "output writer max batching delay")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg := &Config{
		InputTopic:      *inTopic,
		IndicatorTopic:  *indTopic,
		SignalTopic:     *sigTopic,
		GroupID:         *group,
		Partitions:      strings.TrimSpace(*parts),
		StartAt:         strings.ToLower(*start),
		IdleFlush:       *idle,
		MetricsInterval: *metrics,
		WorkerBuffer:    *buf,
		BatchSize:       *batch,
		BatchTimeout:    *batchTimeout,
		Engine: engine.Config{
			WindowSize:          5 * engine.Minute,
			Grace:               grace.Microseconds(),
			SuppressFirstSignal: *suppress,
		},
	}
	for _, b := range strings.Split(*brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.Brokers = append(cfg.Brokers, b)
		}
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers given")
	}
	if cfg.IndicatorTopic == "none" {
		cfg.IndicatorTopic = ""
	}
	if cfg.SignalTopic == "" {
		return nil, fmt.Errorf("signal topic must not be empty")
	}
	switch *policy {
	case "skip":
		cfg.Engine.Policy = engine.SkipEmpty
	case "carry":
		cfg.Engine.Policy = engine.CarryLastClose
	default:
		return nil, fmt.Errorf("unknown policy %q (want skip or carry)", *policy)
	}
	if cfg.StartAt != "first" && cfg.StartAt != "last" {
		return nil, fmt.Errorf("unknown start offset %q (want first or last)", cfg.StartAt)
	}
	if cfg.Partitions == "" && cfg.GroupID == "" {
		return nil, fmt.Errorf("set either a consumer group or explicit partitions")
	}
	if cfg.WorkerBuffer < 1 {
		cfg.WorkerBuffer = 1
	}
	return cfg, nil
}

// Parses a comma-separated list of partition numbers, e.g. "0,1,2".
func parsePartitions(s string) ([]int, error) {
	var out []int
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		n, err := strconv.Atoi(f)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("bad partition %q", f)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no partitions in %q", s)
	}
	return out, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if n, err := strconv.Atoi(env(key, "")); err == nil {
		return n
	}
	return def
}

func envBool(key string, def bool) bool {
	if b, err := strconv.ParseBool(env(key, "")); err == nil {
		return b
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(env(key, "")); err == nil {
		return d
	}
	return def
}
