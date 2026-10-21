package main

import (
	"flag"
	"os"
	"strconv"
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

// LoadConfig parses flags and environment variables, giving flags precedence.
func LoadConfig() *Config {
	cfg := &Config{}

	// Defaults with environment variable fallbacks
	defaultInput := getEnv("INPUT_FILE", "data/trades_cleaned.csv.gz")
	defaultBrokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	defaultTopic := getEnv("KAFKA_TOPIC", "market-ticks")
	defaultMode := getEnv("REPLAY_MODE", "max")
	defaultRate, _ := strconv.Atoi(getEnv("RATE_LIMIT", "10000"))
	defaultSpeed, _ := strconv.ParseFloat(getEnv("SPEED_FACTOR", "10.0"), 64)
	defaultBatchSize, _ := strconv.Atoi(getEnv("BATCH_SIZE", "5000"))
	defaultOnlyTrades, _ := strconv.ParseBool(getEnv("ONLY_TRADES", "true"))
	defaultMaxTicks, _ := strconv.ParseInt(getEnv("MAX_TICKS", "0"), 10, 64)

	// CLI flags
	inputFlag := flag.String("input", defaultInput, "Path to input file (.csv or .csv.gz)")
	brokersFlag := flag.String("brokers", defaultBrokers, "Comma-separated Kafka broker addresses")
	topicFlag := flag.String("topic", defaultTopic, "Destination Kafka topic")
	modeFlag := flag.String("mode", defaultMode, "Replay mode: max, rate, or realtime")
	rateFlag := flag.Int("rate", defaultRate, "Target events per second (if mode=rate)")
	speedFlag := flag.Float64("speed", defaultSpeed, "Speedup factor (if mode=realtime)")
	batchSizeFlag := flag.Int("batch-size", defaultBatchSize, "Kafka batch buffer size")
	onlyTradesFlag := flag.Bool("only-trades", defaultOnlyTrades, "Only stream trade events with non-empty Last price")
	maxTicksFlag := flag.Int64("max-ticks", defaultMaxTicks, "Max ticks to process (0 = unlimited)")

	flag.Parse()

	// Handle input file existence fallback: if trades_cleaned.csv.gz doesn't exist, check .csv
	inputFile := *inputFlag
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		if strings.HasSuffix(inputFile, ".gz") {
			uncompressed := strings.TrimSuffix(inputFile, ".gz")
			if _, err := os.Stat(uncompressed); err == nil {
				inputFile = uncompressed
			}
		} else {
			compressed := inputFile + ".gz"
			if _, err := os.Stat(compressed); err == nil {
				inputFile = compressed
			}
		}
	}

	cfg.InputFile = inputFile
	cfg.Brokers = strings.Split(*brokersFlag, ",")
	for i := range cfg.Brokers {
		cfg.Brokers[i] = strings.TrimSpace(cfg.Brokers[i])
	}
	cfg.Topic = *topicFlag
	cfg.ReplayMode = strings.ToLower(strings.TrimSpace(*modeFlag))
	cfg.Rate = *rateFlag
	cfg.Speed = *speedFlag
	cfg.BatchSize = *batchSizeFlag
	cfg.BatchTimeout = 10 * time.Millisecond
	cfg.OnlyTrades = *onlyTradesFlag
	cfg.MaxTicks = *maxTicksFlag

	return cfg
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}
