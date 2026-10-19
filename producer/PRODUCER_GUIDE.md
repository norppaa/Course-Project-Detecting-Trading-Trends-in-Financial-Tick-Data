# Producer & Data Consumption Guide

A quick reference on how to start the infrastructure, run the Data Producer in different modes, and inspect/consume the streamed market data.

---

## 1. Start the Infrastructure

Make sure the Redpanda broker and Web Console are running:

```bash
docker compose up -d
```

- **Broker Address:** `localhost:19092` (host apps) / `redpanda:9092` (containers)
- **Web Console UI:** [http://localhost:8080](http://localhost:8080)
- **Topic:** `market-ticks` (4 partitions)

---

## 2. Using the Data Producer

The producer reads `.csv.gz` or `.csv` files and streams them into `market-ticks`.

### A. Run Locally (`go run`)

#### 1. Maximum Throughput (Benchmark Mode)
Streams events as fast as possible. Best for performance benchmarks and report evaluation:
```bash
go run ./producer -input data/trades_cleaned.csv.gz
```

#### 2. Fixed Rate Mode
Throttles output to a constant number of events per second (e.g. 5,000 msg/s):
```bash
go run ./producer -input data/trades_cleaned.csv.gz -mode rate -rate 5000
```

#### 3. Simulated Real-Time Replay
Replays events preserving historical timestamp intervals scaled by a speedup multiplier (e.g. 10x or 50x faster than real-time). Best for the live web dashboard:
```bash
go run ./producer -input data/trades_cleaned.csv.gz -mode realtime -speed 10.0
```

#### 4. Quick Test Slices
Stop automatically after streaming $N$ ticks (great for testing):
```bash
go run ./producer -input data/trades_cleaned.csv.gz -max-ticks 1000
```

---

### B. Run via Docker Compose

```bash
# Run the containerized producer
docker compose run --rm producer

# Override arguments inside Docker (e.g. fixed rate)
docker compose run --rm producer ./producer -mode rate -rate 2000
```

---

## 3. How to Read & Inspect the Streamed Data

### Method 1: Redpanda Web Console (Visual)
1. Open [http://localhost:8080](http://localhost:8080) in your browser.
2. Navigate to **Topics** -> **`market-ticks`**.
3. Click the **Messages** tab to view live payloads, partition distribution, and timestamps.

### Method 2: Terminal CLI (`rpk`)
Consume messages directly from the command line:

```bash
# View the first 5 messages
docker exec redpanda rpk topic consume market-ticks --brokers redpanda:9092 -n 5

# Follow live incoming messages
docker exec redpanda rpk topic consume market-ticks --brokers redpanda:9092
```

Sample output:
```json
{
  "topic": "market-ticks",
  "key": "IGGOU.FR",
  "value": "{\"id\":\"IGGOU.FR\",\"sec_type\":\"I\",\"timestamp\":\"2021-11-08T07:00:00.023Z\",\"price\":60.9096,\"trading_date\":\"08-11-2021\",\"trading_time\":\"07:00:00.023\"}",
  "timestamp": 1636354800023,
  "partition": 0,
  "offset": 0
}
```

---

### Method 3: Reading Data in Go (Downstream Consumer)

Downstream services (like the Stream Analyzer) consume from `market-ticks` using `kafka-go` and the shared [`pkg/models`](file:///home/noora/school/scaddis/Course-Project-Detecting-Trading-Trends-in-Financial-Tick-Data/pkg/models/tick.go):

```go
package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"trading-trends/pkg/models"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:19092"},
		Topic:   "market-ticks",
		GroupID: "stream-analyzer-group", // Consumer group balances across partitions
	})
	defer reader.Close()

	ctx := context.Background()
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("Error reading message: %v", err)
		}

		var tick models.MarketTick
		if err := json.Unmarshal(msg.Value, &tick); err != nil {
			log.Printf("Error unmarshaling tick: %v", err)
			continue
		}

		log.Printf("Partition %d | Symbol: %s | Price: %.4f | Time: %s",
			msg.Partition, tick.ID, tick.Price, tick.TradingTime)
	}
}
```
