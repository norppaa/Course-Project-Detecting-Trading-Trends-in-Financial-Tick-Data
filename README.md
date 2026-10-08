# Developer Guide: Detecting Trading Trends in Financial Tick Data

> **CS-E4780 Scalable Systems and Data Management** — Course Project  
> [Overleaf Report](https://www.overleaf.com/8759344531hyxrvyfbdfkc#7fefc3) • [DEBS 2022 Dataset (Zenodo)](https://zenodo.org/records/6382482)

This document is the **development guide** for building, testing, and debugging the system components.

---

## 1. System Pipeline & Data Flow

```
┌─────────────────────┐
│ 1. Data Producer    │ ──► Reads CSV and replays ticks to broker
└──────────┬──────────┘
           │
           ▼ (Topic: "market-ticks", Partition Key: Symbol)
┌─────────────────────┐
│ 2. Message Broker   │ ──► Redpanda cluster (Kafka API, strict FIFO per symbol)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 3. Stream Analyzer  │ ──► 5-min tumbling windows, EMA38/100, crossover detection
└──────────┬──────────┘
           │
           ▼ (Topic: "trading-signals")
┌─────────────────────┐
│ 4. Web Gateway      │ ──► WebSocket server broadcasting indicators and signals
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 5. Web Frontend     │ ──► Live market dashboard with charts and buy/sell alerts
└─────────────────────┘
```

---

## 2. Local Environment Setup

### Prerequisites
- **Docker & Docker Compose**
- **Go 1.25+**

### Step 1: Start the Local Infrastructure
Run the Redpanda message broker, web console, and topic initializer:

```bash
docker compose up -d
```

- **Kafka Broker Endpoint:** `localhost:19092` (used by Go apps running locally)
- **Redpanda Web Console:** [http://localhost:8080](http://localhost:8080)
- **Pre-created Topics:**
  - `market-ticks` (4 partitions)
  - `trading-signals` (4 partitions)

To verify the broker is healthy:
```bash
docker compose ps
docker exec -it redpanda rpk topic list --brokers redpanda:9092
```

---

## 3. Dataset & Preparation

### Downloading Data
Place the raw day CSVs from [Zenodo](https://zenodo.org/records/6382482) inside the `data/` directory (ignored by git):
```text
data/debs2022-gc-trading-day-08-11-21.csv
```

### Preprocessing the Dataset
The raw dataset contains 27+ attributes and millions of housekeeping rows. Use [`scripts/clean_data.go`](file:///home/noora/school/scaddis/Course-Project-Detecting-Trading-Trends-in-Financial-Tick-Data/scripts/clean_data.go) to extract only the starred attributes required by the assignment (`ID`, `SecType`, `Date`, `Time`, `Last`, `TradingTime`, `TradingDate`):

see scripts/readme.md

---

## 4. Message Schemas & Contracts

Define later

---

## 5. Daily Development Workflow

### Local Development vs. Docker Compose Development

This project supports two development modes:

| Mode | How it Runs | When to Use | Advantages |
|---|---|---|---|
| **Local Mode** *(Recommended for coding)* | **Broker in Docker**, Go apps run locally on your machine (`go run ...`) | Daily coding, implementing features, fixing bugs, unit testing | • **Instant feedback:** Runs immediately in <1s (no waiting for Docker images to rebuild)<br>• **Native debugging:** 1-click breakpoints and variable inspection in VS Code / GoLand<br>• **Fast tests:** Instant `go test ./...`<br>• **Resource-light:** Saves laptop RAM and battery |
| **Docker Compose Mode** *(Staging & Submission)* | **Everything in Docker** (`docker compose --profile app up -d --build`) | Integration testing, final verification, grading submission | • **Environment parity:** Guarantees everything works in isolated containers on any teammate's machine<br>• **Zero local dependencies:** Team members don't need Go or tools installed to test the full stack |

#### How to Use Local Mode:
1. Start Redpanda in Docker:
   ```bash
   docker compose up -d
   ```
2. Run your Go services directly in terminal or IDE:
   ```bash
   # Go apps connect to localhost:19092
   go run ./analyzer/main.go
   go run ./producer/main.go -input data/sample_ticks.csv
   ```

#### How to Use Docker Compose Mode:
When you are ready to test the entire containerized pipeline together:
```bash
docker compose --profile app up -d --build
```

---

## 6. Code Quality & Tooling (Formatting, Linting & Testing)

Go includes robust built-in tooling that eliminates the need for heavy external linters in most projects.

### Built-in Go Tools (No Extra Installation Needed)

- **`go fmt ./...` (Auto-Formatter):**
  Enforces the official Go style standard across all files. Run this before committing:
  ```bash
  go fmt ./...
  ```
- **`go vet ./...` (Official Static Analyzer & Bug Finder):**
  Inspects code for subtle bugs, unreachable code, printf format errors, bad mutex usage, and suspicious logic:
  ```bash
  go vet ./...
  ```
- **`go build ./...` (Fast Syntax & Type Check):**
  Compiles all packages to verify syntax and types without outputting binary files:
  ```bash
  go build ./...
  ```
- **`go test -race ./...` (Concurrency Race Detector):**
  Detects unsynchronized concurrent read/write memory access at runtime. Highly recommended when testing the stream analyzer:
  ```bash
  go test -race ./...
  ```

### Recommended Editor Setup (Real-Time Error Checking)

- **VS Code:** Install the official **Go extension** by the Go team (`golang.go`). It automatically runs `gopls` (the Go language server), giving you:
  - Red squiggly lines on syntax/type errors in real time as you type.
  - Auto-completion for structs, methods, and Kafka libraries.
  - Automatic import addition/removal and format-on-save.
- **GoLand:** Real-time syntax checking and static analysis work out of the box.


