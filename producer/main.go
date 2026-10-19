package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := LoadConfig()

	log.Println("==================================================")
	log.Println("           FINANCIAL TICK DATA PRODUCER           ")
	log.Println("==================================================")
	log.Printf("Input File   : %s", cfg.InputFile)
	log.Printf("Brokers      : %v", cfg.Brokers)
	log.Printf("Topic        : %s", cfg.Topic)
	log.Printf("Replay Mode  : %s", cfg.ReplayMode)
	if cfg.ReplayMode == "rate" {
		log.Printf("Target Rate  : %d msgs/sec", cfg.Rate)
	} else if cfg.ReplayMode == "realtime" {
		log.Printf("Speed Factor : %.1fx", cfg.Speed)
	}
	log.Printf("Batch Size   : %d", cfg.BatchSize)
	log.Printf("Only Trades  : %t", cfg.OnlyTrades)
	if cfg.MaxTicks > 0 {
		log.Printf("Max Ticks    : %d", cfg.MaxTicks)
	}
	log.Println("--------------------------------------------------")

	// Context with graceful shutdown on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("\n[Producer] Received signal %v, gracefully shutting down...", sig)
		cancel()
	}()

	// 1. Initialize Reader
	reader, err := NewTickReader(cfg.InputFile, cfg.OnlyTrades)
	if err != nil {
		log.Fatalf("[Producer] Failed to open input file: %v", err)
	}
	defer reader.Close()

	// 2. Initialize Replayer / Pacer
	var pacer Pacer
	switch cfg.ReplayMode {
	case "rate":
		p, err := NewFixedRatePacer(cfg.Rate)
		if err != nil {
			log.Fatalf("[Producer] Failed to initialize rate pacer: %v", err)
		}
		pacer = p
	case "realtime":
		p, err := NewSimulatedRealtimePacer(cfg.Speed, 1*time.Second)
		if err != nil {
			log.Fatalf("[Producer] Failed to initialize realtime pacer: %v", err)
		}
		pacer = p
	default:
		pacer = NewMaxSpeedPacer()
	}
	defer pacer.Close()

	// 3. Initialize Kafka Publisher
	publisher := NewKafkaPublisher(cfg.Brokers, cfg.Topic, cfg.BatchSize, cfg.BatchTimeout, "snappy")
	defer publisher.Close()

	startTime := time.Now()
	var processedTicks int64
	var lastReportedCount int64
	lastReportTime := startTime
	var latestTickTime string

	// 4. Background Progress Logger
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				currentSent, errs := publisher.Stats()
				deltaCount := currentSent - lastReportedCount
				deltaSec := now.Sub(lastReportTime).Seconds()
				instantRate := float64(deltaCount) / deltaSec
				avgRate := float64(currentSent) / now.Sub(startTime).Seconds()

				lastReportedCount = currentSent
				lastReportTime = now

				log.Printf("[Stats] Sent: %-9d | Rate: %-8.0f msg/s | Avg: %-8.0f msg/s | EventTime: %-12s | Errors: %d",
					currentSent, instantRate, avgRate, latestTickTime, errs)
			}
		}
	}()

	// 5. Main Replay Stream Loop
	log.Println("[Producer] Streaming started...")

	for {
		if ctx.Err() != nil {
			break
		}

		if cfg.MaxTicks > 0 && processedTicks >= cfg.MaxTicks {
			log.Printf("[Producer] Reached limit of %d ticks. Stopping.", cfg.MaxTicks)
			break
		}

		tick, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				log.Println("[Producer] Reached end of file (EOF).")
				break
			}
			log.Printf("[Producer] Read error: %v", err)
			break
		}

		// Update pacing
		if err := pacer.Wait(ctx, tick); err != nil {
			break
		}

		// Publish to Kafka
		if err := publisher.Publish(ctx, tick); err != nil {
			log.Printf("[Producer] Publish error: %v", err)
		}

		processedTicks++
		latestTickTime = tick.TradingTime
	}

	// 6. Graceful Flush and Final Report
	log.Println("[Producer] Flushing in-flight batches to Kafka...")
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()
	_ = publisher.Flush(flushCtx)

	elapsed := time.Since(startTime)
	sent, errs := publisher.Stats()
	totalLines, _ := reader.Stats()

	log.Println("==================================================")
	log.Println("           PRODUCER EXECUTION SUMMARY             ")
	log.Println("==================================================")
	log.Printf("Total Elapsed Time : %v", elapsed.Round(time.Millisecond))
	log.Printf("Total Lines Read   : %d", totalLines)
	log.Printf("Total Ticks Sent   : %d", sent)
	log.Printf("Publish Errors     : %d", errs)
	if elapsed.Seconds() > 0 {
		log.Printf("Overall Avg Rate   : %.0f msgs/second", float64(sent)/elapsed.Seconds())
	}
	log.Println("==================================================")
}
