package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"trading-trends/pkg/models"
)

func main() {
	cfg, err := LoadConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Fatalf("[Producer] Invalid configuration: %v", err)
	}

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

	// Context cancelled on SIGINT/SIGTERM for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pacer, err := newPacer(cfg)
	if err != nil {
		log.Fatalf("[Producer] Failed to initialize pacer: %v", err)
	}

	reader, err := NewTickReader(cfg.InputFile, cfg.OnlyTrades)
	if err != nil {
		log.Fatalf("[Producer] Failed to open input file: %v", err)
	}
	defer reader.Close()

	publisher := NewKafkaPublisher(cfg.Brokers, cfg.Topic, cfg.BatchSize, cfg.BatchTimeout)

	startTime := time.Now()
	var latestTick atomic.Pointer[models.MarketTick]

	// Background progress logger
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var lastDelivered int64
		lastReport := startTime
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				queued, delivered, failed := publisher.Stats()
				instantRate := float64(delivered-lastDelivered) / now.Sub(lastReport).Seconds()
				avgRate := float64(delivered) / now.Sub(startTime).Seconds()
				lastDelivered, lastReport = delivered, now

				eventTime := ""
				if t := latestTick.Load(); t != nil {
					eventTime = t.TradingTime
				}
				log.Printf("[Stats] Queued: %-9d | Delivered: %-9d | Rate: %-8.0f msg/s | Avg: %-8.0f msg/s | EventTime: %-12s | Failed: %d",
					queued, delivered, instantRate, avgRate, eventTime, failed)
			}
		}
	}()

	log.Println("[Producer] Streaming started...")

	// In max mode ticks are handed to Kafka in chunks, which cuts per-call overhead.
	// Paced modes send each tick immediately so it never waits behind a sleep.
	chunkSize := 1
	if cfg.ReplayMode == "max" {
		chunkSize = cfg.BatchSize
	}
	pending := make([]*models.MarketTick, 0, chunkSize)
	publishPending := func() {
		if err := publisher.Publish(ctx, pending...); err != nil {
			log.Printf("[Producer] Publish error: %v", err)
		}
		pending = pending[:0]
	}

	var processedTicks int64
	for cfg.MaxTicks == 0 || processedTicks < cfg.MaxTicks {
		tick, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("[Producer] Reached end of file (EOF).")
			} else {
				log.Printf("[Producer] Read error: %v", err)
			}
			break
		}

		if err := pacer.Wait(ctx, tick); err != nil {
			log.Println("[Producer] Shutdown signal received, stopping...")
			break
		}

		pending = append(pending, tick)
		if len(pending) == chunkSize {
			publishPending()
		}

		processedTicks++
		latestTick.Store(tick)
	}
	publishPending()
	if cfg.MaxTicks > 0 && processedTicks >= cfg.MaxTicks {
		log.Printf("[Producer] Reached limit of %d ticks. Stopping.", cfg.MaxTicks)
	}

	// Restore default signal handling so a second Ctrl+C exits immediately.
	stop()

	log.Println("[Producer] Waiting for in-flight batches to be delivered...")
	_ = publisher.Close()

	elapsed := time.Since(startTime)
	queued, delivered, failed := publisher.Stats()
	totalLines, _ := reader.Stats()

	log.Println("==================================================")
	log.Println("           PRODUCER EXECUTION SUMMARY             ")
	log.Println("==================================================")
	log.Printf("Total Elapsed Time : %v", elapsed.Round(time.Millisecond))
	log.Printf("Total Lines Read   : %d", totalLines)
	log.Printf("Ticks Queued       : %d", queued)
	log.Printf("Ticks Delivered    : %d", delivered)
	log.Printf("Ticks Failed       : %d", failed)
	log.Printf("Overall Avg Rate   : %.0f msgs/second", float64(delivered)/elapsed.Seconds())
	log.Println("==================================================")
}
