package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"trading-trends/gateway/kafka"
	"trading-trends/gateway/server"
	"trading-trends/gateway/store"
)

func main() {
	// 1. Command-line flags and environment variable defaults
	port := flag.String("port", getEnv("PORT", "8081"), "HTTP/WebSocket server port")
	brokersStr := flag.String("kafka-brokers", getEnv("KAFKA_BROKERS", "localhost:19092"), "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", getEnv("SIGNALS_TOPIC", "trading-signals"), "Kafka topic with analyzer output (indicators + signals)")
	groupID := flag.String("group", getEnv("CONSUMER_GROUP", ""), "Kafka consumer group ID (empty = unique per gateway instance)")
	startAt := flag.String("start", getEnv("START_OFFSET", "first"), "Where to start reading: 'first' (replay topic to rebuild history) or 'last' (new results only)")
	historySize := flag.Int("history", 500, "Windows kept in memory per symbol")
	flag.Parse()

	log.Println("=========================================================")
	log.Println("  CS-E4780 Trading Trends - Web Gateway & Subscription   ")
	log.Println("=========================================================")
	log.Printf("Listening Port:     %s", *port)
	log.Printf("Kafka Brokers:      %s", *brokersStr)
	log.Printf("Signals Topic:      %s", *topic)
	log.Printf("Start Offset:       %s", *startAt)
	log.Println("---------------------------------------------------------")

	// 2. In-memory store and WebSocket hub
	memStore := store.NewMemoryStore(*historySize, 500)
	hub := server.NewHub(memStore)
	go hub.Run()

	// 3. Root context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Kafka consumer for the analyzer output
	consumer := kafka.NewSignalConsumer(kafka.ConsumerConfig{
		Brokers: splitAndTrim(*brokersStr),
		Topic:   *topic,
		GroupID: *groupID,
		StartAt: *startAt,
	}, hub)
	go consumer.Start(ctx)

	// 5. HTTP & WebSocket server
	srv := server.NewServer(hub, memStore)
	srv.ConsumerStats = func() any { return consumer.Stats() }
	httpServer := &http.Server{
		Handler:     srv.Handler(),
		ReadTimeout: 15 * time.Second,
		IdleTimeout: 60 * time.Second,
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil && *port == "8081" {
		log.Printf("[Gateway] Port 8081 is currently in use on host. Falling back to port 8085...")
		*port = "8085"
		listener, err = net.Listen("tcp", fmt.Sprintf(":%s", *port))
	}
	if err != nil {
		log.Fatalf("[Gateway] Server failed to bind port: %v", err)
	}

	go func() {
		log.Printf("[Gateway] Dashboard: http://localhost:%s  (WebSocket: ws://localhost:%s/ws)", *port, *port)
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Gateway] Server failed: %v", err)
		}
	}()

	// 6. Wait for termination signals (Ctrl+C, Docker stop)
	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, os.Interrupt, syscall.SIGTERM)
	<-stopSig

	log.Println("[Gateway] Shutting down gracefully...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[Gateway] HTTP server shutdown error: %v", err)
	}
	if err := consumer.Close(); err != nil {
		log.Printf("[Gateway] Kafka consumer close error: %v", err)
	}

	log.Println("[Gateway] Goodbye!")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			res = append(res, p)
		}
	}
	return res
}
