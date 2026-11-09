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
	"trading-trends/gateway/mock"
	"trading-trends/gateway/server"
	"trading-trends/gateway/store"
)

func main() {
	// 1. Command-line flags and environment variable defaults
	defaultBrokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	defaultPort := getEnv("PORT", "8081")
	defaultTopic := getEnv("SIGNALS_TOPIC", "trading-signals")
	defaultGroup := getEnv("CONSUMER_GROUP", "web-gateway-group")
	defaultMock := getEnvBool("ENABLE_MOCK", false)

	port := flag.String("port", defaultPort, "HTTP/WebSocket server port")
	brokersStr := flag.String("kafka-brokers", defaultBrokers, "Comma-separated Kafka broker addresses")
	topic := flag.String("topic", defaultTopic, "Kafka topic for trading signals")
	groupID := flag.String("group", defaultGroup, "Kafka consumer group ID")
	enableMock := flag.Bool("mock", defaultMock, "Run in mock mode generating simulated EMA signals")
	mockIntervalMs := flag.Int("mock-interval-ms", 1200, "Interval in ms between simulated window events in mock mode")
	flag.Parse()

	log.Println("=========================================================")
	log.Println("  CS-E4780 Trading Trends - Web Gateway & Subscription   ")
	log.Println("=========================================================")
	log.Printf("Listening Port:     %s", *port)
	log.Printf("Kafka Brokers:      %s", *brokersStr)
	log.Printf("Signals Topic:      %s", *topic)
	log.Printf("Mock Generator:     %t", *enableMock)
	log.Println("---------------------------------------------------------")

	// 2. Initialize In-Memory Store and WebSocket Hub
	memStore := store.NewMemoryStore(500, 200)
	hub := server.NewHub(memStore)
	go hub.Run()

	// 3. Root context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Start Mock Generator if requested
	if *enableMock {
		log.Println("[Gateway] Mock generator active. Simulating DEBS 2022 market ticks...")
		go mock.StartMockGenerator(ctx, hub, time.Duration(*mockIntervalMs)*time.Millisecond)
	}

	// 5. Start Kafka Signal Consumer
	brokers := splitAndTrim(*brokersStr)
	consumerCfg := kafka.ConsumerConfig{
		Brokers: brokers,
		Topic:   *topic,
		GroupID: *groupID,
	}
	consumer := kafka.NewSignalConsumer(consumerCfg, hub)
	go consumer.Start(ctx)

	// 6. Start HTTP & WebSocket Server
	srv := server.NewServer(hub, memStore)
	httpServer := &http.Server{
		Handler:      srv.Handler(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
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
		log.Printf("[Gateway] HTTP API & WebSocket ready at http://localhost:%s (WS: ws://localhost:%s/ws)", *port, *port)
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Gateway] Server failed: %v", err)
		}
	}()

	// 7. Wait for termination signals (Ctrl+C, Docker stop)
	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, os.Interrupt, syscall.SIGTERM)
	<-stopSig

	log.Println("\n[Gateway] Shutting down gracefully...")
	cancel() // Cancel background consumer and mock goroutines

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

func getEnvBool(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return strings.ToLower(val) == "true" || val == "1"
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
