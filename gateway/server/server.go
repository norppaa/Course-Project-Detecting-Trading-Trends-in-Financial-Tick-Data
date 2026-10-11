package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"trading-trends/gateway/store"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024 * 64,
	WriteBufferSize: 1024 * 64,
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins for dev/dashboard flexibility
		return true
	},
}

// Server coordinates HTTP endpoints and WebSocket connections.
type Server struct {
	hub       *Hub
	store     *store.MemoryStore
	startTime time.Time
}

// NewServer initializes a new Server.
func NewServer(hub *Hub, st *store.MemoryStore) *Server {
	return &Server{
		hub:       hub,
		store:     st,
		startTime: time.Now(),
	}
}

// Handler returns the HTTP handler with all registered routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// WebSocket endpoint
	mux.HandleFunc("/ws", s.handleWebSocket)

	// REST API endpoints
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/symbols", s.handleSymbols)
	mux.HandleFunc("/api/history", s.handleHistory)
	mux.HandleFunc("/api/alerts", s.handleAlerts)
	mux.HandleFunc("/api/snapshot", s.handleSnapshot)

	// Static frontend file server (searches local directory or parent directory)
	staticDir := s.resolveStaticDir()
	if staticDir != "" {
		fs := http.FileServer(http.Dir(staticDir))
		mux.Handle("/", fs)
		log.Printf("[Server] Serving static dashboard from %s", staticDir)
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintf(w, "Trading Trends Gateway is running. Connect to WebSocket at /ws")
		})
	}

	return s.withCORS(mux)
}

func (s *Server) resolveStaticDir() string {
	candidates := []string{
		"./frontend",
		"../frontend",
		"/frontend",
		"/usr/share/nginx/html",
	}
	for _, dir := range candidates {
		if info, err := os.Stat(filepath.Join(dir, "index.html")); err == nil && !info.IsDir() {
			return dir
		}
	}
	return ""
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Server] WebSocket upgrade failed: %v", err)
		return
	}

	client := NewClient(s.hub, conn)
	s.hub.register <- client

	go client.WritePump()
	go client.ReadPump()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":         "ok",
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"active_clients": s.hub.ClientCount(),
		"timestamp":      time.Now().UTC(),
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.store.GetStats()
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"store_stats":    stats,
		"active_clients": s.hub.ClientCount(),
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"timestamp":      time.Now().UTC(),
	})
}

func (s *Server) handleSymbols(w http.ResponseWriter, r *http.Request) {
	symbols := s.store.GetSymbols()
	s.writeJSON(w, http.StatusOK, symbols)
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		http.Error(w, "missing 'symbol' parameter", http.StatusBadRequest)
		return
	}

	limit := 200
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = l
		}
	}

	history := s.store.GetHistory(symbol, limit)
	s.writeJSON(w, http.StatusOK, history)
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 {
			limit = l
		}
	}

	alerts := s.store.GetLatestAlerts(limit)
	s.writeJSON(w, http.StatusOK, alerts)
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshot := s.store.GetSnapshot()
	s.writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

