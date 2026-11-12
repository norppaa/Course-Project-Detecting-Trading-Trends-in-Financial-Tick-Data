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
		// Allow all origins (the dashboard may be served by nginx on another port)
		return true
	},
}

// Server coordinates HTTP endpoints and WebSocket connections.
type Server struct {
	hub       *Hub
	store     *store.MemoryStore
	startTime time.Time
	// ConsumerStats, if set, is included in /api/stats.
	ConsumerStats func() any
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
	mux.HandleFunc("/api/markets", s.handleMarkets)
	mux.HandleFunc("/api/history", s.handleHistory)
	mux.HandleFunc("/api/alerts", s.handleAlerts)
	mux.HandleFunc("/api/snapshot", s.handleSnapshot)

	// Static frontend file server
	staticDir := s.resolveStaticDir()
	if staticDir != "" {
		fs := http.FileServer(http.Dir(staticDir))
		mux.Handle("/", noCache(fs))
		log.Printf("[Server] Serving static dashboard from %s", staticDir)
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintf(w, "Trading Trends Gateway is running. Connect to WebSocket at /ws")
		})
	}

	return s.withCORS(mux)
}

// noCache makes browsers revalidate the dashboard files on every load so a
// changed app.js / index.html is picked up without a hard refresh.
func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) resolveStaticDir() string {
	candidates := []string{
		"./frontend",
		"../frontend",
		"/app/frontend",
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
	resp := map[string]interface{}{
		"store_stats":    s.store.GetStats(),
		"hub_stats":      s.hub.Stats(),
		"active_clients": s.hub.ClientCount(),
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"timestamp":      time.Now().UTC(),
	}
	if s.ConsumerStats != nil {
		resp["consumer_stats"] = s.ConsumerStats()
	}
	s.writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleSymbols(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.store.GetSymbols())
}

func (s *Server) handleMarkets(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.store.GetMarkets(queryInt(r, "limit", 0)))
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		http.Error(w, "missing 'symbol' parameter", http.StatusBadRequest)
		return
	}
	s.writeJSON(w, http.StatusOK, s.store.GetHistory(symbol, queryInt(r, "limit", 200)))
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	warmup := true
	if v := r.URL.Query().Get("warmup"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			warmup = b
		}
	}
	s.writeJSON(w, http.StatusOK, s.store.GetAlerts(queryInt(r, "limit", 50), warmup))
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, s.store.GetSnapshot())
}

func queryInt(r *http.Request, key string, def int) int {
	if v := r.URL.Query().Get(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
