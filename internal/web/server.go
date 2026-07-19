// Package web serves mithril-dash's dashboard: a single embedded HTML page,
// a Server-Sent-Events stream of the aggregated state, and a JSON history
// endpoint for the charts.
package web

import (
	"embed"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/stakingfacilities/mithril-dash/internal/store"
)

//go:embed dashboard.html
var assets embed.FS

var page = template.Must(template.ParseFS(assets, "dashboard.html"))

type Server struct {
	store *store.Store
}

func NewServer(s *store.Store) *Server {
	return &Server{store: s}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/history", s.handleHistory)
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := page.Execute(w, nil); err != nil {
		log.Printf("template execute: %v", err)
	}
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	writeSnapshot := func() bool {
		b, err := s.store.SnapshotJSON()
		if err != nil {
			return true
		}
		if _, err := w.Write([]byte("data: ")); err != nil {
			return false
		}
		if _, err := w.Write(b); err != nil {
			return false
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	if !writeSnapshot() {
		return
	}
	gen := s.store.Generation()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		gen = s.store.WaitForChange(gen, 30*time.Second)
		if !writeSnapshot() {
			return
		}
	}
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	kind := store.HistoryKind(r.URL.Query().Get("kind"))
	limit := 500
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	data := s.store.History(kind, limit)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("history encode: %v", err)
	}
}
