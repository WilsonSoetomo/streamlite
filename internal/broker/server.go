package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/wilsonsoetomo/streamlite/internal/protocol"
)

// Server wraps the broker with HTTP handlers.
type Server struct {
	broker *Broker
}

// NewServer creates a new HTTP server for the broker.
func NewServer(broker *Broker) *Server {
	return &Server{broker: broker}
}

// Start starts the HTTP server on the given address.
func (s *Server) Start(addr string) error {
	http.HandleFunc("/topics", s.handleTopics)
	http.HandleFunc("/produce", s.handleProduce)
	http.HandleFunc("/fetch", s.handleFetch)

	log.Printf("Broker starting on %s", addr)
	return http.ListenAndServe(addr, nil)
}

// requireJSON checks Content-Type and returns false with 415 if not application/json.
func requireJSON(w http.ResponseWriter, r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.Contains(ct, "application/json") {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return false
	}
	return true
}

// handleTopics handles topic creation.
func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireJSON(w, r) {
		return
	}

	var req protocol.CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "field \"name\" is required and must be a non-empty string", http.StatusBadRequest)
		return
	}

	if err := s.broker.CreateTopic(req.Name, req.Partitions); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := protocol.CreateTopicResponse{
		Name:       req.Name,
		Partitions: req.Partitions,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleProduce handles message production.
func (s *Server) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !requireJSON(w, r) {
		return
	}

	var req protocol.ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Topic == "" {
		http.Error(w, "field \"topic\" is required and must be a non-empty string", http.StatusBadRequest)
		return
	}
	if req.Topic == "application/json" {
		http.Error(w, "field \"topic\" must be the topic name (e.g. \"my-topic\"), not the Content-Type. Use a JSON body: {\"topic\": \"my-topic\", \"key\": \"...\", \"value\": \"...\"}", http.StatusBadRequest)
		return
	}

	partition, offset, err := s.broker.Produce(req.Topic, req.Key, req.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := protocol.ProduceResponse{
		Topic:     req.Topic,
		Partition: partition,
		Offset:    offset,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleFetch handles message fetching.
func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	topic := r.URL.Query().Get("topic")
	partitionStr := r.URL.Query().Get("partition")
	offsetStr := r.URL.Query().Get("offset")
	maxBytesStr := r.URL.Query().Get("max_bytes")

	if topic == "" || partitionStr == "" || offsetStr == "" {
		http.Error(w, "Missing required parameters: topic, partition, offset", http.StatusBadRequest)
		return
	}

	var partition int
	var offset int64
	var maxBytes int

	if _, err := fmt.Sscanf(partitionStr, "%d", &partition); err != nil {
		http.Error(w, "Invalid partition: "+err.Error(), http.StatusBadRequest)
		return
	}

	if _, err := fmt.Sscanf(offsetStr, "%d", &offset); err != nil {
		http.Error(w, "Invalid offset: "+err.Error(), http.StatusBadRequest)
		return
	}

	if maxBytesStr != "" {
		if _, err := fmt.Sscanf(maxBytesStr, "%d", &maxBytes); err != nil {
			http.Error(w, "Invalid max_bytes: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	messages, nextOffset, err := s.broker.Fetch(topic, partition, offset, maxBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := protocol.FetchResponse{
		Topic:      topic,
		Partition:  partition,
		Messages:   messages,
		NextOffset: nextOffset,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
