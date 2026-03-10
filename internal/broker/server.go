package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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

// handleTopics handles topic creation.
func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
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

	var req protocol.ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
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
