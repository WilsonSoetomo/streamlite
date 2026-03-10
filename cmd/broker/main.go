package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/wilsonsoetomo/streamlite/internal/broker"
)

func main() {
	var (
		addr    = flag.String("addr", ":8080", "Address to listen on")
		dataDir = flag.String("data-dir", "./data", "Directory for partition log files")
	)
	flag.Parse()

	// Create broker
	b := broker.NewBroker(*dataDir)
	defer b.Close()

	// Create and start server
	s := broker.NewServer(b)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := s.Start(*addr); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	log.Printf("Broker running. Press Ctrl+C to stop.")
	<-sigChan
	log.Println("Shutting down...")
}
