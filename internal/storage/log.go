package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/wilsonsoetomo/streamlite/internal/protocol"
)

// PartitionLog manages an append-only log file for a single partition.
type PartitionLog struct {
	topic     string
	partition int
	filePath  string
	file      *os.File
	writer    *bufio.Writer
	mu        sync.Mutex
	nextOffset int64
}

// OpenPartitionLog opens or creates a partition log file.
func OpenPartitionLog(dataDir, topic string, partition int) (*PartitionLog, error) {
	// Create topic directory if it doesn't exist
	topicDir := filepath.Join(dataDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create topic directory: %w", err)
	}

	// Partition log file path
	filePath := filepath.Join(topicDir, fmt.Sprintf("partition-%d.log", partition))

	// Open file in append mode (creates if doesn't exist)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	log := &PartitionLog{
		topic:     topic,
		partition: partition,
		filePath:  filePath,
		file:      file,
		writer:    bufio.NewWriter(file),
	}

	// Scan existing file to determine next offset
	if err := log.scanForNextOffset(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to scan log file: %w", err)
	}

	return log, nil
}

// scanForNextOffset reads through the log file to find the next offset.
func (l *PartitionLog) scanForNextOffset() error {
	// Seek to beginning
	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(l.file)
	var lastOffset int64 = -1

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var msg protocol.Message
		if err := json.Unmarshal(line, &msg); err != nil {
			// Skip malformed lines (could be partial writes)
			continue
		}

		if msg.Offset > lastOffset {
			lastOffset = msg.Offset
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Next offset is lastOffset + 1 (or 0 if file was empty)
	l.nextOffset = lastOffset + 1

	// Seek back to end for appending
	if _, err := l.file.Seek(0, 2); err != nil {
		return err
	}

	return nil
}

// Append writes a message to the log and returns its offset.
func (l *PartitionLog) Append(key, value string) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset := l.nextOffset
	msg := protocol.Message{
		Offset:    offset,
		Key:       key,
		Value:     value,
		Timestamp: protocol.Now(),
	}

	// Marshal to JSON line
	data, err := json.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal message: %w", err)
	}

	// Write line
	if _, err := l.writer.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write message: %w", err)
	}

	if _, err := l.writer.WriteString("\n"); err != nil {
		return 0, fmt.Errorf("failed to write newline: %w", err)
	}

	// Flush to ensure durability
	if err := l.writer.Flush(); err != nil {
		return 0, fmt.Errorf("failed to flush: %w", err)
	}

	// Sync to disk for durability
	if err := l.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync: %w", err)
	}

	l.nextOffset++
	return offset, nil
}

// ReadFrom reads messages starting from the given offset.
func (l *PartitionLog) ReadFrom(offset int64, maxBytes int) ([]protocol.Message, int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Seek to beginning
	if _, err := l.file.Seek(0, 0); err != nil {
		return nil, 0, err
	}

	var messages []protocol.Message
	var bytesRead int
	nextOffset := offset

	scanner := bufio.NewScanner(l.file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		bytesRead += len(line) + 1 // +1 for newline

		var msg protocol.Message
		if err := json.Unmarshal(line, &msg); err != nil {
			// Skip malformed lines
			continue
		}

		// Only include messages at or after the requested offset
		if msg.Offset >= offset {
			messages = append(messages, msg)
			nextOffset = msg.Offset + 1

			// Stop if we've hit maxBytes limit
			if maxBytes > 0 && bytesRead >= maxBytes {
				break
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}

	return messages, nextOffset, nil
}

// Close closes the log file.
func (l *PartitionLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.writer != nil {
		if err := l.writer.Flush(); err != nil {
			return err
		}
	}

	if l.file != nil {
		return l.file.Close()
	}

	return nil
}
