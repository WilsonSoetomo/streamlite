package broker

import (
	"fmt"
	"sync"

	"github.com/wilsonsoetomo/streamlite/internal/partitioner"
	"github.com/wilsonsoetomo/streamlite/internal/protocol"
	"github.com/wilsonsoetomo/streamlite/internal/storage"
)

// Topic represents a topic with its partitions.
type Topic struct {
	Name       string
	Partitions int
	logs       map[int]*storage.PartitionLog
	mu         sync.RWMutex
}

// Broker manages topics and handles produce/fetch operations.
type Broker struct {
	dataDir string
	topics  map[string]*Topic
	mu      sync.RWMutex
}

// NewBroker creates a new broker instance.
func NewBroker(dataDir string) *Broker {
	return &Broker{
		dataDir: dataDir,
		topics:  make(map[string]*Topic),
	}
}

// CreateTopic creates a new topic with the specified number of partitions.
func (b *Broker) CreateTopic(name string, numPartitions int) error {
	if numPartitions <= 0 {
		return fmt.Errorf("number of partitions must be > 0")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	topic := &Topic{
		Name:       name,
		Partitions: numPartitions,
		logs:       make(map[int]*storage.PartitionLog),
	}

	// Open log files for all partitions
	for i := 0; i < numPartitions; i++ {
		log, err := storage.OpenPartitionLog(b.dataDir, name, i)
		if err != nil {
			// Close any logs we already opened
			for _, l := range topic.logs {
				l.Close()
			}
			return fmt.Errorf("failed to open partition %d: %w", i, err)
		}
		topic.logs[i] = log
	}

	b.topics[name] = topic
	return nil
}

// Produce writes a message to the appropriate partition.
func (b *Broker) Produce(topicName, key, value string) (int, int64, error) {
	b.mu.RLock()
	topic, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return 0, 0, fmt.Errorf("topic %s does not exist", topicName)
	}

	// Determine partition
	partition := partitioner.Partition(key, topic.Partitions)

	// Get partition log
	topic.mu.RLock()
	log, exists := topic.logs[partition]
	topic.mu.RUnlock()

	if !exists {
		return 0, 0, fmt.Errorf("partition %d does not exist", partition)
	}

	// Append message
	offset, err := log.Append(key, value)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to append message: %w", err)
	}

	return partition, offset, nil
}

// Fetch reads messages from a partition starting at the given offset.
func (b *Broker) Fetch(topicName string, partition int, offset int64, maxBytes int) ([]protocol.Message, int64, error) {
	b.mu.RLock()
	topic, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return nil, 0, fmt.Errorf("topic %s does not exist", topicName)
	}

	if partition < 0 || partition >= topic.Partitions {
		return nil, 0, fmt.Errorf("invalid partition %d (topic has %d partitions)", partition, topic.Partitions)
	}

	// Get partition log
	topic.mu.RLock()
	log, exists := topic.logs[partition]
	topic.mu.RUnlock()

	if !exists {
		return nil, 0, fmt.Errorf("partition %d does not exist", partition)
	}

	// Read messages
	messages, nextOffset, err := log.ReadFrom(offset, maxBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read from partition: %w", err)
	}

	return messages, nextOffset, nil
}

// Close closes all partition logs.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, topic := range b.topics {
		for _, log := range topic.logs {
			if err := log.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}
