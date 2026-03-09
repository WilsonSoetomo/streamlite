package protocol

import "time"

// Message represents a single message in a partition log.
type Message struct {
	Offset    int64     `json:"offset"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// CreateTopicRequest is used to create a new topic.
type CreateTopicRequest struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

// CreateTopicResponse confirms topic creation.
type CreateTopicResponse struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

// ProduceRequest contains a message to produce to a topic.
type ProduceRequest struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ProduceResponse returns the partition and offset where the message was stored.
type ProduceResponse struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// FetchRequest specifies which messages to retrieve.
type FetchRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	MaxBytes  int    `json:"max_bytes,omitempty"` // Optional limit
}

// FetchResponse contains the fetched messages.
type FetchResponse struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Messages  []Message `json:"messages"`
	NextOffset int64    `json:"next_offset"` // Where to fetch from next
}
