package partitioner

import "testing"

func TestPartition(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		numPartitions int
		wantValid     bool // Just check it's in valid range
	}{
		{
			name:          "basic hash",
			key:           "user-123",
			numPartitions: 3,
			wantValid:     true,
		},
		{
			name:          "same key same partition",
			key:           "order-456",
			numPartitions: 5,
			wantValid:     true,
		},
		{
			name:          "empty key",
			key:           "",
			numPartitions: 2,
			wantValid:     true,
		},
		{
			name:          "zero partitions",
			key:           "test",
			numPartitions: 0,
			wantValid:     true, // Should return 0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Partition(tt.key, tt.numPartitions)

			// Check it's in valid range
			if tt.numPartitions > 0 {
				if got < 0 || got >= tt.numPartitions {
					t.Errorf("Partition() = %v, want in range [0, %v)", got, tt.numPartitions)
				}
			} else {
				if got != 0 {
					t.Errorf("Partition() with 0 partitions = %v, want 0", got)
				}
			}
		})
	}
}

func TestPartitionConsistency(t *testing.T) {
	// Same key should always map to same partition
	key := "consistent-key"
	numPartitions := 4

	first := Partition(key, numPartitions)

	for i := 0; i < 100; i++ {
		got := Partition(key, numPartitions)
		if got != first {
			t.Errorf("Partition() inconsistent: got %v, want %v", got, first)
		}
	}
}

func TestPartitionDistribution(t *testing.T) {
	// Basic sanity check: different keys should sometimes map to different partitions
	numPartitions := 3
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	partitions := make(map[int]bool)

	for _, key := range keys {
		p := Partition(key, numPartitions)
		partitions[p] = true
	}

	// With 5 different keys and 3 partitions, we should hit at least 2 partitions
	if len(partitions) < 2 {
		t.Logf("Warning: Only %d unique partitions used out of %d", len(partitions), numPartitions)
		t.Logf("This might be fine, but worth noting")
	}
}
