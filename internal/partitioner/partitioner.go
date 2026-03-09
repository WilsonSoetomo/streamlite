package partitioner

import (
	"hash/fnv"
)

// Partition determines which partition a message should go to
// based on its key using consistent hashing.
func Partition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}
