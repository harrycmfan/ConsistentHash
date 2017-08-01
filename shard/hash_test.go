package shard

import (
	"fmt"
	"strconv"
	"testing"
)

func TestConsistentHash(t *testing.T) {
	cHashRing := NewConsistentHash()

	for i := 0; i < 10; i++ {
		cHashRing.Add(*NewNode(i, "127.0.0.1", 3306, "paysicore", "ledger"+strconv.Itoa(i)))
	}
	fmt.Println(cHashRing.Nodes)

	nodeCount := make(map[string]int)
	for i := 0; i < 1000; i++ {
		node := cHashRing.Get(strconv.Itoa(i))
		if _, ok := nodeCount[node.String()]; ok {
			nodeCount[node.String()]++
		} else {
			nodeCount[node.String()] = 1
		}
	}

	for node, count := range nodeCount {
		fmt.Printf("Node: %s, Count %d\n", node, count)
	}
}
