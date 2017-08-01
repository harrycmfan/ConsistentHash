package shard

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
)

const (
	virtualNodeNum = 10
)

type HashRing []uint32

func (hr HashRing) Len() int {
	return len(hr)
}

func (hr HashRing) Less(i, j int) bool {
	return hr[i] < hr[j]
}

func (hr HashRing) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

type Node struct {
	ID    int
	IP    string
	Port  int
	DB    string
	Table string
}

func (n *Node) String() string {
	return fmt.Sprintf("%d:%s:%d/%s.%s", n.ID, n.IP, n.Port, n.DB, n.Table)
}

func NewNode(id int, ip string, port int, db, table string) *Node {
	return &Node{
		ID:    id,
		IP:    ip,
		Port:  port,
		DB:    db,
		Table: table,
	}
}

type ConsistentHash struct {
	Nodes     map[uint32]Node
	Resources map[int]bool
	hashRing  HashRing
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		Nodes:     make(map[uint32]Node),
		Resources: make(map[int]bool),
		hashRing:  HashRing{},
	}
}

func (c *ConsistentHash) Add(node Node) bool {
	if _, ok := c.Resources[node.ID]; ok {
		return false
	}
	for i := 0; i < virtualNodeNum; i++ {
		hashPos := c.hashStr(node.String() + "-" + strconv.Itoa(i))
		c.Nodes[hashPos] = node
		c.hashRing = append(c.hashRing, hashPos)
		sort.Sort(c.hashRing)
	}
	c.Resources[node.ID] = true
	return true
}

func (c *ConsistentHash) Get(key string) Node {
	i := sort.Search(len(c.hashRing), func(i int) bool { return c.hashRing[i] >= c.hashStr(key) })
	if i == len(c.hashRing) {
		i = 0
	}
	return c.Nodes[c.hashRing[i]]
}

func (c *ConsistentHash) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}
