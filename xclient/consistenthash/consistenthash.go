package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type ConsistentHash struct {
	// 虚拟节点倍数
	replicas int
	// 虚拟节点的哈希值，排序好的
	keys []uint32
	// 虚拟节点与真实节点的对应关系
	// key 是虚拟节点的哈希值，value 是真实节点
	virtualMap map[uint32]string
}

func New(replicas int) *ConsistentHash {
	return &ConsistentHash{
		replicas:   replicas,
		keys:       make([]uint32, 0),
		virtualMap: make(map[uint32]string),
	}
}

func (c *ConsistentHash) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < c.replicas; i++ {
			hash := crc32.ChecksumIEEE([]byte(strconv.Itoa(i) + key))
			c.keys = append(c.keys, hash)
			c.virtualMap[hash] = key
		}
	}
	//  可以采用插入排序
	sort.Slice(c.keys, func(i, j int) bool {
		return c.keys[i] < c.keys[j]
	})
}

func (c *ConsistentHash) Get(key string) string {
	if len(c.keys) == 0 {
		return ""
	}
	hash := crc32.ChecksumIEEE([]byte(key))
	// 二分查找
	idx := sort.Search(len(c.keys), func(i int) bool {
		return c.keys[i] >= hash
	})
	if idx == len(c.keys) {
		idx = 0
	}
	return c.virtualMap[c.keys[idx]]
}

func (c *ConsistentHash) Delete(key string) {
	if len(key) == 0 {
		return
	}
	// 二分查找
	for i := 0; i < c.replicas; i++ {
		hash := crc32.ChecksumIEEE([]byte(strconv.Itoa(i) + key))
		idx := sort.Search(len(c.keys), func(i int) bool {
			return c.keys[i] >= hash
		})
		if idx == len(c.keys) {
			idx = 0
		}
		if c.keys[idx] != hash {
			continue
		}
		c.keys = append(c.keys[:idx], c.keys[idx+1:]...)
		delete(c.virtualMap, hash)
	}
}
