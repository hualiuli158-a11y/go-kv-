package main

import (
	"cmp"
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel    = 32   // 跳表最大层数
	probability = 0.25 // 节点晋升的概率 (通常为 1/4)
)

// Node 跳表节点，K 需要支持比较大小，V 可以是任意类型
type Node[K cmp.Ordered, V any] struct {
	Key     K
	Value   V
	Forward []*Node[K, V] // 替代了复杂的 Node**，直接用切片管理多层指针
}

// newNode 创建新节点
func newNode[K cmp.Ordered, V any](key K, value V, level int) *Node[K, V] {
	return &Node[K, V]{
		Key:     key,
		Value:   value,
		Forward: make([]*Node[K, V], level),
	}
}

// SkipList 跳表结构
type SkipList[K cmp.Ordered, V any] struct {
	head    *Node[K, V]
	level   int // 当前最大有效层数
	randGen *rand.Rand
	mu      sync.RWMutex // 2. 在跳表主体中加入读写锁
}

// NewSkipList 初始化跳表
func NewSkipList[K cmp.Ordered, V any]() *SkipList[K, V] {
	source := rand.NewSource(time.Now().UnixNano())
	return &SkipList[K, V]{
		// 头节点不存储实际数据，层数直接拉满
		head:    newNode(*new(K), *new(V), maxLevel),
		level:   1,
		randGen: rand.New(source),
	}
}

// randomLevel 生成随机层数
func (sl *SkipList[K, V]) randomLevel() int {
	level := 1
	// 每次有 25% 的概率晋升到下一层
	for sl.randGen.Float32() < probability && level < maxLevel {
		level++
	}
	return level
}

// Search 查找键值，返回 Value 和 是否存在的布尔值
func (sl *SkipList[K, V]) Search(key K) (V, bool) {
	sl.mu.RLock()         // 1. 加读锁（多个 Goroutine 可以同时获取读锁进入该临界区）
	defer sl.mu.RUnlock() // 2. 函数执行完毕或发生异常退出时，自动释放读锁

	curr := sl.head
	// 从最高层开始往下找
	for i := sl.level - 1; i >= 0; i-- {
		// 在当前层向右遍历，直到遇到大于等于目标 key 的节点
		for curr.Forward[i] != nil && curr.Forward[i].Key < key {
			curr = curr.Forward[i]
		}
	}

	// 降到第 0 层，curr 的下一个节点就是可能的目标
	curr = curr.Forward[0]
	if curr != nil && curr.Key == key {
		return curr.Value, true
	}

	var zero V // 返回零值
	return zero, false
}

// Insert 插入或更新键值对
func (sl *SkipList[K, V]) Insert(key K, value V) {
	sl.mu.Lock()         // 1. 加写锁（排他锁，其他读写操作必须等待）
	defer sl.mu.Unlock() // 2. 函数退出时自动释放写锁
	// update 数组用于记录每一层向下拐弯的节点
	update := make([]*Node[K, V], maxLevel)
	curr := sl.head

	// 1. 寻找插入位置并记录路径
	for i := sl.level - 1; i >= 0; i-- {
		for curr.Forward[i] != nil && curr.Forward[i].Key < key {
			curr = curr.Forward[i]
		}
		update[i] = curr
	}

	curr = curr.Forward[0]

	// 2. 如果 Key 已存在，直接更新 Value
	if curr != nil && curr.Key == key {
		curr.Value = value
		return
	}

	// 3. 生成新节点的随机层数
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		// 如果新层数超过了当前跳表的最大层数，超出的部分由 head 指向它
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	// 4. 创建新节点并调整指针
	newNode := newNode(key, value, newLevel)
	for i := 0; i < newLevel; i++ {
		newNode.Forward[i] = update[i].Forward[i]
		update[i].Forward[i] = newNode
	}
}

// Delete 删除节点
func (sl *SkipList[K, V]) Delete(key K) bool {
	sl.mu.Lock()         // 1. 加写锁
	defer sl.mu.Unlock() // 2. 函数退出时自动释放写锁
	update := make([]*Node[K, V], maxLevel)
	curr := sl.head

	// 1. 寻找待删除节点并记录路径
	for i := sl.level - 1; i >= 0; i-- {
		for curr.Forward[i] != nil && curr.Forward[i].Key < key {
			curr = curr.Forward[i]
		}
		update[i] = curr
	}

	curr = curr.Forward[0]

	// 如果找不到目标节点，直接返回
	if curr == nil || curr.Key != key {
		return false
	}

	// 2. 调整指针，绕过该节点
	for i := 0; i < sl.level; i++ {
		if update[i].Forward[i] != curr {
			break
		}
		update[i].Forward[i] = curr.Forward[i]
	}

	// 3. 更新跳表的当前有效层数 (如果最高层变空了，就降级)
	for sl.level > 1 && sl.head.Forward[sl.level-1] == nil {
		sl.level--
	}

	// 注意：这里不需要手动 delete(curr)，一旦没有指针指向它，Go 的 GC 会自动回收它。
	return true
}
