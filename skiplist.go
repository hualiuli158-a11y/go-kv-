package main

import (
	"bufio"
	"cmp"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	maxLevel    = 32   // 跳表最大层数
	probability = 0.25 // 节点晋升的概率
)

// Node 跳表节点
type Node[K cmp.Ordered, V any] struct {
	Key     K
	Value   V
	Forward []*Node[K, V]
}

// SkipList 跳表结构
type SkipList[K cmp.Ordered, V any] struct {
	head     *Node[K, V]
	level    int
	randGen  *rand.Rand
	mu       sync.RWMutex
	nodePool sync.Pool

	aofFile *os.File
	aofChan chan string
}

// NewSkipList 初始化跳表
func NewSkipList[K cmp.Ordered, V any]() *SkipList[K, V] {
	source := rand.NewSource(time.Now().UnixNano())
	sl := &SkipList[K, V]{
		level:   1,
		randGen: rand.New(source),
	}

	sl.nodePool.New = func() any {
		return &Node[K, V]{
			Forward: make([]*Node[K, V], maxLevel),
		}
	}

	sl.head = sl.nodePool.Get().(*Node[K, V])
	sl.head.Forward = sl.head.Forward[:maxLevel]
	for i := 0; i < maxLevel; i++ {
		sl.head.Forward[i] = nil
	}

	// 1. 打开 AOF 文件
	file, err := os.OpenFile("aof.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("无法打开 AOF 文件: %v", err))
	}
	sl.aofFile = file

	// 2. 先从 AOF 恢复数据 (此时 aofChan 还是 nil，Insert 时不会触发重复写磁盘)
	sl.loadAOF()

	// 3. 恢复完成后，再初始化 Channel 并启动 Worker
	sl.aofChan = make(chan string, 100000)
	go sl.aofWorker()

	return sl
}

// loadAOF 逐行读取并重放指令
func (sl *SkipList[K, V]) loadAOF() {
	sl.aofFile.Seek(0, 0)
	scanner := bufio.NewScanner(sl.aofFile)

	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "SET":
			if len(parts) == 3 {
				// 这里的断言要求外部调用时泛型必须是 [string, string]
				sl.Insert(any(parts[1]).(K), any(parts[2]).(V))
				count++
			}
		case "DEL":
			if len(parts) == 2 {
				sl.Delete(any(parts[1]).(K))
				count++
			}
		}
	}
	fmt.Printf("AOF 恢复完成，共重放 %d 条指令\n", count)
}

// aofWorker 后台异步刷盘协程
func (sl *SkipList[K, V]) aofWorker() {
	for cmd := range sl.aofChan {
		_, err := sl.aofFile.WriteString(cmd + "\n")
		if err != nil {
			fmt.Printf("AOF 写入失败: %v\n", err)
		}
	}
}

// allocNode 从对象池获取一个节点并初始化
func (sl *SkipList[K, V]) allocNode(key K, value V, level int) *Node[K, V] {
	node := sl.nodePool.Get().(*Node[K, V])
	node.Key = key
	node.Value = value
	node.Forward = node.Forward[:level]
	for i := 0; i < level; i++ {
		node.Forward[i] = nil
	}
	return node
}

// freeNode 回收节点到对象池
func (sl *SkipList[K, V]) freeNode(node *Node[K, V]) {
	var zeroK K
	var zeroV V
	node.Key = zeroK
	node.Value = zeroV
	node.Forward = node.Forward[:maxLevel]
	for i := 0; i < maxLevel; i++ {
		node.Forward[i] = nil
	}
	sl.nodePool.Put(node)
}

// randomLevel 生成随机层数
func (sl *SkipList[K, V]) randomLevel() int {
	level := 1
	for sl.randGen.Float32() < probability && level < maxLevel {
		level++
	}
	return level
}

// Search 查找键值
func (sl *SkipList[K, V]) Search(key K) (V, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.Forward[i] != nil && curr.Forward[i].Key < key {
			curr = curr.Forward[i]
		}
	}

	curr = curr.Forward[0]
	if curr != nil && curr.Key == key {
		return curr.Value, true
	}

	var zero V
	return zero, false
}

// Insert 插入或更新键值对
func (sl *SkipList[K, V]) Insert(key K, value V) {
	sl.mu.Lock()

	update := make([]*Node[K, V], maxLevel)
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for curr.Forward[i] != nil && curr.Forward[i].Key < key {
			curr = curr.Forward[i]
		}
		update[i] = curr
	}

	curr = curr.Forward[0]

	if curr != nil && curr.Key == key {
		curr.Value = value
		sl.mu.Unlock() // 提前解锁，防止死锁
		if sl.aofChan != nil {
			sl.aofChan <- fmt.Sprintf("SET %v %v", key, value)
		}
		return
	}

	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := sl.allocNode(key, value, newLevel)
	for i := 0; i < newLevel; i++ {
		newNode.Forward[i] = update[i].Forward[i]
		update[i].Forward[i] = newNode
	}

	sl.mu.Unlock() // 释放写锁

	// Channel 发送放在锁外，不阻塞其他协程
	if sl.aofChan != nil {
		sl.aofChan <- fmt.Sprintf("SET %v %v", key, value)
	}
}

// Delete 删除节点
func (sl *SkipList[K, V]) Delete(key K) bool {
	sl.mu.Lock()

	update := make([]*Node[K, V], maxLevel)
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for curr.Forward[i] != nil && curr.Forward[i].Key < key {
			curr = curr.Forward[i]
		}
		update[i] = curr
	}

	curr = curr.Forward[0]

	if curr == nil || curr.Key != key {
		sl.mu.Unlock() // 没找到直接解锁返回
		return false
	}

	for i := 0; i < sl.level; i++ {
		if update[i].Forward[i] != curr {
			break
		}
		update[i].Forward[i] = curr.Forward[i]
	}

	for sl.level > 1 && sl.head.Forward[sl.level-1] == nil {
		sl.level--
	}

	sl.freeNode(curr)
	sl.mu.Unlock() // 释放锁

	// 确实删除了，发送日志
	if sl.aofChan != nil {
		sl.aofChan <- fmt.Sprintf("DEL %v", key)
	}
	return true
}
