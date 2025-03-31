package db

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
)

// cachingDB 是中间层，实现了 DB 接口，提供内存缓存功能。
type cachingDB struct {
	db        DB                    // 底层 LevelDB 实例
	mu        sync.RWMutex          // 读写锁保护缓存和关闭状态
	cache     map[string]*cacheItem // 内存缓存，存储未提交的修改
	batchSize int                   // 触发异步写入的批次大小
	closed    chan struct{}         // 关闭信号通道
}

func (c *cachingDB) Print() error {
	return c.db.Print()
}

func (c *cachingDB) Stats() map[string]string {
	return c.db.Stats()
}

// cacheItem 表示缓存中的一个条目，可能是设置的值或删除标记。
type cacheItem struct {
	value   []byte
	deleted bool
}

// NewCachingDB 创建一个新的缓存中间层实例。
func NewCachingDB(underlyingDB DB, batchSize int) DB {
	c := &cachingDB{
		db:        underlyingDB,
		cache:     make(map[string]*cacheItem),
		batchSize: batchSize,
		closed:    make(chan struct{}),
	}
	fmt.Println("NewCachingDB")
	go c.backgroundWriter() // 启动后台写入协程
	return c
}

// 后台异步写入处理
func (c *cachingDB) backgroundWriter() {
	for {
		select {
		case <-c.closed:
			c.flush(true) // 关闭时强制刷新
			return
		default:
			c.flush(false) // 定期或条件触发刷新
		}
	}
}

// flush 将当前缓存写入底层数据库
func (c *cachingDB) flush(sync bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cache) == 0 {
		return nil
	}

	batch := c.db.NewBatch()
	for key, cacheItem := range c.cache {
		k := []byte(key)
		if cacheItem.deleted {
			if err := batch.Delete(k); err != nil {
				return err
			}
		} else {
			if err := batch.Set(k, cacheItem.value); err != nil {
				return err
			}
		}
	}

	var err error
	if sync {
		err = batch.WriteSync()
	} else {
		err = batch.Write()
	}

	if err != nil {
		return err
	}

	// 成功写入后清空缓存
	c.cache = make(map[string]*cacheItem)
	return nil
}

// Get 实现 DB 接口
func (c *cachingDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}

	c.mu.RLock()
	it, exists := c.cache[string(key)]
	c.mu.RUnlock()

	if exists {
		if it.deleted {
			return nil, nil
		}
		return it.value, nil
	}

	return c.db.Get(key)
}

// Set 实现 DB 接口
func (c *cachingDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}

	c.mu.Lock()
	c.cache[string(key)] = &cacheItem{value: value}
	shouldFlush := len(c.cache) >= c.batchSize
	c.mu.Unlock()

	if shouldFlush {
		go c.flush(false)
	}
	return nil
}

// SetSync 实现 DB 接口
func (c *cachingDB) SetSync(key, value []byte) error {
	if err := c.Set(key, value); err != nil {
		return err
	}
	return c.flush(true)
}

// Delete 实现 DB 接口
func (c *cachingDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}

	c.mu.Lock()
	c.cache[string(key)] = &cacheItem{deleted: true}
	shouldFlush := len(c.cache) >= c.batchSize
	c.mu.Unlock()

	if shouldFlush {
		go c.flush(false)
	}
	return nil
}

// DeleteSync 实现 DB 接口
func (c *cachingDB) DeleteSync(key []byte) error {
	if err := c.Delete(key); err != nil {
		return err
	}
	return c.flush(true)
}

// 其他接口方法实现（部分示例）
func (c *cachingDB) Close() error {
	close(c.closed)
	return c.db.Close()
}

func (c *cachingDB) Has(key []byte) (bool, error) {
	value, err := c.Get(key)
	if err != nil {
		return false, err
	}
	return value != nil, nil
}

// Iterator 实现（合并缓存和底层数据）
func (c *cachingDB) Iterator(start, end []byte) (Iterator, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	underlyingIter, err := c.db.Iterator(start, end)
	if err != nil {
		return nil, err
	}

	var cacheEntries []*struct {
		key   []byte
		value []byte
	}
	for k, v := range c.cache {
		key := []byte(k)
		if inRange(key, start, end) {
			if !v.deleted {
				cacheEntries = append(cacheEntries, &struct {
					key   []byte
					value []byte
				}{key: key, value: v.value})
			}
		}
	}

	sort.Slice(cacheEntries, func(i, j int) bool {
		return bytes.Compare(cacheEntries[i].key, cacheEntries[j].key) < 0
	})

	return &mergedIterator{
		cache:      cacheEntries,
		underlying: underlyingIter,
	}, nil
}

// mergedIterator 合并缓存和底层迭代器的实现
type mergedIterator struct {
	cache      []*struct{ key, value []byte }
	underlying Iterator
	cachePos   int
	current    struct{ key, value []byte }
	valid      bool
}

func (it *mergedIterator) Domain() ([]byte, []byte) {
	return it.underlying.Domain()
}

func (it *mergedIterator) Valid() bool {
	return it.valid
}

// 迭代器核心逻辑（示例实现）
func (it *mergedIterator) Next() {
	// 实现合并逻辑，此处需要比较缓存和底层数据的当前键值
	// 此处为简化示例，实际需要处理各种边界情况
	it.valid = false
	if it.cachePos < len(it.cache) {
		it.current = *it.cache[it.cachePos]
		it.cachePos++
		it.valid = true
		return
	}

	if it.underlying.Valid() {
		it.current.key = it.underlying.Key()
		it.current.value = it.underlying.Value()
		it.underlying.Next()
		it.valid = true
	}
}

// 其他迭代器方法实现
func (it *mergedIterator) Key() []byte   { return it.current.key }
func (it *mergedIterator) Value() []byte { return it.current.value }
func (it *mergedIterator) Error() error  { return it.underlying.Error() }
func (it *mergedIterator) Close() error  { return it.underlying.Close() }

// 辅助函数：判断键是否在范围内
func inRange(key, start, end []byte) bool {
	return (start == nil || bytes.Compare(key, start) >= 0) &&
		(end == nil || bytes.Compare(key, end) < 0)
}

// 实现其他必要的接口方法（部分示例）
func (c *cachingDB) ReverseIterator(start, end []byte) (Iterator, error) {
	// 实现类似 Iterator 但方向相反
	// 此处省略具体实现
	return c.db.ReverseIterator(start, end)
}

func (c *cachingDB) NewBatch() Batch {
	return &cachingBatch{
		cacheDB: c,
		ops:     make([]batchOperation, 0),
	}
}

func (c *cachingDB) NewBatchWithSize(size int) Batch {
	return &cachingBatch{
		cacheDB: c,
		ops:     make([]batchOperation, 0, size),
	}
}

// cachingBatch 实现 Batch 接口
type cachingBatch struct {
	cacheDB *cachingDB
	ops     []batchOperation
	closed  bool
}

type batchOperation struct {
	key    []byte
	value  []byte
	delete bool
}

func (b *cachingBatch) Set(key, value []byte) error {
	if b.closed {
		return errBatchClosed
	}
	b.ops = append(b.ops, batchOperation{key, value, false})
	return nil
}

func (b *cachingBatch) Delete(key []byte) error {
	if b.closed {
		return errBatchClosed
	}
	b.ops = append(b.ops, batchOperation{key, nil, true})
	return nil
}

func (b *cachingBatch) Write() error {
	b.cacheDB.mu.Lock()
	defer b.cacheDB.mu.Unlock()

	for _, op := range b.ops {
		keyStr := string(op.key)
		if op.delete {
			b.cacheDB.cache[keyStr] = &cacheItem{deleted: true}
		} else {
			b.cacheDB.cache[keyStr] = &cacheItem{value: op.value}
		}
	}
	return nil
}

// 其他 Batch 方法实现
func (b *cachingBatch) WriteSync() error { return b.Write() }
func (b *cachingBatch) Close() error     { b.closed = true; return nil }
func (b *cachingBatch) GetByteSize() (int, error) {
	size := 0
	for _, op := range b.ops {
		size += len(op.key)
		if !op.delete {
			size += len(op.value)
		}
	}
	return size, nil
}
