package db

import (
	"bytes"
	"sort"
	"sync"
)

// cachingDB is a middleware layer that implements the DB interface and provides in-memory caching.
type cachingDB struct {
	db        DB                    // Underlying LevelDB instance
	mu        sync.RWMutex          // Read-write lock to protect cache and closed state
	cache     map[string]*cacheItem // In-memory cache to store uncommitted changes
	batchSize int                   // Batch size to trigger asynchronous writes
	closed    chan struct{}         // Channel to signal closure
}

func (c *cachingDB) Print() error {
	return c.db.Print()
}

func (c *cachingDB) Stats() map[string]string {
	return c.db.Stats()
}

// cacheItem represents an entry in the cache, which could be a set value or a delete marker.
type cacheItem struct {
	value   []byte
	deleted bool
}

// NewCachingDB creates a new caching middleware instance.
func NewCachingDB(underlyingDB DB, batchSize int) DB {
	c := &cachingDB{
		db:        underlyingDB,
		cache:     make(map[string]*cacheItem),
		batchSize: batchSize,
		closed:    make(chan struct{}),
	}
	go c.backgroundWriter() // Start the background writer goroutine
	return c
}

// Background asynchronous write handler
func (c *cachingDB) backgroundWriter() {
	for {
		select {
		case <-c.closed:
			c.flush(true) // Force flush on close
			return
		default:
			c.flush(false) // Periodic or condition-triggered flush
		}
	}
}

// flush writes the current cache to the underlying database
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

	// Clear the cache after successful write
	c.cache = make(map[string]*cacheItem)
	return nil
}

// Get implements the DB interface
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

// Set implements the DB interface
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

// SetSync implements the DB interface
func (c *cachingDB) SetSync(key, value []byte) error {
	if err := c.Set(key, value); err != nil {
		return err
	}
	return c.flush(true)
}

// Delete implements the DB interface
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

// DeleteSync implements the DB interface
func (c *cachingDB) DeleteSync(key []byte) error {
	if err := c.Delete(key); err != nil {
		return err
	}
	return c.flush(true)
}

// Other interface method implementations (partial examples)
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

// Iterator implementation (merging cache and underlying data)
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

// mergedIterator implementation merging cache and underlying iterator
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

// Core logic of the iterator (example implementation)
func (it *mergedIterator) Next() {
	// Implement merge logic, handling various edge cases
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

// Other iterator method implementations
func (it *mergedIterator) Key() []byte   { return it.current.key }
func (it *mergedIterator) Value() []byte { return it.current.value }
func (it *mergedIterator) Error() error  { return it.underlying.Error() }
func (it *mergedIterator) Close() error  { return it.underlying.Close() }

// Helper function: check if a key is within the range
func inRange(key, start, end []byte) bool {
	return (start == nil || bytes.Compare(key, start) >= 0) &&
		(end == nil || bytes.Compare(key, end) < 0)
}

// Implement other necessary interface methods (partial examples)
func (c *cachingDB) ReverseIterator(start, end []byte) (Iterator, error) {
	// Implement similar to Iterator but in reverse direction
	// Omitted for brevity
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

// cachingBatch implements the Batch interface
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

// Other Batch method implementations
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
