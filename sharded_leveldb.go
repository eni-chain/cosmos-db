package db

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/spf13/cast"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type ShardedDB struct {
	shards    []*leveldb.DB    // LevelDB instances
	shardFunc func([]byte) int // Shard function
	mu        sync.Mutex       // Mutex
}

var _ DB = (*ShardedDB)(nil)

func NewShardedDBWithOpts(name string, dirs []string, o *opt.Options) (*ShardedDB, error) {
	dbPaths := make([]string, len(dirs))
	for i, dir := range dirs {
		dbPath := filepath.Join(dir, name+DBFileSuffix)
		dbPaths[i] = dbPath
	}
	numShards := len(dbPaths)
	shardFunc := HashShardFunc(numShards)
	// Open all shards with options
	shards := make([]*leveldb.DB, numShards)
	for i := 0; i < numShards; i++ {
		db, err := leveldb.OpenFile(dbPaths[i], o)
		if err != nil {
			// Close any opened shards
			for j := 0; j < i; j++ {
				shards[j].Close()
			}
			return nil, err
		}
		shards[i] = db
	}

	return &ShardedDB{
		shards:    shards,
		shardFunc: shardFunc,
	}, nil
}
func NewShardedDB(name string, dirs []string, opts Options) (*ShardedDB, error) {
	defaultOpts := &opt.Options{
		Filter: filter.NewBloomFilter(10), // by default, goleveldb doesn't use a bloom filter.
	}
	if opts != nil {
		files := cast.ToInt(opts.Get("maxopenfiles"))
		if files > 0 {
			defaultOpts.OpenFilesCacheCapacity = files
		}
	}

	return NewShardedDBWithOpts(name, dirs, defaultOpts)
}

func (s *ShardedDB) Get(key []byte) ([]byte, error) {
	shardIndex := s.shardFunc(key)
	v, err := s.shards[shardIndex].Get(key, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return v, err
}

func (s *ShardedDB) Set(key, value []byte) error {
	shardIndex := s.shardFunc(key)
	return s.shards[shardIndex].Put(key, value, nil)
}

func (s *ShardedDB) Delete(key []byte) error {
	shardIndex := s.shardFunc(key)
	return s.shards[shardIndex].Delete(key, nil)
}

func (s *ShardedDB) Has(key []byte) (bool, error) {
	shardIndex := s.shardFunc(key)
	return s.shards[shardIndex].Has(key, nil)
}

func (s *ShardedDB) SetSync(bytes []byte, bytes2 []byte) error {
	shardIndex := s.shardFunc(bytes)
	return s.shards[shardIndex].Put(bytes, bytes2, &opt.WriteOptions{Sync: true})
}

func (s *ShardedDB) DeleteSync(bytes []byte) error {
	shardIndex := s.shardFunc(bytes)
	return s.shards[shardIndex].Delete(bytes, &opt.WriteOptions{Sync: true})
}

func (s *ShardedDB) Close() error {
	for _, db := range s.shards {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardedDB) NewBatch() Batch {
	return s.NewShardedBatch(0)
}

func (s *ShardedDB) NewBatchWithSize(i int) Batch {
	return s.NewShardedBatch(i)
}

func (s *ShardedDB) Print() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, db := range s.shards {
		fmt.Printf("Shard #%d\n", i)
		if err := print(db); err != nil {
			return err
		}
	}
	return nil
}
func print(db *leveldb.DB) error {
	str, err := db.GetProperty("leveldb.stats")
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", str)

	itr := db.NewIterator(nil, nil)
	for itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}
func (s *ShardedDB) Stats() map[string]string {
	result := make(map[string]string)
	for i, db := range s.shards {
		st := stats(db)
		for k, v := range st {
			result[fmt.Sprintf("[#%d]%s", i, k)] = v
		}
	}
	return result
}
func stats(db *leveldb.DB) map[string]string {
	keys := []string{
		"leveldb.num-files-at-level{n}",
		"leveldb.stats",
		"leveldb.sstables",
		"leveldb.blockpool",
		"leveldb.cachedblock",
		"leveldb.openedtables",
		"leveldb.alivesnaps",
		"leveldb.aliveiters",
	}

	st := make(map[string]string)
	for _, key := range keys {
		str, err := db.GetProperty(key)
		if err == nil {
			st[key] = str
		}
	}
	return st
}
func (s *ShardedDB) Iterator(start, end []byte) (Iterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var iters []iterator.Iterator
	for _, db := range s.shards {
		iter := db.NewIterator(&util.Range{
			Start: start,
			Limit: end,
		}, nil)
		iters = append(iters, iter)
	}

	mergedIter := iterator.NewMergedIterator(iters, comparer.DefaultComparer, true)
	return newGoLevelDBIterator(mergedIter, start, end, false), nil
}
func (s *ShardedDB) ReverseIterator(start, end []byte) (Iterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var iters []iterator.Iterator
	for _, db := range s.shards {
		iter := db.NewIterator(nil, nil)
		iters = append(iters, iter)
	}

	mergedIter := iterator.NewMergedIterator(iters, comparer.DefaultComparer, true)
	return newGoLevelDBIterator(mergedIter, start, end, true), nil

}

// NewShardedBatch creates a new sharded batch.  FNV-1a
func HashShardFunc(numShards int) func([]byte) int {
	return func(key []byte) int {
		hash := uint32(2166136261)
		for _, b := range key {
			hash ^= uint32(b)
			hash *= 16777619
		}
		return int(hash % uint32(numShards))
	}
}
