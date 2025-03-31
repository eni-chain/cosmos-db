package db

import (
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type ShardedBatch struct {
	shards map[int]*leveldb.Batch
	db     *ShardedDB
}

func (s *ShardedDB) NewShardedBatch(n int) Batch {
	batches := &ShardedBatch{
		shards: make(map[int]*leveldb.Batch),
		db:     s,
	}
	for i := 0; i < len(s.shards); i++ {
		batches.shards[i] = leveldb.MakeBatch(n)
	}
	return batches
}

func (b *ShardedBatch) Set(key, value []byte) error {
	shardIndex := b.db.shardFunc(key)
	if _, ok := b.shards[shardIndex]; !ok {
		b.shards[shardIndex] = new(leveldb.Batch)
	}
	b.shards[shardIndex].Put(key, value)
	return nil
}

func (b *ShardedBatch) Delete(key []byte) error {
	shardIndex := b.db.shardFunc(key)
	if _, ok := b.shards[shardIndex]; !ok {
		b.shards[shardIndex] = new(leveldb.Batch)
	}
	b.shards[shardIndex].Delete(key)
	return nil
}

func (b *ShardedBatch) Write() error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(b.shards))

	for shardIdx, batch := range b.shards {
		wg.Add(1)
		go func(shardIdx int, batch *leveldb.Batch) {
			defer wg.Done()
			err := b.db.shards[shardIdx].Write(batch, nil)
			if err != nil {
				errChan <- err
			}
		}(shardIdx, batch)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *ShardedBatch) WriteSync() error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(b.shards))

	for shardIdx, batch := range b.shards {
		wg.Add(1)
		go func(shardIdx int, batch *leveldb.Batch) {
			defer wg.Done()
			err := b.db.shards[shardIdx].Write(batch, &opt.WriteOptions{Sync: true})
			if err != nil {
				errChan <- err
			}
		}(shardIdx, batch)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *ShardedBatch) Close() error {
	b.shards = nil
	return nil
}

func (b *ShardedBatch) GetByteSize() (int, error) {
	size := 0
	for _, batch := range b.shards {
		size += batch.Len()
	}
	return size, nil
}
