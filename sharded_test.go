package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestShardedLevelDBNewShardedDB(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)
	dirs := []string{"dir1", "dir2", "dir3"}
	// Test we can't open the db twice for writing
	wr1, err := NewShardedDB(name, dirs, nil)
	require.Nil(t, err)
	defer func() {
		wr1.Close()
		for _, dir := range dirs {
			//rm dir
			os.RemoveAll(dir)
		}
	}()
	_, err = NewShardedDB(name, dirs, nil)
	require.NotNil(t, err)
	wr1.Close() // Close the db to release the lock

	// Test we can open the db twice for reading only
	ro1, err := NewShardedDBWithOpts(name, dirs, &opt.Options{ReadOnly: true})
	require.Nil(t, err)
	defer ro1.Close()
	ro2, err := NewShardedDBWithOpts(name, dirs, &opt.Options{ReadOnly: true})
	require.Nil(t, err)
	defer ro2.Close()
}

func BenchmarkShardedLevelDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dirs := []string{"dir1", "dir2", "dir3"}

	db, err := NewShardedDB(name, dirs, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		for _, dir := range dirs {
			os.RemoveAll(dir)
		}
	}()

	benchmarkRandomReadsWrites(b, db)
}

func BenchmarkShardedRangeScans1M(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	dirs := []string{"dir1", "dir2", "dir3"}

	db, err := NewShardedDB(name, dirs, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		for _, dir := range dirs {
			os.RemoveAll(dir)
		}
	}()

	benchmarkRangeScans(b, db, int64(1e6))
}
