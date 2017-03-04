package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type levelDBStore struct {
	db *leveldb.DB
}

func newLevelDBStore(path string) Store {
	// use 1 gb block cache
	db, err := leveldb.OpenFile(path, &opt.Options{
		BlockCacheCapacity: 1 * opt.GiB,
	})
	if err != nil {
		panic(err)
	}
	return &levelDBStore{
		db: db,
	}
}

func (t *levelDBStore) Seek(keyPref []byte, batchSize int) []*KV {
	iter := t.db.NewIterator(util.BytesPrefix(keyPref), nil)
	cnt := 0
	var res []*KV
	for iter.Next() {
		if !iter.Valid() {
			break
		}
		res = append(res, &KV{
			K: append([]byte{}, iter.Key()...),
			V: append([]byte{}, iter.Value()...),
		})
		cnt++
		if cnt == batchSize {
			break
		}
	}
	iter.Release()
	return res
}

func (t *levelDBStore) Exists(key []byte) bool {
	b, _ := t.db.Has(key, nil)
	return b
}

func (t *levelDBStore) Put(kv *KV) error {
	return t.db.Put(kv.K, kv.V, nil)
}

func (t *levelDBStore) Get(key []byte) *KV {
	v, _ := t.db.Get(key, nil)
	return &KV{
		K: key,
		V: v,
	}
}

func (t *levelDBStore) Close() {
	t.db.Close()
}
