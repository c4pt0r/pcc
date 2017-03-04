package main

/*
type btreeStore struct {
	tree *btree.BTree
}

func (kv *KV) Less(than btree.Item) bool {
	return bytes.Compare(kv.K, than.(*KV).K) < 0
}

func newBtreeStore() Store {
	return &btreeStore{
		tree: btree.New(2),
	}
}

func (t *btreeStore) Scan(keyPref []byte, batchSize int) []*KV {
	var res []*KV
	i := 0
	t.tree.AscendGreaterOrEqual(&KV{K: keyPref}, func(kv btree.Item) bool {
		if i == batchSize {
			return false
		}
		res = append(res, kv.(*KV))
		return true
	})
	return res
}

func (t *btreeStore) Exists(key []byte) bool {
	return t.tree.Has(&KV{K: key})
}

func (t *btreeStore) Put(kv *KV) error {
	if !t.Exists(kv.K) {
		t.tree.ReplaceOrInsert(kv)
		return nil
	}
	return errors.New("key exists")
}

func (t *btreeStore) Get(key []byte) *KV {
	if r := t.tree.Get(&KV{K: key}); r != nil {
		return r.(*KV)
	}
	return nil
}
*/
