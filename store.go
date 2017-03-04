package main

import "fmt"

const (
	Prefix_Like             = 'l'
	Prefix_ObjectLikeCnt    = 'c'
	Prefix_Rel              = 'r'
	Prefix_UserInfo         = 'u'
	Prefix_ObjectRecentLike = 'o'
)

type KV struct {
	K []byte
	V []byte
}

func (kv *KV) String() string {
	return fmt.Sprintf("%s=%s", string(kv.K), string(kv.V))
}

type Store interface {
	Scan(start []byte, batchSize int, fnShouldStop func(k []byte) bool) []*KV
	Exists(key []byte) bool
	Put(kv *KV) error
	Get(key []byte) *KV
}
