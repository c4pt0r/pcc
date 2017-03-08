package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	redis "gopkg.in/redis.v5"

	"github.com/ngaut/log"
	"github.com/syndtr/goleveldb/leveldb"
)

// next returns the next key in byte-order.
func next(b []byte) []byte {
	// add 0x0 to the end of key
	buf := make([]byte, len([]byte(b))+1)
	copy(buf, []byte(b))
	return buf
}

// PrefixNext returns the next prefix key.
//
// Assume there are keys like:
//
//   rowkey1
//   rowkey1_column1
//   rowkey1_column2
//   rowKey2
//
// If we seek 'rowkey1' Next, we will get 'rowkey1_colum1'.
// If we seek 'rowkey1' PrefixNext, we will get 'rowkey2'.
func prefixNext(k []byte) []byte {
	buf := make([]byte, len([]byte(k)))
	copy(buf, []byte(k))
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		copy(buf, k)
		buf = append(buf, 0)
	}
	return buf
}

func loadNickname(path string) {
	log.Info("loading nicknames...")
	fp, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	rdr := bufio.NewReader(fp)
	cnt := 0
	for {
		l, _, err := rdr.ReadLine()
		if err != nil {
			break
		}
		ll := strings.TrimSpace(string(l))
		if len(ll) == 0 {
			continue
		}
		// split file
		parts := strings.Split(ll, ",")

		uid, _ := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		nickname := strings.TrimSpace(parts[1])
		nicknameMap[uid] = nickname

		cnt++
		if cnt%50000 == 0 {
			log.Info("loading...", cnt)
		}
	}

	log.Info("done", cnt)
}

func skipNLines(rdr *bufio.Reader, n int) error {
	cnt := 0
	for cnt < n {
		var prefix bool
		var err error
		for {
			_, prefix, err = rdr.ReadLine()
			if prefix == false {
				break
			}
		}
		if err != nil {
			return err
		}
		cnt++
	}
	return nil
}

func loadRelationship(storePath, relationFile string, skip int) {
	log.Info("loading user relations...")
	levelDB := newLevelDBStore(storePath).(*levelDBStore)

	fp, err := os.Open(relationFile)
	if err != nil {
		log.Fatal(err)
	}

	rdr := bufio.NewReader(fp)
	if skip > 0 {
		skipNLines(rdr, skip)
	}
	cnt := skip
	b := &leveldb.Batch{}
	for {
		l, _, err := rdr.ReadLine()
		if err != nil {
			break
		}

		ll := strings.TrimSpace(string(l))
		if len(ll) == 0 {
			continue
		}

		// split file
		parts := strings.Split(ll, ",")
		uid1 := strings.TrimSpace(parts[0])
		uid2 := strings.TrimSpace(parts[1])

		key1 := fmt.Sprintf("%c%s/%s", Prefix_Rel, uid1, uid2)
		key2 := fmt.Sprintf("%c%s/%s", Prefix_Rel, uid2, uid1)

		// batch 2 key
		b.Put([]byte(key1), nil)
		b.Put([]byte(key2), nil)

		cnt++
		if b.Len() == 100000 {
			log.Info("loading...", cnt)
			levelDB.db.Write(b, nil)
			b = &leveldb.Batch{}
		}
	}

	if b.Len() > 0 {
		levelDB.db.Write(b, nil)
	}

	log.Info("done", cnt)
	levelDB.Close()
}

func loadLikes(redisAddr, storePath, likesFile string, skip int) {
	log.Info("loading likes...")
	levelDB := newLevelDBStore(storePath).(*levelDBStore)

	fp, err := os.Open(likesFile)
	if err != nil {
		log.Fatal(err)
	}

	rdr := bufio.NewReader(fp)
	if skip > 0 {
		skipNLines(rdr, skip)
	}
	cnt := skip

	redisCli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	b := &leveldb.Batch{}
	for {
		var l, tmp []byte
		var prefix bool
		var err error
		for {
			tmp, prefix, err = rdr.ReadLine()
			l = append(l, tmp...)
			if prefix == false {
				break
			}
		}
		if err != nil {
			break
		}

		ll := strings.TrimSpace(string(l))
		if len(ll) == 0 {
			continue
		}

		parts := strings.Split(ll, ":")
		oid := strings.TrimSpace(parts[0])
		ulist := strings.TrimSpace(parts[1])

		if ulist[0] != '[' || ulist[len(ulist)-1] != ']' {
			log.Fatal("error format", ulist)
		}

		uidlist := strings.Split(ulist[1:len(ulist)-1], ",")
		for _, uid := range uidlist {
			uid = strings.TrimSpace(uid)
			if len(uid) == 0 {
				continue
			}

			log.Debug("writing", oid, uid)
			b.Put([]byte(fmt.Sprintf("%c%s/%s", Prefix_Like, oid, uid)), nil)
			if b.Len() == 50000 {
				levelDB.db.Write(b, nil)
				b = &leveldb.Batch{}
			}
		}

		// update like count
		redisCli.IncrBy(fmt.Sprintf("%c%s", Prefix_ObjectLikeCnt, oid), int64(len(uidlist)))

		if b.Len() > 0 {
			levelDB.db.Write(b, nil)
			b = &leveldb.Batch{}
		}

		cnt++
		if cnt%500 == 0 {
			log.Info("loading...", cnt)
		}
	}
	if b.Len() > 0 {
		levelDB.db.Write(b, nil)
	}
	log.Info("done", cnt)
	levelDB.Close()
}
