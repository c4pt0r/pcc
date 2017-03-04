package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ngaut/log"
	"github.com/syndtr/goleveldb/leveldb"
)

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

func loadRelationship(storePath, relationFile string) {
	log.Info("loading user relations...")
	levelDB := newLevelDBStore(storePath).(*levelDBStore)

	fp, err := os.Open(relationFile)
	if err != nil {
		log.Fatal(err)
	}
	rdr := bufio.NewReader(fp)
	cnt := 0

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

		key1 := fmt.Sprintf("%c%d/%d", Prefix_Rel, uid1, uid2)
		key2 := fmt.Sprintf("%c%d/%d", Prefix_Rel, uid2, uid1)

		// batch 2 key
		b.Put([]byte(key1), nil)
		b.Put([]byte(key2), nil)

		cnt++
		if b.Len() == 500 {
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

func loadLikes(storePath, likesFile string) {
	log.Info("loading likes...")
	levelDB := newLevelDBStore(storePath).(*levelDBStore)

	fp, err := os.Open(likesFile)
	if err != nil {
		log.Fatal(err)
	}
	rdr := bufio.NewReader(fp)
	cnt := 0

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
		//

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
			if b.Len() == 500 {
				levelDB.db.Write(b, nil)
				b = &leveldb.Batch{}
			}
		}

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
