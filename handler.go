package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	redis "gopkg.in/redis.v5"
)

func errResp(w io.Writer, code int, msg string, oid, uid uint64) {
	payload, _ := json.Marshal(map[string]interface{}{
		"error_code":    code,
		"error_message": msg,
		"oid":           oid,
		"uid":           uid,
	})
	w.Write(payload)
}

type LikeHandler struct {
	localStore   Store
	remoteStores Store
	redisStore   *redis.Client
}

func (hdlr LikeHandler) isFriend(uid1, uid2 uint64) bool {
	key := fmt.Sprintf("%c%d/%d", Prefix_Rel, uid1, uid2)
	if hdlr.localStore.Exists([]byte(key)) {
		return true
	}
	return false
}

func (hdlr LikeHandler) doList(w http.ResponseWriter, oid, uid uint64, cursor string, pageSize, needFriend int) error {
	var start, prefix []byte
	prefix = []byte(fmt.Sprintf("%c%d/", Prefix_Like, oid))

	if len(cursor) > 0 {
		start = next([]byte(cursor))
	} else {
		start = prefix
	}

	// default batch size
	var res []uint64
	var lastKey []byte
L:
	for len(res) < pageSize {
		kvs := hdlr.localStore.Scan(start, 20, func(k []byte) bool {
			return !bytes.HasPrefix(k, prefix)
		})
		// no such object
		if len(kvs) == 0 {
			break L
		}
		// get uid
		for _, kv := range kvs {
			s := strings.Split(string(kv.K), "/")[1]
			r, _ := strconv.ParseUint(s, 10, 64)
			if needFriend == 0 || (needFriend == 1 && hdlr.isFriend(uid, r)) {
				res = append(res, r)
			}
			lastKey = kv.K
			if len(res) == pageSize {
				break L
			}
		}
		// update prefix
		start = next(lastKey)
	}

	var list []map[uint64]string
	for _, uid := range res {
		list = append(list, map[uint64]string{
			uid: nicknameMap[uid],
		})
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"oid":       oid,
		"like_list": list,
		"cursor":    string(lastKey),
	})
	w.Write(payload)
	return nil
}

func genLikeCountKey(oid uint64) string {
	return fmt.Sprintf("%c%d", Prefix_ObjectLikeCnt, oid)
}

func genRecentLikeKey(oid uint64) string {
	return fmt.Sprintf("%c%d", Prefix_ObjectRecentLike, oid)
}

func (hdlr LikeHandler) doLike(w http.ResponseWriter, oid, uid uint64) error {
	key := fmt.Sprintf("%c%d/%d", Prefix_Like, oid, uid)
	// check if already liked
	if hdlr.localStore.Exists([]byte(key)) {
		return errors.New("already liked")
	}
	// update like count
	ck := genLikeCountKey(oid)
	hdlr.redisStore.Incr(ck)

	// update recent like list
	rk := genRecentLikeKey(oid)
	hdlr.redisStore.LPush(rk, uid)
	// check if need to update redis recent list
	cnt, err := hdlr.redisStore.LLen(rk).Result()
	if err != nil {
		return err
	}
	if cnt > 20 {
		hdlr.redisStore.RPop(rk)
	}

	// load recent 20 users
	uidList, err := hdlr.redisStore.LRange(rk, 0, 20).Result()
	if err != nil {
		return err
	}
	// get nickname info
	var recentUserList []map[string]string
	for _, uid := range uidList {
		id, _ := strconv.ParseUint(uid, 10, 64)
		recentUserList = append(recentUserList, map[string]string{
			uid: nicknameMap[id],
		})
	}

	// async persist to disk
	go func() {
		hdlr.localStore.Put(&KV{K: []byte(key)})
		hdlr.remoteStores.Put(&KV{K: []byte(key)})
	}()

	// write response
	payload, _ := json.Marshal(map[string]interface{}{
		"oid":       oid,
		"uid":       uid,
		"like_list": recentUserList,
	})

	w.Write(payload)
	return nil
}

func (hdlr LikeHandler) doIsLike(w http.ResponseWriter, oid, uid uint64) error {
	key := []byte(fmt.Sprintf("%c%d/%d", Prefix_Like, oid, uid))
	m := map[string]interface{}{
		"oid": oid,
		"uid": uid,
	}
	if hdlr.localStore.Exists(key) {
		m["is_like"] = 1
	} else {
		m["is_like"] = 0
	}
	payload, _ := json.Marshal(m)
	w.Write(payload)
	return nil
}

func (hdlr LikeHandler) doCount(w http.ResponseWriter, oid uint64) error {
	ck := genLikeCountKey(oid)
	cnt, err := hdlr.redisStore.Get(ck).Int64()
	if err != nil {
		return err
	}
	// write payload
	payload, _ := json.Marshal(map[string]interface{}{
		"oid":   oid,
		"count": cnt,
	})

	w.Write(payload)
	return nil
}

func (hdlr LikeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	m, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		errResp(w, 500, "invlid url", 0, 0)
		return
	}
	// get action
	action := m.Get("action")
	// get oid
	oid, err := strconv.ParseUint(m.Get("oid"), 10, 64)
	if err != nil {
		errResp(w, 500, "invlid oid", 0, 0)
		return
	}
	// get uid, default 0
	uid, _ := strconv.ParseUint(m.Get("uid"), 10, 64)

	switch action {
	case "like":
		err := hdlr.doLike(w, oid, uid)
		if err != nil {
			errResp(w, 602, err.Error(), oid, uid)
			return
		}
	case "is_like":
		err := hdlr.doIsLike(w, oid, uid)
		if err != nil {
			errResp(w, 702, err.Error(), oid, uid)
			return
		}
	case "count":
		err := hdlr.doCount(w, oid)
		if err != nil {
			errResp(w, 802, err.Error(), oid, uid)
			return
		}
	case "list":
		cursor := m.Get("cursor")
		isFriend, _ := strconv.Atoi(m.Get("is_friend"))
		pageSize, _ := strconv.Atoi(m.Get("page_size"))
		if pageSize == 0 {
			pageSize = 10
		}
		err := hdlr.doList(w, oid, uid, cursor, pageSize, isFriend)
		if err != nil {
			errResp(w, 802, err.Error(), oid, uid)
			return
		}
	default:
		errResp(w, 502, "no such action", oid, uid)
		return
	}
}
