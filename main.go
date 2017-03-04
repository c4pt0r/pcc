package main

import (
	"flag"
	"net/http"

	"github.com/ngaut/log"

	redis "gopkg.in/redis.v5"
)

var (
	flagLoadRelationship = flag.String("load-rel", "", "")
	flagLoadLikes        = flag.String("load-likes", "", "")
	flagRedisAddr        = flag.String("redis-addr", "127.0.0.1:6379", "")
	flagStorePath        = flag.String("store", "localstore", "")
	flagNicknames        = flag.String("nicknames", "nicknames", "")
	logLevel             = flag.String("log-level", "info", "")
)

var nicknameMap = make(map[uint64]string)

func main() {
	flag.Parse()
	log.SetLevelByString(*logLevel)

	loadNickname(*flagNicknames)

	if len(*flagLoadRelationship) > 0 {
		loadRelationship(*flagStorePath, *flagLoadRelationship)
		return
	}

	if len(*flagLoadLikes) > 0 {
		loadLikes(*flagStorePath, *flagLoadLikes)
		return
	}

	// launch server
	hdlr := LikeHandler{
		localStore: newLevelDBStore(*flagStorePath),
		redisStore: redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
	}

	log.Info("Listening http://0.0.0.0:8080/")
	err := http.ListenAndServe("0.0.0.0:8080", hdlr)
	if err != nil {
		log.Fatal(err)
	}
}
