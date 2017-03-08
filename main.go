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
	flagSkip             = flag.Int("load-from", 0, "")
	logLevel             = flag.String("log-level", "info", "")
)

var nicknameMap = make(map[uint64]string)

func main() {
	flag.Parse()
	log.SetLevelByString(*logLevel)

	if len(*flagLoadRelationship) > 0 {
		loadRelationship(*flagStorePath, *flagLoadRelationship, *flagSkip)
		return
	}

	if len(*flagLoadLikes) > 0 {
		loadLikes(*flagRedisAddr, *flagStorePath, *flagLoadLikes, *flagSkip)
		return
	}

	// launch server
	loadNickname(*flagNicknames)
	hdlr := LikeHandler{
		localStore: newLevelDBStore(*flagStorePath),
		redisStore: redis.NewClient(&redis.Options{
			Addr: *flagRedisAddr,
		}),
	}

	log.Info("Listening http://0.0.0.0:8080/")
	err := http.ListenAndServe("0.0.0.0:8080", hdlr)
	if err != nil {
		log.Fatal(err)
	}
}
