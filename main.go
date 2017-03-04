package main

import (
	"log"
	"net/http"

	redis "gopkg.in/redis.v5"
)

func main() {
	hdlr := LikeHandler{
		localStore: newLevelDBStore("./localStore"),
		redisStore: redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
	}
	log.Printf("Listening http://0.0.0.0:8080/")
	err := http.ListenAndServe("0.0.0.0:8080", hdlr)
	if err != nil {
		log.Fatal(err)
	}
}
