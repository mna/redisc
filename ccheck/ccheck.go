package main

import (
	"flag"
	"time"

	"github.com/PuerkitoBio/redisc"
	"github.com/garyburd/redigo/redis"
)

var (
	addrFlag         = flag.String("addr", "localhost:7000", "Redis server `address`.")
	connTimeoutFlag  = flag.Duration("c", time.Second, "Connection `timeout`.")
	readTimeoutFlag  = flag.Duration("r", 100*time.Millisecond, "Read `timeout`.")
	writeTimeoutFlag = flag.Duration("w", 100*time.Millisecond, "Write `timeout`.")
	maxIdleFlag      = flag.Int("max-idle", 10, "Maximum idle `connections` per pool.")
	maxActiveFlag    = flag.Int("max-active", 100, "Maximum active `connections` per pool.")
	idleTimeoutFlag  = flag.Duration("i", 30*time.Second, "Pooled connection idle `timeout`.")
)

func main() {
	flag.Parse()

	cluster := &redisc.Cluster{
		StartupNodes: []string{*addrFlag},
		DialOptions: []redis.DialOption{
			redis.DialConnectTimeout(*connTimeoutFlag),
			redis.DialReadTimeout(*readTimeoutFlag),
			redis.DialWriteTimeout(*writeTimeoutFlag),
		},
		CreatePool: createPool,
	}
	defer cluster.Close()
}

func createPool(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxActive:   *maxActiveFlag,
		MaxIdle:     *maxIdleFlag,
		IdleTimeout: *idleTimeoutFlag,
	}, nil
}
