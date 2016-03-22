package redisc_test

import (
	"log"
	"time"

	"github.com/PuerkitoBio/juggler/redisc"
	"github.com/garyburd/redigo/redis"
)

// Example of how to create and use a Cluster.
func Example() {
	// create the cluster
	cluster := redisc.Cluster{
		StartupNodes: []string{":7000", ":7001", ":7002"},
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(5 * time.Second)},
		CreatePool:   createPool,
	}
	defer cluster.Close()

	// initialize its mapping
	if err := cluster.Refresh(); err != nil {
		log.Fatalf("Refresh failed: %v", err)
	}

	// grab a connection from the pool
	conn := cluster.Get()
	defer conn.Close()

	// call commands on it
	s, err := redis.String(conn.Do("GET", "some-key"))
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	log.Println(s)

	// grab a non-pooled connection
	conn2, err := cluster.Dial()
	if err != nil {
		log.Fatalf("Dial failed: %v", err)
	}
	defer conn2.Close()

	// make it handle redirections automatically
	rc, err := redisc.RetryConn(conn2)
	if err != nil {
		log.Fatalf("RetryConn failed: %v", err)
	}

	_, err = rc.Do("SET", "some-key", 2)
	if err != nil {
		log.Fatalf("SET failed: %v", err)
	}
}

func createPool(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
	return &redis.Pool{
		MaxIdle:     5,
		MaxActive:   10,
		IdleTimeout: time.Minute,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}, nil
}
