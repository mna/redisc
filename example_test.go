package redisc_test

import (
	"fmt"
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

// Create and use a cluster.
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
	rc, err := redisc.RetryConn(conn2, 3, 100*time.Millisecond)
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

// Execute scripts.
func ExampleConn() {
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

	// create a script that takes 2 keys and 2 values, and returns 1
	var script = redis.NewScript(2, `
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[2], ARGV[2])
		return 1
	`)

	// get a connection from the cluster
	conn := cluster.Get()
	defer conn.Close()

	// bind it to the right node for the required keys, ahead of time
	if err := redisc.BindConn(conn, "scr{a}1", "src{a}2"); err != nil {
		log.Fatalf("BindConn failed: %v", err)
	}

	// script.Do, sends the whole script on first use
	v, err := script.Do(conn, "scr{a}1", "scr{a}2", "x", "y")
	if err != nil {
		log.Fatalf("script.Do failed: %v", err)
	}
	fmt.Println("Do returned ", v)

	// it is also possible to send only the hash, once it has been
	// loaded on that node
	if err := script.SendHash(conn, "scr{a}1", "scr{a}2", "x", "y"); err != nil {
		log.Fatalf("script.SendHash failed: %v", err)
	}
	if err := conn.Flush(); err != nil {
		log.Fatalf("Flush failed: %v", err)
	}

	// and receive the script's result
	v, err = conn.Receive()
	if err != nil {
		log.Fatalf("Receive failed: %v", err)
	}
	fmt.Println("Receive returned ", v)
}

// Automatically retry in case of redirection errors.
func ExampleRetryConn() {
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

	// get a connection from the cluster
	conn := cluster.Get()
	defer conn.Close()

	// create the retry connection - only Do, Close and Err are
	// supported on that connection. It will make up to 3 attempts
	// to get a valid response, and will wait 100ms before a retry
	// in case of a TRYAGAIN redis error.
	retryConn, err := redisc.RetryConn(conn, 3, 100*time.Millisecond)
	if err != nil {
		log.Fatalf("RetryConn failed: %v", err)
	}

	// call commands
	v, err := retryConn.Do("GET", "key")
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Println("GET returned ", v)
}
