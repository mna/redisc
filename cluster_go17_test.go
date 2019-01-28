// +build go1.7

package redisc

import (
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc/redistest"
	"github.com/stretchr/testify/assert"
)

// TestGetPoolTimedOut test case where we can't get the connection because the pool
// is full
func TestGetPoolTimedOut(t *testing.T) {
	s := redistest.StartMockServer(t, func(cmd string, args ...string) interface{} {
		return nil
	})
	defer s.Close()

	p := &redis.Pool{
		MaxActive: 1,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", s.Addr)
		},
		Wait: true,
	}
	c := Cluster{
		PoolWaitTime: 100 * time.Millisecond,
	}
	conn, err := c.getFromPool(p)
	assert.NoError(t, err)

	// second connection should be failed because we only have 1 MaxActive
	_, err = c.getFromPool(p)
	assert.Error(t, err)

	conn.Close()
}

// TestGetPoolGotOnFull test that we could get the connection when the pool
// is full and we can wait for it
func TestGetPoolGotOnFull(t *testing.T) {
	s := redistest.StartMockServer(t, func(cmd string, args ...string) interface{} {
		return nil
	})
	defer s.Close()

	var (
		usageTime = 100 * time.Millisecond // how long the connection will be used
		waitTime  = 3 * usageTime          // how long we want to wait
	)

	p := &redis.Pool{
		MaxActive: 1,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", s.Addr)
		},
		Wait: true,
	}
	c := Cluster{
		PoolWaitTime: waitTime,
	}

	// first connection OK
	conn, err := c.getFromPool(p)
	assert.NoError(t, err)

	// second connection should be failed because we only have 1 MaxActive
	_, err = c.getFromPool(p)
	assert.Error(t, err)

	go func() {
		time.Sleep(usageTime) // sleep before close, to simulate waiting for connection
		conn.Close()
	}()
	conn2, err := c.getFromPool(p)
	assert.NoError(t, err)

	conn2.Close()
}
