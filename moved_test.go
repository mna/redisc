package redisc

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc/redistest"
	"github.com/stretchr/testify/require"
)

// This tests the effect of a MOVED on a cluster with pools.
func TestMoved(t *testing.T) {
	var clientCount, poolCount int

	createPool := func(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
		t.Log("create pool called for ", addr)
		poolCount++

		return &redis.Pool{
			MaxIdle:     5,
			MaxActive:   10,
			IdleTimeout: time.Minute,
			Dial: func() (redis.Conn, error) {
				clientCount++
				opts = append(opts, redis.DialClientName("redisc-test-"+strconv.Itoa(clientCount)))
				return redis.Dial("tcp", addr, opts...)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		}, nil
	}

	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	for i, p := range ports {
		ports[i] = "127.0.0.1:" + p
	}
	c := &Cluster{
		StartupNodes: []string{ports[0]},
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(2 * time.Second)},
		CreatePool:   createPool,
	}
	require.NoError(t, c.Refresh(), "Refresh")

	t.Logf("Stats before first connection: %#v", c.Stats())

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := c.Get()
			defer conn.Close()

			for k := 'A'; k <= 'z'; k++ {
				if _, err := conn.Do("GET", string(k)); err != nil {
					re := ParseRedir(err)
					require.True(t, re.Type == "MOVED")
					//t.Logf("key %s moved: %v", string(k), err)
				}
			}
		}()
	}

	wg.Wait()

	// wait for refreshing to become false again
	c.mu.Lock()
	for c.refreshing {
		c.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		c.mu.Lock()
	}
	c.mu.Unlock()

	stats := c.Stats()
	var activeConns int
	for _, v := range stats {
		activeConns += v.ActiveCount
	}

	t.Logf("Stats at the end: %#v", c.Stats())

	// list the connections *as seen from the redis nodes*
	var redisConnsCount int
	for _, addr := range ports {
		func() {
			conn, err := redis.Dial("tcp", addr, redis.DialClientName("redisc-test-client-list"))
			require.NoError(t, err)
			defer conn.Close()

			t.Log(addr)
			res, err := redis.String(conn.Do("CLIENT", "LIST"))
			require.NoError(t, err)
			fields := strings.Fields(res)
			for _, f := range fields {
				if strings.HasPrefix(f, "id=") {
					t.Log(f)
				} else if strings.HasPrefix(f, "name=") {
					t.Log(f)
					if !strings.HasSuffix(f, "redisc-test-client-list") {
						redisConnsCount++
					}
				}
			}
		}()
	}

	t.Log("Final client count: ", clientCount, ", Stats total active conns: ", activeConns, ", Redis-reported conns: ", redisConnsCount)
	t.Log("Final pool count (some pools may have been created and dropped immediately, never used, due to concurrency): ", poolCount)

	require.Equal(t, activeConns, clientCount)
	require.Equal(t, activeConns, redisConnsCount)
}
