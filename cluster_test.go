package redisc

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PuerkitoBio/juggler/internal/redistest"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterRefreshNormalServer(t *testing.T) {
	cmd, port := redistest.StartServer(t, nil, "")
	defer cmd.Process.Kill()

	c := &Cluster{
		StartupNodes: []string{":" + port},
	}
	err := c.Refresh()
	if assert.Error(t, err, "Refresh") {
		assert.Contains(t, err.Error(), "redisc: all nodes failed", "expected error message")
	}
}

func TestClusterRefresh(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	for i, p := range ports {
		ports[i] = ":" + p
	}
	c := &Cluster{
		StartupNodes: ports,
	}

	err := c.Refresh()
	if assert.NoError(t, err, "Refresh") {
		var prev string
		pix := -1
		for ix, master := range c.mapping {
			if master != prev || ix == len(c.mapping)-1 {
				prev = master
				t.Logf("%5d: %s\n", ix, master)
				pix++
			}
			if assert.NotEmpty(t, master) {
				split := strings.Index(master, ":")
				assert.Contains(t, ports, master[split:], "expected master")
			}
		}
	}
}

func TestClusterClose(t *testing.T) {
	c := &Cluster{
		StartupNodes: []string{":6379"},
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(2 * time.Second)},
		CreatePool:   createPool,
	}
	assert.NoError(t, c.Close(), "Close")
	if err := c.Close(); assert.Error(t, err, "Close after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	if conn := c.Get(); assert.Error(t, conn.Err(), "Get after Close") {
		assert.Contains(t, conn.Err().Error(), "redisc: closed", "expected message")
	}
	if _, err := c.Dial(); assert.Error(t, err, "Dial after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	if err := c.Refresh(); assert.Error(t, err, "Refresh after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
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

type redisCmd struct {
	name   string
	args   redis.Args
	resp   interface{} // if resp is of type lenResult, asserts that there is a result at least this long
	errMsg string
}

type lenResult int

func TestCommands(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	cmdsPerGroup := map[string][]redisCmd{
		"cluster": {
			{"CLUSTER", redis.Args{"INFO"}, lenResult(10), ""},
			{"READONLY", nil, "OK", ""},
			{"READWRITE", nil, "OK", ""},
			{"CLUSTER", redis.Args{"COUNTKEYSINSLOT", 12345}, int64(0), ""},
			{"CLUSTER", redis.Args{"KEYSLOT", "a"}, int64(15495), ""},
			{"CLUSTER", redis.Args{"NODES"}, lenResult(100), ""},
		},
		"connection": {
			{"AUTH", redis.Args{"pwd"}, nil, "ERR Client sent AUTH, but no password is set"},
			{"ECHO", redis.Args{"a"}, []byte("a"), ""},
			{"PING", nil, "PONG", ""},
			{"SELECT", redis.Args{1}, nil, "ERR SELECT is not allowed in cluster mode"},
			{"QUIT", nil, "OK", ""},
		},
		"hashes": {
			{"HSET", redis.Args{"ha", "f1", "1"}, int64(1), ""},
			{"HLEN", redis.Args{"ha"}, int64(1), ""},
			{"HEXISTS", redis.Args{"ha", "f1"}, int64(1), ""},
			{"HDEL", redis.Args{"ha", "f1", "f2"}, int64(1), ""},
			{"HINCRBY", redis.Args{"hb", "f1", "1"}, int64(1), ""},
			{"HINCRBYFLOAT", redis.Args{"hb", "f2", "0.5"}, []byte("0.5"), ""},
			{"HKEYS", redis.Args{"hb"}, []interface{}{[]byte("f1"), []byte("f2")}, ""},
			{"HMGET", redis.Args{"hb", "f1", "f2"}, []interface{}{[]byte("1"), []byte("0.5")}, ""},
			{"HMSET", redis.Args{"hc", "f1", "a", "f2", "b"}, "OK", ""},
			{"HSET", redis.Args{"ha", "f1", "2"}, int64(1), ""},
			{"HGET", redis.Args{"ha", "f1"}, []byte("2"), ""},
			{"HGETALL", redis.Args{"ha"}, []interface{}{[]byte("f1"), []byte("2")}, ""},
			{"HSETNX", redis.Args{"ha", "f2", "3"}, int64(1), ""},
			//{"HSTRLEN", redis.Args{"hb", "f2"}, int64(3), ""}, // redis 3.2 only
			{"HVALS", redis.Args{"hb"}, []interface{}{[]byte("1"), []byte("0.5")}, ""},
			{"HSCAN", redis.Args{"hb", 0}, lenResult(2), ""},
		},
		"hyperloglog": {
			{"PFADD", redis.Args{"hll", "a", "b", "c"}, int64(1), ""},
			{"PFCOUNT", redis.Args{"hll"}, int64(3), ""},
			{"PFADD", redis.Args{"hll2", "d"}, int64(1), ""},
			{"PFMERGE", redis.Args{"hll", "hll2"}, nil, "CROSSSLOT Keys in request don't hash to the same slot"},
		},
		"keys": {
			{"SET", redis.Args{"k1", "z"}, "OK", ""},
			{"EXISTS", redis.Args{"k1"}, int64(1), ""},
			{"DUMP", redis.Args{"k1"}, lenResult(10), ""},
			{"EXPIRE", redis.Args{"k1", 10}, int64(1), ""},
			{"EXPIREAT", redis.Args{"k1", time.Now().Add(time.Hour).Unix()}, int64(1), ""},
			{"KEYS", redis.Args{"z*"}, []interface{}{}, ""}, // KEYS is supported, but uses a random node and returns keys from that node (undeterministic)
			{"MOVE", redis.Args{"k1", 2}, nil, "ERR MOVE is not allowed in cluster mode"},
			{"PERSIST", redis.Args{"k1"}, int64(1), ""},
			{"PEXPIRE", redis.Args{"k1", 10000}, int64(1), ""},
			{"PEXPIREAT", redis.Args{"k1", time.Now().Add(time.Hour).UnixNano() / int64(time.Millisecond)}, int64(1), ""},
			{"PTTL", redis.Args{"k1"}, lenResult(3500000), ""},
			// RANDOMKEY is not deterministic
			{"RENAME", redis.Args{"k1", "k2"}, nil, "CROSSSLOT Keys in request don't hash to the same slot"},
			{"RENAMENX", redis.Args{"k1", "k2"}, nil, "CROSSSLOT Keys in request don't hash to the same slot"},
			{"SCAN", redis.Args{0}, lenResult(2), ""}, // works, but only for the keys on that random node
			{"TTL", redis.Args{"k1"}, lenResult(3000), ""},
			{"TYPE", redis.Args{"k1"}, "string", ""},
			{"DEL", redis.Args{"k1"}, int64(1), ""},
		},
		"lists": {
			{"LPUSH", redis.Args{"l1", "a", "b", "c"}, int64(3), ""},
			{"LINDEX", redis.Args{"l1", 1}, []byte("b"), ""},
			{"LINSERT", redis.Args{"l1", "BEFORE", "b", "d"}, int64(4), ""},
			{"LLEN", redis.Args{"l1"}, int64(4), ""},
			{"LPOP", redis.Args{"l1"}, []byte("c"), ""},
			{"LPUSHX", redis.Args{"l1", "e"}, int64(4), ""},
			{"LRANGE", redis.Args{"l1", 0, 1}, []interface{}{[]byte("e"), []byte("d")}, ""},
			{"LREM", redis.Args{"l1", 0, "d"}, int64(1), ""},
			{"LSET", redis.Args{"l1", 0, "f"}, "OK", ""},
			{"LTRIM", redis.Args{"l1", 0, 3}, "OK", ""},
			{"RPOP", redis.Args{"l1"}, []byte("a"), ""},
			{"RPOPLPUSH", redis.Args{"l1", "l2"}, nil, "CROSSSLOT Keys in request don't hash to the same slot"},
			{"RPUSH", redis.Args{"l1", "g"}, int64(3), ""},
			{"RPUSH", redis.Args{"l1", "h"}, int64(4), ""},
			{"BLPOP", redis.Args{"l1", 1}, lenResult(2), ""},
			{"BRPOP", redis.Args{"l1", 1}, lenResult(2), ""},
			{"BRPOPLPUSH", redis.Args{"l1", "l2", 1}, nil, "CROSSSLOT Keys in request don't hash to the same slot"},
		},
		"pubsub": {
			{"PUBSUB", redis.Args{"NUMPAT"}, lenResult(0), ""},
			{"PUBLISH", redis.Args{"ev1", "a"}, lenResult(0), ""},
		},
		"scripting": {
			{"SCRIPT", redis.Args{"FLUSH"}, "OK", ""},
			{"SCRIPT", redis.Args{"EXISTS", "return GET x"}, []interface{}{int64(0)}, ""},
			// to actually use scripts with keys, conn.Bind must be called to select the right node
		},
		"server": {
			{"CLIENT", redis.Args{"LIST"}, lenResult(10), ""},
		},
	}

	for i, p := range ports {
		ports[i] = ":" + p
	}
	c := &Cluster{
		StartupNodes: ports,
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(2 * time.Second)},
		CreatePool:   createPool,
	}
	require.NoError(t, c.Refresh(), "Refresh")

	var wg sync.WaitGroup
	// start a goroutine that subscribes and listens to events
	ok := make(chan int)
	done := make(chan int)
	go runPubSubCommands(t, c, ok, done)

	<-ok
	wg.Add(len(cmdsPerGroup))
	for _, cmds := range cmdsPerGroup {
		go runCommands(t, c, cmds, &wg)
	}
	wg.Add(1)
	go runScriptCommands(t, c, &wg)

	wg.Wait()
	close(done)
	<-ok
}

func runPubSubCommands(t *testing.T, c *Cluster, steps, stop chan int) {
	conn, err := c.Dial()
	require.NoError(t, err, "Dial for PubSub")
	psc := redis.PubSubConn{Conn: conn}
	assert.NoError(t, psc.PSubscribe("ev*"), "PSubscribe")
	assert.NoError(t, psc.Subscribe("e1"), "Subscribe")

	// allow commands to start running
	steps <- 1

	var received bool
loop:
	for {
		select {
		case <-stop:
			break loop
		default:
		}

		v := psc.Receive()
		switch v := v.(type) {
		case redis.PMessage:
			if !assert.Equal(t, []byte("a"), v.Data, "Received value") {
				t.Logf("%T", v)
			}
			received = true
			break loop
		}
	}

	<-stop

	assert.NoError(t, psc.Unsubscribe("e1"), "Unsubscribe")
	assert.NoError(t, psc.PUnsubscribe("ev*"), "PUnsubscribe")
	assert.NoError(t, psc.Close(), "Close for PubSub")
	assert.True(t, received, "Did receive event")
	steps <- 1
}

func runScriptCommands(t *testing.T, c *Cluster, wg *sync.WaitGroup) {
	defer wg.Done()

	var script = redis.NewScript(2, `
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[2], ARGV[2])
		return 1
	`)

	conn := c.Get()
	defer conn.Close()
	if conn, ok := conn.(*Conn); ok {
		require.NoError(t, conn.Bind("scr{a}1", "src{a}2"), "Bind")
	}

	// script.Do, send the whole script
	v, err := script.Do(conn, "scr{a}1", "scr{a}2", "x", "y")
	assert.NoError(t, err, "Do script")
	assert.Equal(t, int64(1), v, "Script result")

	// send only the hash, should work because the script is now loaded on this node
	assert.NoError(t, script.SendHash(conn, "scr{a}1", "scr{a}2", "x", "y"), "SendHash")
	assert.NoError(t, conn.Flush(), "Flush")
	v, err = conn.Receive()
	assert.NoError(t, err, "SendHash Receive")
	assert.Equal(t, int64(1), v, "SendHash Script result")

	// do with keys from different slots
	v, err = script.Do(conn, "scr{a}1", "scr{b}2", "x", "y")
	if assert.Error(t, err, "Do script invalid keys") {
		assert.Contains(t, err.Error(), "CROSSSLOT", "Do script invalid keys")
	}
}

func runCommands(t *testing.T, c *Cluster, cmds []redisCmd, wg *sync.WaitGroup) {
	defer wg.Done()

	for _, cmd := range cmds {
		conn := c.Get()
		res, err := conn.Do(cmd.name, cmd.args...)
		if cmd.errMsg != "" {
			if assert.Error(t, err, cmd.name) {
				assert.Contains(t, err.Error(), cmd.errMsg, cmd.name)
			}
		} else {
			assert.NoError(t, err, cmd.name)
			if lr, ok := cmd.resp.(lenResult); ok {
				switch res := res.(type) {
				case []byte:
					assert.True(t, len(res) >= int(lr), "result has at least %d bytes, has %d", lr, len(res))
				case []interface{}:
					assert.Equal(t, int(lr), len(res), "result array has %d items, has %d", lr, len(res))
				case int64:
					assert.True(t, res >= int64(lr), "result is at least %d, is %d", lr, res)
				default:
					t.Errorf("unexpected result type %T", res)
				}
			} else {
				if !assert.Equal(t, cmd.resp, res, cmd.name) {
					t.Logf("%T vs %T", cmd.resp, res)
				}
			}
		}
		require.NoError(t, conn.Close(), "Close")
	}
}
