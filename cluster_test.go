package redisc

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc/redistest"
	"github.com/mna/redisc/redistest/resp"
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

func assertMapping(t *testing.T, mapping [hashSlots][]string, masterPorts, replicaPorts []string) {
	var prev string
	pix := -1
	expectedMappingNodes := 1 // at least a master node
	if len(replicaPorts) > 0 {
		// if there are replicase, then we expected 2 mapping nodes (master+replica)
		expectedMappingNodes = 2
	}
	for ix, maps := range mapping {
		if assert.Equal(t, expectedMappingNodes, len(maps), "Mapping has %d node(s)", expectedMappingNodes) {
			if maps[0] != prev || ix == len(mapping)-1 {
				prev = maps[0]
				t.Logf("%5d: %s\n", ix, maps[0])
				pix++
			}
			if assert.NotEmpty(t, maps[0]) {
				split := strings.Index(maps[0], ":")
				assert.Contains(t, masterPorts, maps[0][split+1:], "expected master")
			}
			if len(maps) > 1 && assert.NotEmpty(t, maps[1]) {
				split := strings.Index(maps[1], ":")
				assert.Contains(t, replicaPorts, maps[1][split+1:], "expected replica")
			}
		}
	}
}

func TestClusterRefresh(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	c := &Cluster{
		StartupNodes: []string{":" + ports[0]},
	}

	err := c.Refresh()
	if assert.NoError(t, err, "Refresh") {
		assertMapping(t, c.mapping, ports, nil)
	}
}

func TestClusterRefreshStartWithReplica(t *testing.T) {
	fn, ports := redistest.StartClusterWithReplicas(t, nil)
	defer fn()

	c := &Cluster{
		StartupNodes: []string{":" + ports[len(ports)-1]}, // last port is a replica
	}
	err := c.Refresh()
	if assert.NoError(t, err, "Refresh") {
		assertMapping(t, c.mapping, ports[:redistest.NumClusterNodes], ports[redistest.NumClusterNodes:])
	}
}

func TestClusterRefreshAllFail(t *testing.T) {
	s := redistest.StartMockServer(t, func(cmd string, args ...string) interface{} {
		return resp.Error("nope")
	})
	defer s.Close()

	c := &Cluster{
		StartupNodes: []string{s.Addr},
	}
	if err := c.Refresh(); assert.Error(t, err, "Refresh") {
		assert.Contains(t, err.Error(), "all nodes failed", "expected message")
	}
	require.NoError(t, c.Close(), "Close")
}

func TestClusterNoNode(t *testing.T) {
	c := &Cluster{}
	conn := c.Get()
	_, err := conn.Do("A")
	if assert.Error(t, err, "Do") {
		assert.Contains(t, err.Error(), "failed to get a connection", "expected message")
	}
	if err := BindConn(conn); assert.Error(t, err, "Bind without key") {
		assert.Contains(t, err.Error(), "failed to get a connection", "expected message")
	}
	if err := BindConn(conn, "A"); assert.Error(t, err, "Bind with key") {
		assert.Contains(t, err.Error(), "failed to get a connection", "expected message")
	}
}

func TestClusterNeedsRefresh(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	for i, p := range ports {
		ports[i] = ":" + p
	}
	c := &Cluster{
		StartupNodes: ports,
	}
	defer c.Close()

	conn := c.Get().(*Conn)
	defer conn.Close()

	// at this point, no mapping is stored
	c.mu.Lock()
	for i, v := range c.mapping {
		if !assert.Empty(t, v, "No addr for %d", i) {
			break
		}
	}
	c.mu.Unlock()

	// calling Do may or may not generate a MOVED error (it will get a
	// random node, because no mapping is known yet)
	conn.Do("GET", "b")

	// wait for refreshing to become false again
	c.mu.Lock()
	for c.refreshing {
		c.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		c.mu.Lock()
	}
	for i, v := range c.mapping {
		if !assert.NotEmpty(t, v, "Addr for %d", i) {
			break
		}
	}
	c.mu.Unlock()
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
			{"PFMERGE", redis.Args{"hll", "hll2"}, nil, "CROSSSLOT"},
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
			{"RENAME", redis.Args{"k1", "k2"}, nil, "CROSSSLOT"},
			{"RENAMENX", redis.Args{"k1", "k2"}, nil, "CROSSSLOT"},
			{"SCAN", redis.Args{0}, lenResult(2), ""}, // works, but only for the keys on that random node
			{"TTL", redis.Args{"k1"}, lenResult(3000), ""},
			{"TYPE", redis.Args{"k1"}, "string", ""},
			{"DEL", redis.Args{"k1"}, int64(1), ""},
			{"SADD", redis.Args{"k3", "a", "z", "d"}, int64(3), ""},
			{"SORT", redis.Args{"k3", "ALPHA"}, []interface{}{[]byte("a"), []byte("d"), []byte("z")}, ""},
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
			{"RPOPLPUSH", redis.Args{"l1", "l2"}, nil, "CROSSSLOT"},
			{"RPUSH", redis.Args{"l1", "g"}, int64(3), ""},
			{"RPUSH", redis.Args{"l1", "h"}, int64(4), ""},
			{"BLPOP", redis.Args{"l1", 1}, lenResult(2), ""},
			{"BRPOP", redis.Args{"l1", 1}, lenResult(2), ""},
			{"BRPOPLPUSH", redis.Args{"l1", "l2", 1}, nil, "CROSSSLOT"},
		},
		"pubsub": {
			{"PUBSUB", redis.Args{"NUMPAT"}, lenResult(0), ""},
			{"PUBLISH", redis.Args{"ev1", "a"}, lenResult(0), ""},
			// to actually subscribe to events, only Send must be called, and Receive to listen (or redis.PubSubConn must be used)
		},
		"scripting": {
			{"SCRIPT", redis.Args{"FLUSH"}, "OK", ""},
			{"SCRIPT", redis.Args{"EXISTS", "return GET x"}, []interface{}{int64(0)}, ""},
			// to actually use scripts with keys, conn.Bind must be called to select the right node
		},
		"server": {
			{"CLIENT", redis.Args{"LIST"}, lenResult(10), ""},
			{"COMMAND", nil, lenResult(50), ""},
			{"INFO", nil, lenResult(100), ""},
			{"TIME", nil, lenResult(2), ""},
		},
		"sets": {
			{"SADD", redis.Args{"t1", "a", "b"}, int64(2), ""},
			{"SADD", redis.Args{"{t1}.b", "c", "b"}, int64(2), ""},
			{"SCARD", redis.Args{"t1"}, int64(2), ""},
			{"SDIFF", redis.Args{"t1", "t2"}, nil, "CROSSSLOT"},
			{"SDIFFSTORE", redis.Args{"{t1}.3", "t1", "{t1}.2"}, int64(2), ""},
			{"SINTER", redis.Args{"t1", "{t1}.b"}, []interface{}{[]byte("b")}, ""},
			{"SINTERSTORE", redis.Args{"{t1}.c", "t1", "{t1}.b"}, int64(1), ""},
			{"SISMEMBER", redis.Args{"t1", "a"}, int64(1), ""},
			{"SMEMBERS", redis.Args{"t1"}, lenResult(2), ""}, // order is not deterministic
			{"SMOVE", redis.Args{"t1", "{t1}.c", "a"}, int64(1), ""},
			{"SPOP", redis.Args{"t3"}, nil, ""},
			{"SRANDMEMBER", redis.Args{"t3"}, nil, ""},
			{"SREM", redis.Args{"t1", "b"}, int64(1), ""},
			{"SSCAN", redis.Args{"{t1}.b", 0}, lenResult(2), ""},
			{"SUNION", redis.Args{"{t1}.b", "{t1}.c"}, lenResult(3), ""},
			{"SUNIONSTORE", redis.Args{"{t1}.d", "{t1}.b", "{t1}.c"}, int64(3), ""},
		},
		"sortedsets": {
			{"ZADD", redis.Args{"z1", 1, "m1", 2, "m2", 3, "m3"}, int64(3), ""},
			{"ZCARD", redis.Args{"z1"}, int64(3), ""},
			{"ZCOUNT", redis.Args{"z1", "(1", "3"}, int64(2), ""},
			{"ZINCRBY", redis.Args{"z1", 1, "m1"}, []byte("2"), ""},
			{"ZINTERSTORE", redis.Args{"z2", 1, "z1"}, nil, "CROSSSLOT"},
			{"ZLEXCOUNT", redis.Args{"z1", "[m1", "[m2"}, int64(2), ""},
			{"ZRANGE", redis.Args{"z1", 0, 0}, []interface{}{[]byte("m1")}, ""},
			{"ZRANGEBYLEX", redis.Args{"z1", "[m1", "(m2"}, []interface{}{[]byte("m1")}, ""},
			{"ZRANGEBYSCORE", redis.Args{"z1", "(2", "3"}, []interface{}{[]byte("m3")}, ""},
			{"ZRANK", redis.Args{"z1", "m3"}, int64(2), ""},
			{"ZREM", redis.Args{"z1", "m1"}, int64(1), ""},
			// TODO : complete commands...
			{"ZSCORE", redis.Args{"z1", "m3"}, []byte("3"), ""},
		},
		"strings": {
			{"APPEND", redis.Args{"s1", "a"}, int64(1), ""},
			{"BITCOUNT", redis.Args{"s1"}, int64(3), ""},
			{"GET", redis.Args{"s1"}, []byte("a"), ""},
			{"MSET", redis.Args{"s2", "b", "s3", "c"}, "", "CROSSSLOT"},
			{"SET", redis.Args{"s{b}", "b"}, "OK", ""},
			{"SET", redis.Args{"s{bcd}", "c"}, "OK", ""},
			// keys "b" (3300) and "bcd" (1872) are both in a hash slot < 5000, so on same node for this test
			// yet it still fails with CROSSSLOT.
			{"MGET", redis.Args{"s{b}", "s{bcd}"}, "", "CROSSSLOT"},
		},
		"transactions": {
			{"DISCARD", nil, "", "ERR DISCARD without MULTI"},
			{"EXEC", nil, "", "ERR EXEC without MULTI"},
			{"MULTI", nil, "OK", ""},
			{"SET", redis.Args{"tr1", 1}, "OK", ""},
			{"WATCH", redis.Args{"tr1"}, "OK", ""},
			{"UNWATCH", nil, "OK", ""},
			// to actually use transactions, conn.Bind must be called to select the right node
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
	wg.Add(2)
	go runScriptCommands(t, c, &wg)
	go runTransactionsCommands(t, c, &wg)

	wg.Wait()
	close(done)
	<-ok

	assert.NoError(t, c.Close(), "Cluster Close")
}

func runTransactionsCommands(t *testing.T, c *Cluster, wg *sync.WaitGroup) {
	defer wg.Done()

	conn := c.Get()
	defer conn.Close()
	if conn, ok := conn.(*Conn); ok {
		require.NoError(t, conn.Bind("tr{a}1", "tr{a}2"), "Bind")
	}

	_, err := conn.Do("WATCH", "tr{a}1")
	assert.NoError(t, err, "WATCH")
	_, err = conn.Do("MULTI")
	assert.NoError(t, err, "MULTI")
	_, err = conn.Do("SET", "tr{a}1", "a")
	assert.NoError(t, err, "SET 1")
	_, err = conn.Do("SET", "tr{a}2", "b")
	assert.NoError(t, err, "SET 2")
	_, err = conn.Do("EXEC")
	assert.NoError(t, err, "EXEC")

	v, err := redis.Strings(conn.Do("MGET", "tr{a}1", "tr{a}2"))
	assert.NoError(t, err, "MGET")
	if assert.Equal(t, 2, len(v), "Number of MGET results") {
		assert.Equal(t, "a", v[0], "MGET[0]")
		assert.Equal(t, "b", v[1], "MGET[1]")
	}
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
		case redis.Message:
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
	require.NoError(t, BindConn(conn, "scr{a}1", "src{a}2"), "Bind")

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
					assert.True(t, len(res) >= int(lr), "result array has at least %d items, has %d", lr, len(res))
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
