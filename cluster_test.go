package redisc

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc/redistest"
	"github.com/mna/redisc/redistest/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStandaloneRedis(t *testing.T) {
	cmd, port := redistest.StartServer(t, nil, "")
	defer cmd.Process.Kill() //nolint:errcheck
	port = ":" + port

	t.Run("refresh", func(t *testing.T) {
		c := &Cluster{
			StartupNodes: []string{port},
		}
		err := c.Refresh()
		if assert.Error(t, err, "Refresh") {
			assert.Contains(t, err.Error(), "redisc: all nodes failed", "expected redisc error message")
			assert.Contains(t, err.Error(), "cluster support disabled", "expected redis error message")
		}
	})
}

func TestClusterRedis(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()
	for i, p := range ports {
		ports[i] = ":" + p
	}

	t.Run("refresh", func(t *testing.T) { testClusterRefresh(t, ports) })
	t.Run("needs refresh", func(t *testing.T) { testClusterNeedsRefresh(t, ports) })
	t.Run("close", func(t *testing.T) { testClusterClose(t, ports) })
	t.Run("closed refresh", func(t *testing.T) { testClusterClosedRefresh(t, ports) })
	t.Run("conn readonly no replica", func(t *testing.T) { testConnReadOnlyNoReplica(t, ports) })
	t.Run("conn bind", func(t *testing.T) { testConnBind(t, ports) })
	t.Run("conn blank do", func(t *testing.T) { testConnBlankDo(t, ports) })
	t.Run("conn with timeout", func(t *testing.T) { testConnWithTimeout(t, ports) })
	t.Run("retry conn too many attempts", func(t *testing.T) { testRetryConnTooManyAttempts(t, ports) })
	t.Run("retry conn moved", func(t *testing.T) { testRetryConnMoved(t, ports) })
	t.Run("each node none", func(t *testing.T) { testEachNodeNone(t, ports) })
	t.Run("each node some", func(t *testing.T) { testEachNodeSome(t, ports) })
}

func TestClusterRedisWithReplica(t *testing.T) {
	fn, ports := redistest.StartClusterWithReplicas(t, nil)
	defer fn()
	for i, p := range ports {
		ports[i] = ":" + p
	}

	t.Run("refresh startup nodes a replica", func(t *testing.T) { testClusterRefreshStartWithReplica(t, ports) })
	t.Run("conn readonly", func(t *testing.T) { testConnReadOnlyWithReplicas(t, ports) })
	t.Run("each node some", func(t *testing.T) { testEachNodeSomeWithReplica(t, ports) })
	t.Run("each node scan keys", func(t *testing.T) { testEachNodeScanKeysWithReplica(t, ports) })
	t.Run("layout refresh", func(t *testing.T) { testLayoutRefreshWithReplica(t, ports) })
	t.Run("layout moved", func(t *testing.T) { testLayoutMovedWithReplica(t, ports) })
}

func assertMapping(t *testing.T, mapping [hashSlots][]string, masterPorts, replicaPorts []string) {
	expectedMappingNodes := 1 // at least a master node
	if len(replicaPorts) > 0 {
		// if there are replicas, then we expected 2 mapping nodes (master+replica)
		expectedMappingNodes = 2
	}
	for _, maps := range mapping {
		if assert.Equal(t, expectedMappingNodes, len(maps), "Mapping has %d node(s)", expectedMappingNodes) {
			if assert.NotEmpty(t, maps[0]) {
				split := strings.Index(maps[0], ":")
				assert.Contains(t, masterPorts, maps[0][split:], "expected master")
			}
			if len(maps) > 1 && assert.NotEmpty(t, maps[1]) {
				split := strings.Index(maps[1], ":")
				assert.Contains(t, replicaPorts, maps[1][split:], "expected replica")
			}
		}
	}
}

func testEachNodeNone(t *testing.T, _ []string) {
	c := &Cluster{}
	defer c.Close()

	// no known node
	var count int
	err := c.EachNode(false, func(addr string, conn redis.Conn) error {
		count++
		return nil
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "no known node")
	}
	assert.Equal(t, 0, count)

	// no known replica
	count = 0
	err = c.EachNode(true, func(addr string, conn redis.Conn) error {
		count++
		return nil
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "no known node")
	}
	assert.Equal(t, 0, count)
}

func assertNodeIdentity(t *testing.T, conn redis.Conn, gotAddr, wantPort, wantRole string) {
	assertNodeIdentityIn(t, conn, gotAddr, wantRole, map[string]bool{wantPort: true})
}

func assertNodeIdentityIn(t *testing.T, conn redis.Conn, gotAddr, wantRole string, portIn map[string]bool) {
	var foundPort string
	for port := range portIn {
		if strings.HasSuffix(gotAddr, port) {
			foundPort = port
			delete(portIn, port)
		}
	}
	assert.NotEmpty(t, foundPort, "address not in %#v", portIn)
	vs, err := redis.Values(conn.Do("ROLE"))
	require.NoError(t, err)

	var role string
	_, err = redis.Scan(vs, &role)
	require.NoError(t, err)
	assert.Equal(t, wantRole, role)

	info, err := redis.String(conn.Do("INFO", "server"))
	require.NoError(t, err)
	assert.Contains(t, info, "tcp_port"+foundPort)
}

func testEachNodeSome(t *testing.T, ports []string) {
	c := &Cluster{
		StartupNodes: []string{ports[0]},
	}
	defer c.Close()

	// only the single startup node at the moment
	var count int
	err := c.EachNode(false, func(addr string, conn redis.Conn) error {
		count++
		assertNodeIdentity(t, conn, addr, ports[0], "master")
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// no known replica
	count = 0
	err = c.EachNode(true, func(addr string, conn redis.Conn) error {
		count++
		return nil
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "no known node")
	}
	assert.Equal(t, 0, count)

	require.NoError(t, c.Refresh())

	portsIn := make(map[string]bool, len(ports))
	for _, port := range ports {
		portsIn[port] = true
	}

	count = 0
	err = c.EachNode(false, func(addr string, conn redis.Conn) error {
		count++
		assertNodeIdentityIn(t, conn, addr, "master", portsIn)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, len(ports), count)

	// no known replica
	count = 0
	err = c.EachNode(true, func(addr string, conn redis.Conn) error {
		count++
		return nil
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "no known node")
	}
	assert.Equal(t, 0, count)
}

func testEachNodeSomeWithReplica(t *testing.T, ports []string) {
	c := &Cluster{
		StartupNodes: []string{ports[0]},
	}
	defer c.Close()

	// only the single startup node at the moment
	var count int
	err := c.EachNode(false, func(addr string, conn redis.Conn) error {
		count++
		assertNodeIdentity(t, conn, addr, ports[0], "master")
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// no known replica
	count = 0
	err = c.EachNode(true, func(addr string, conn redis.Conn) error {
		count++
		return nil
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "no known node")
	}
	assert.Equal(t, 0, count)

	require.NoError(t, c.Refresh())

	// visit each primary
	primaries, replicas := ports[:redistest.NumClusterNodes], ports[redistest.NumClusterNodes:]
	portsIn := make(map[string]bool, len(primaries))
	for _, port := range primaries {
		portsIn[port] = true
	}

	count = 0
	err = c.EachNode(false, func(addr string, conn redis.Conn) error {
		count++
		assertNodeIdentityIn(t, conn, addr, "master", portsIn)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, len(primaries), count)

	// visit each replica
	portsIn = make(map[string]bool, len(replicas))
	for _, port := range replicas {
		portsIn[port] = true
	}

	count = 0
	err = c.EachNode(true, func(addr string, conn redis.Conn) error {
		count++
		assertNodeIdentityIn(t, conn, addr, "slave", portsIn)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, len(replicas), count)
}

func testEachNodeScanKeysWithReplica(t *testing.T, ports []string) {
	c := &Cluster{
		StartupNodes: []string{"127.0.0.1" + ports[0]},
		CreatePool:   createPool,
	}
	defer c.Close()
	require.NoError(t, c.Refresh())

	conn := c.Get()
	conn, _ = RetryConn(conn, 3, 100*time.Millisecond)
	defer conn.Close()

	const prefix = "eachnode:"
	keys := []string{"a", "b", "c", "d", "e"}
	for i, k := range keys {
		k = prefix + "{" + k + "}"
		keys[i] = k
		_, err := conn.Do("SET", k, i)
		require.NoError(t, err)
	}
	conn.Close() // close it now so it does not show up as in use in stats

	// collect from primaries
	var gotKeys []string
	err := c.EachNode(false, func(addr string, conn redis.Conn) error {
		var cursor int
		for {
			var keyList []string
			vs, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", prefix+"*"))
			require.NoError(t, err)
			_, err = redis.Scan(vs, &cursor, &keyList)
			require.NoError(t, err)
			gotKeys = append(gotKeys, keyList...)
			if cursor == 0 {
				return nil
			}
		}
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, keys, gotKeys)

	// collect from replicas
	gotKeys = nil
	err = c.EachNode(true, func(addr string, conn redis.Conn) error {
		var cursor int
		for {
			var keyList []string
			vs, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", prefix+"*"))
			require.NoError(t, err)
			_, err = redis.Scan(vs, &cursor, &keyList)
			require.NoError(t, err)
			gotKeys = append(gotKeys, keyList...)
			if cursor == 0 {
				return nil
			}
		}
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, keys, gotKeys)

	var inuse, idle int
	stats := c.Stats()
	for _, st := range stats {
		inuse += st.ActiveCount - st.IdleCount
		idle += st.IdleCount
	}
	assert.Equal(t, 0, inuse)         // all connections were closed/returned to the pool
	assert.Equal(t, len(ports), idle) // one for each node, primary + replica
}

func testLayoutRefreshWithReplica(t *testing.T, ports []string) {
	var count int
	c := &Cluster{
		StartupNodes: []string{ports[0]},
		LayoutRefresh: func(old, new [hashSlots][]string) {
			for slot, maps := range old {
				assert.Len(t, maps, 0, "slot %d", slot)
			}
			for slot, maps := range new {
				assert.Len(t, maps, 2, "slot %d", slot)
			}
			count++
		},
	}
	defer c.Close()

	// LayoutRefresh is called synchronously when Refresh call is explicit
	require.NoError(t, c.Refresh())
	require.Equal(t, count, 1)
}

func testLayoutMovedWithReplica(t *testing.T, ports []string) {
	var count int64
	done := make(chan bool, 1)
	c := &Cluster{
		StartupNodes: []string{ports[0]},
		LayoutRefresh: func(old, new [hashSlots][]string) {
			for slot, maps := range old {
				if slot == 15495 { // slot of key "a"
					assert.Len(t, maps, 1, "slot %d", slot)
					continue
				}
				assert.Len(t, maps, 0, "slot %d", slot)
			}
			for slot, maps := range new {
				assert.Len(t, maps, 2, "slot %d", slot)
			}
			atomic.AddInt64(&count, 1)
			done <- true
		},
	}
	defer c.Close()

	conn := c.Get()
	defer conn.Close()

	// to trigger this properly, first do EachNode (which only knows about the
	// current node, which serves the bottom tier slots), and request key "a"
	// which hashes to a high slot. This will result in a MOVED error that will
	// update the single mapping and trigger a full refresh.
	var eachCalls int
	_ = c.EachNode(false, func(_ string, conn redis.Conn) error {
		eachCalls++
		_, err := conn.Do("GET", "a")
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "MOVED")
		}
		return nil
	})
	assert.Equal(t, 1, eachCalls)

	// LayoutRefresh call might not have completed yet, so wait for the channel
	// receive, or fail after a second.
	waitForClusterRefresh(c, nil)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "LayoutRefresh call not done after timeout")
	case <-done:
		count := atomic.LoadInt64(&count)
		require.Equal(t, int(count), 1)
	}
}

func testClusterRefresh(t *testing.T, ports []string) {
	c := &Cluster{
		StartupNodes: []string{ports[0]},
	}
	defer c.Close()

	err := c.Refresh()
	if assert.NoError(t, err, "Refresh") {
		assertMapping(t, c.mapping, ports, nil)
	}
}

func testClusterRefreshStartWithReplica(t *testing.T, ports []string) {
	c := &Cluster{
		StartupNodes: []string{ports[len(ports)-1]}, // last port is a replica
	}
	defer c.Close()

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
	defer c.Close()

	if err := c.Refresh(); assert.Error(t, err, "Refresh") {
		assert.Contains(t, err.Error(), "all nodes failed", "expected message")
		assert.Contains(t, err.Error(), "nope", "expected server message")
	}
	require.NoError(t, c.Close(), "Close")
}

func TestClusterNoNode(t *testing.T) {
	c := &Cluster{}
	defer c.Close()

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

func testClusterNeedsRefresh(t *testing.T, ports []string) {
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
	_, _ = conn.Do("GET", "b")

	waitForClusterRefresh(c, func() {
		for i, v := range c.mapping {
			if !assert.NotEmpty(t, v, "Addr for %d", i) {
				break
			}
		}
	})
}

func testClusterClose(t *testing.T, ports []string) {
	c := &Cluster{
		StartupNodes: []string{ports[0]},
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(2 * time.Second)},
		CreatePool:   createPool,
	}
	defer c.Close()

	require.NoError(t, c.Refresh())

	// get some connections before closing
	connUnbound := c.Get()
	defer connUnbound.Close()

	connBound := c.Get()
	defer connBound.Close()
	_ = BindConn(connBound, "b")

	connRetry := c.Get()
	defer connRetry.Close()
	connRetry, _ = RetryConn(connRetry, 3, time.Millisecond)

	// close the cluster and check that all API works as expected
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
	if err := c.EachNode(false, func(addr string, c redis.Conn) error { return c.Err() }); assert.Error(t, err, "EachNode after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}

	if _, err := connUnbound.Do("SET", "a", 1); assert.Error(t, err, "unbound connection Do") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	// connection was bound pre-cluster-close, so it already has a valid connection
	if _, err := connBound.Do("SET", "b", 1); assert.NoError(t, err, "bound connection Do") {
		err = connBound.Close()
		assert.NoError(t, err)
	}
	if _, err := connRetry.Do("GET", "a"); assert.Error(t, err, "retry connection Do") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}

	// Stats still works after Close
	stats := c.Stats()
	assert.True(t, len(stats) > 0)
}

func testClusterClosedRefresh(t *testing.T, ports []string) {
	var clusterRefreshCount int64
	var clusterRefreshErr atomic.Value

	done := make(chan bool)
	c := &Cluster{
		StartupNodes: []string{ports[0]},
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(2 * time.Second)},
		CreatePool:   createPool,
		BgError: func(src BgErrorSrc, err error) {
			if src == ClusterRefresh {
				atomic.AddInt64(&clusterRefreshCount, 1)
				clusterRefreshErr.Store(err)
				done <- true
			}
		},
	}
	defer c.Close()

	conn := c.Get()
	defer conn.Close()

	// close the cluster and check that all API works as expected
	assert.NoError(t, c.Close(), "Close")
	if _, err := conn.Do("SET", "a", 1); assert.Error(t, err, "connection Do") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	waitForClusterRefresh(c, nil)

	// BgError call might not have completed yet, so wait for the channel
	// receive, or fail after a second.
	select {
	case <-time.After(time.Second):
		require.Fail(t, "BgError call not done after timeout")
	case <-done:
		count := atomic.LoadInt64(&clusterRefreshCount)
		require.Equal(t, int(count), 1)
		if err := clusterRefreshErr.Load().(error); assert.Error(t, err, "refresh error") {
			assert.Contains(t, err.Error(), "redisc: closed", "expected message")
		}
	}
}

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
	defer c.Close()

	conn, err := c.getFromPool(p)
	if assert.NoError(t, err) {
		defer conn.Close()
	}

	// second connection should be failed because we only have 1 MaxActive
	start := time.Now()
	_, err = c.getFromPool(p)
	if assert.Error(t, err) {
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.True(t, time.Since(start) >= 100*time.Millisecond)
	}
}

// TestGetPoolWaitOnFull test that we could get the connection when the pool
// is full and we can wait for it
func TestGetPoolWaitOnFull(t *testing.T) {
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
	defer c.Close()

	// first connection OK
	conn, err := c.getFromPool(p)
	assert.NoError(t, err)

	// second connection should be failed because we only have 1 MaxActive
	start := time.Now()
	_, err = c.getFromPool(p)
	if assert.Error(t, err) {
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.True(t, time.Since(start) >= waitTime)
	}

	go func() {
		time.Sleep(usageTime) // sleep before close, to simulate waiting for connection
		conn.Close()
	}()

	start = time.Now()
	conn2, err := c.getFromPool(p)
	if assert.NoError(t, err) {
		assert.True(t, time.Since(start) >= usageTime)
	}
	conn2.Close()
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

// waits for a running Cluster.refresh call to complete before calling fn.
// Note that fn is called while the Cluster's lock is held - to just wait
// for refresh to complete and continue without holding the lock, simply
// pass nil as fn - the lock is released before this call returns.
func waitForClusterRefresh(cluster *Cluster, fn func()) {
	// wait for refreshing to become false again
	cluster.mu.Lock()
	for cluster.refreshing {
		cluster.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		cluster.mu.Lock()
	}
	if fn != nil {
		fn()
	}
	cluster.mu.Unlock()
}

type redisCmd struct {
	name   string
	args   redis.Args
	resp   interface{} // if resp is of type lenResult, asserts that there is a result at least this long
	errMsg string
}

type lenResult int

func TestCommands(t *testing.T) {
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
			{"AUTH", redis.Args{"pwd"}, nil, "AUTH"},
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
			// connection will bind to the node that serves slot of "k1"
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
			{"SADD", redis.Args{"k1", "a", "z", "d"}, int64(3), ""},
			{"SORT", redis.Args{"k1", "ALPHA"}, []interface{}{[]byte("a"), []byte("d"), []byte("z")}, ""},
			{"DEL", redis.Args{"a", "b"}, nil, "CROSSSLOT"},
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
			{"SPOP", redis.Args{"t3{t1}"}, nil, ""},
			{"SRANDMEMBER", redis.Args{"t3{t1}"}, nil, ""},
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
			// yet it still fails with CROSSSLOT (i.e. redis does not accept multi-key commands that don't
			// strictly hash to the same slot, regardless of which host serves them).
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

	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	for i, p := range ports {
		ports[i] = ":" + p
	}
	c := &Cluster{
		StartupNodes: ports,
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(2 * time.Second)},
		CreatePool:   createPool,
	}
	defer c.Close()

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
	require.NoError(t, BindConn(conn, "tr{a}1", "tr{a}2"), "Bind")

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
	_, err = script.Do(conn, "scr{a}1", "scr{b}2", "x", "y")
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
