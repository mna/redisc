package redisc

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc/redistest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test the conn.ReadOnly behaviour in a cluster setup with 1 replica per
// node. Runs multiple tests in the same function because setting up
// such a cluster is slow.
func TestConnReadOnlyWithReplicas(t *testing.T) {
	fn, ports := redistest.StartClusterWithReplicas(t, nil)
	defer fn()

	c := &Cluster{}
	testWithReplicaBindRandomWithoutNode(t, c)

	c = &Cluster{StartupNodes: []string{":" + ports[0]}}
	testWithReplicaBindEmptySlot(t, c)

	c = &Cluster{StartupNodes: []string{":" + ports[0]}}
	testWithReplicaClusterRefresh(t, c, ports)

	// at this point the cluster has refreshed its mapping
	testReadWriteFromReplica(t, c, ports[redistest.NumClusterNodes:])

	testReadOnlyWithRandomConn(t, c, ports[redistest.NumClusterNodes:])

	testRetryReadOnlyConn(t, c, ports[:redistest.NumClusterNodes], ports[redistest.NumClusterNodes:])
}

func testRetryReadOnlyConn(t *testing.T, c *Cluster, masters []string, replicas []string) {
	conn := c.Get().(*Conn)
	defer conn.Close()

	assert.NoError(t, ReadOnlyConn(conn), "ReadOnly")
	rc, _ := RetryConn(conn, 4, time.Second)

	// keys "a" and "b" are not in the same slot - bind to "a" and
	// then ask for "b" to force a redirect.
	assert.NoError(t, BindConn(conn, "a"), "Bind")
	addr1 := assertBoundTo(t, conn, replicas)

	if _, err := rc.Do("GET", "b"); assert.NoError(t, err, "GET b") {
		addr2 := assertBoundTo(t, conn, replicas)
		assert.NotEqual(t, addr1, addr2, "Bound to different replica")

		// conn is now bound to the node serving slot "b". Send a READWRITE
		// command and get "b" again, should re-bind to the same slot, but to
		// the master.
		_, err := rc.Do("READWRITE")
		assert.NoError(t, err, "READWRITE")
		if _, err := rc.Do("GET", "b"); assert.NoError(t, err, "GET b") {
			addr3 := assertBoundTo(t, conn, masters)
			assert.NotEqual(t, addr2, addr3, "Bound to the master")
		}
	}
}

// assert that conn is bound to one of the specified ports.
func assertBoundTo(t *testing.T, conn *Conn, ports []string) string {
	conn.mu.Lock()
	addr := conn.boundAddr
	conn.mu.Unlock()

	found := false
	for _, port := range ports {
		if strings.HasSuffix(addr, ":"+port) {
			found = true
			break
		}
	}
	assert.True(t, found, "Bound address")
	return addr
}

func testReadOnlyWithRandomConn(t *testing.T, c *Cluster, replicas []string) {
	conn := c.Get().(*Conn)
	defer conn.Close()

	assert.NoError(t, ReadOnlyConn(conn), "ReadOnlyConn")
	assert.NoError(t, BindConn(conn), "BindConn")

	// it should now be bound to a random replica
	assertBoundTo(t, conn, replicas)
}

func testReadWriteFromReplica(t *testing.T, c *Cluster, replicas []string) {
	conn1 := c.Get()
	defer conn1.Close()

	_, err := conn1.Do("SET", "k1", "a")
	assert.NoError(t, err, "SET on master")

	conn2 := c.Get().(*Conn)
	defer conn2.Close()
	ReadOnlyConn(conn2)

	// can read the key from the replica (may take a moment to replicate,
	// so retry a few times)
	var got string
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		got, err = redis.String(conn2.Do("GET", "k1"))
		if err != nil && got == "a" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if assert.NoError(t, err, "GET from replica") {
		assert.Equal(t, "a", got, "expected value")
	}

	// bound address should be a replica
	assertBoundTo(t, conn2, replicas)

	// write command should fail with a MOVED
	if _, err = conn2.Do("SET", "k1", "b"); assert.Error(t, err, "SET on ReadOnly conn") {
		assert.Contains(t, err.Error(), "MOVED", "MOVED error")
	}

	// sending READWRITE switches the connection back to read from master
	_, err = conn2.Do("READWRITE")
	assert.NoError(t, err, "READWRITE")

	// now even a GET fails with a MOVED
	if _, err = conn2.Do("GET", "k1"); assert.Error(t, err, "GET on replica conn after READWRITE") {
		assert.Contains(t, err.Error(), "MOVED", "MOVED error")
	}
}

func testWithReplicaBindEmptySlot(t *testing.T, c *Cluster) {
	conn := c.Get()
	defer conn.Close()

	// key "a" is not in node at [0], so will generate a refresh and connect
	// to a random node (to node at [0]).
	assert.NoError(t, conn.(*Conn).Bind("a"), "Bind to missing slot")
	if _, err := conn.Do("GET", "a"); assert.Error(t, err, "GET") {
		assert.Contains(t, err.Error(), "MOVED", "MOVED error")
	}

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

func testWithReplicaBindRandomWithoutNode(t *testing.T, c *Cluster) {
	conn := c.Get()
	defer conn.Close()
	if err := conn.(*Conn).Bind(); assert.Error(t, err, "Bind fails") {
		assert.Contains(t, err.Error(), "failed to get a connection", "expected message")
	}
}

func testWithReplicaClusterRefresh(t *testing.T, c *Cluster, ports []string) {
	err := c.Refresh()
	if assert.NoError(t, err, "Refresh") {
		var prev string
		pix := -1
		for ix, node := range c.mapping {
			if assert.Equal(t, 2, len(node), "Mapping for slot %d must have 2 nodes", ix) {
				if node[0] != prev || ix == len(c.mapping)-1 {
					prev = node[0]
					t.Logf("%5d: %s\n", ix, node[0])
					pix++
				}
				if assert.NotEmpty(t, node[0]) {
					split0, split1 := strings.Index(node[0], ":"), strings.Index(node[1], ":")
					assert.Contains(t, ports, node[0][split0+1:], "expected address")
					assert.Contains(t, ports, node[1][split1+1:], "expected address")
				}
			} else {
				break
			}
		}
	}
}

func TestConnReadOnly(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	c := &Cluster{
		StartupNodes: []string{":" + ports[0]},
	}
	require.NoError(t, c.Refresh(), "Refresh")

	conn := c.Get()
	defer conn.Close()
	cc := conn.(*Conn)
	assert.NoError(t, cc.ReadOnly(), "ReadOnly")

	// both get and set work, because the connection is on a master
	_, err := cc.Do("SET", "b", 1)
	assert.NoError(t, err, "SET")
	v, err := redis.Int(cc.Do("GET", "b"))
	if assert.NoError(t, err, "GET") {
		assert.Equal(t, 1, v, "expected result")
	}

	conn2 := c.Get()
	defer conn2.Close()
	cc2 := conn2.(*Conn)
	assert.NoError(t, cc2.Bind(), "Bind")
	assert.Error(t, cc2.ReadOnly(), "ReadOnly after Bind")
}

func TestConnBind(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	for i, p := range ports {
		ports[i] = ":" + p
	}
	c := &Cluster{
		StartupNodes: ports,
		DialOptions:  []redis.DialOption{redis.DialConnectTimeout(2 * time.Second)},
	}
	require.NoError(t, c.Refresh(), "Refresh")

	conn := c.Get()
	defer conn.Close()

	if err := BindConn(conn, "A", "B"); assert.Error(t, err, "Bind with different keys") {
		assert.Contains(t, err.Error(), "keys do not belong to the same slot", "expected message")
	}
	assert.NoError(t, BindConn(conn, "A"), "Bind")
	if err := BindConn(conn, "B"); assert.Error(t, err, "Bind after Bind") {
		assert.Contains(t, err.Error(), "connection already bound", "expected message")
	}

	conn2 := c.Get()
	defer conn2.Close()

	assert.NoError(t, BindConn(conn2), "Bind without key")
}

func TestConnWithTimeout(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	c := &Cluster{
		StartupNodes: []string{":" + ports[0]},
		DialOptions: []redis.DialOption{
			redis.DialConnectTimeout(2 * time.Second),
			redis.DialReadTimeout(time.Second),
		},
	}
	require.NoError(t, c.Refresh(), "Refresh")

	testConnDoWithTimeout(t, c)
	testConnReceiveWithTimeout(t, c)
}

func testConnDoWithTimeout(t *testing.T, c *Cluster) {
	conn1 := c.Get().(*Conn)
	defer conn1.Close()

	// Do fails because the default timeout is 1s, but command blocks for 2s
	_, err1 := conn1.Do("BLPOP", "x", 2)
	if assert.Error(t, err1, "Do") {
		if assert.IsType(t, &net.OpError{}, err1) {
			oe := err1.(*net.OpError)
			assert.True(t, oe.Timeout(), "is timeout")
		}
	}

	conn2 := c.Get().(*Conn)
	defer conn2.Close()

	// DoWithTimeout succeeds because overrides timeout with 3s.
	v2, err2 := conn2.DoWithTimeout(time.Second*3, "BLPOP", "x", 2)
	assert.NoError(t, err2, "DoWithTimeout")
	assert.Equal(t, nil, v2, "expected result")
}

func testConnReceiveWithTimeout(t *testing.T, c *Cluster) {
	conn1 := c.Get().(*Conn)
	defer conn1.Close()

	assert.NoError(t, conn1.Send("BLPOP", "x", 2), "Send")
	assert.NoError(t, conn1.Flush(), "Flush")

	// Receive fails with its default timeout of 1s vs Block command for 2s
	_, err1 := conn1.Receive()
	if assert.Error(t, err1, "Receive") {
		if assert.IsType(t, &net.OpError{}, err1) {
			oe := err1.(*net.OpError)
			assert.True(t, oe.Timeout(), "is timeout")
		}
	}

	conn2 := c.Get().(*Conn)
	defer conn2.Close()

	// ReceiveWithTimeout succeeds with timeout of 3s vs Block command for 2s
	assert.NoError(t, conn2.Send("BLPOP", "x", 2), "Send")
	assert.NoError(t, conn2.Flush(), "Flush")
	v2, err2 := conn2.ReceiveWithTimeout(time.Second * 3)
	assert.NoError(t, err2, "ReceiveWithTimeout")
	assert.Equal(t, nil, v2, "expected result")
}

func TestConnClose(t *testing.T) {
	c := &Cluster{
		StartupNodes: []string{":6379"},
	}
	conn := c.Get()
	require.NoError(t, conn.Close(), "Close")

	_, err := conn.Do("A")
	if assert.Error(t, err, "Do after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	if assert.Error(t, conn.Err(), "Err after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	if assert.Error(t, conn.Close(), "Close after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	if assert.Error(t, conn.Flush(), "Flush after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	if assert.Error(t, conn.Send("A"), "Send after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	_, err = conn.Receive()
	if assert.Error(t, err, "Receive after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	cc := conn.(*Conn)
	if assert.Error(t, cc.Bind("A"), "Bind after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
	if assert.Error(t, cc.ReadOnly(), "ReadOnly after Close") {
		assert.Contains(t, err.Error(), "redisc: closed", "expected message")
	}
}

func TestIsRedisError(t *testing.T) {
	err := error(redis.Error("CROSSSLOT some message"))
	assert.True(t, IsCrossSlot(err), "CrossSlot")
	assert.False(t, IsTryAgain(err), "CrossSlot")
	err = redis.Error("TRYAGAIN some message")
	assert.False(t, IsCrossSlot(err), "TryAgain")
	assert.True(t, IsTryAgain(err), "TryAgain")
	err = io.EOF
	assert.False(t, IsCrossSlot(err), "EOF")
	assert.False(t, IsTryAgain(err), "EOF")
	err = redis.Error("ERR some error")
	assert.False(t, IsCrossSlot(err), "ERR")
	assert.False(t, IsTryAgain(err), "ERR")
}
