package redisc

import (
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc/redistest"
	"github.com/mna/redisc/redistest/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryConnAsk(t *testing.T) {
	var s *redistest.MockServer
	var asking int32

	s = redistest.StartMockServer(t, func(cmd string, args ...string) interface{} {
		switch cmd {
		case "CLUSTER":
			addr, port, _ := net.SplitHostPort(s.Addr)
			nPort, _ := strconv.Atoi(port)
			return resp.Array{
				0: resp.Array{0: int64(0), 1: int64(16383), 2: resp.Array{0: addr, 1: int64(nPort)}},
			}
		case "GET":
			if atomic.LoadInt32(&asking) == 0 {
				return resp.Error("ASK 1234 " + s.Addr)
			}
			return "ok"
		case "ASKING":
			atomic.AddInt32(&asking, 1)
			return nil
		}

		return resp.Error("unexpected command " + cmd)
	})
	defer s.Close()

	c := &Cluster{
		StartupNodes: []string{s.Addr},
	}
	defer c.Close()
	require.NoError(t, c.Refresh(), "Refresh")

	conn := c.Get()
	defer conn.Close()

	_, err := conn.Do("GET", "x")
	if assert.Error(t, err, "GET without retry") {
		re := ParseRedir(err)
		if assert.NotNil(t, re, "ParseRedir") {
			assert.Equal(t, "ASK", re.Type, "ASK")
		}
	}

	rc, err := RetryConn(conn, 3, time.Second)
	require.NoError(t, err, "RetryConn")
	v, err := rc.Do("GET", "x")
	if assert.NoError(t, err, "GET with retry") {
		assert.Equal(t, []byte("ok"), v, "expected result")
	}
}

func TestRetryConnTryAgain(t *testing.T) {
	var s *redistest.MockServer
	var tryagain int32

	s = redistest.StartMockServer(t, func(cmd string, args ...string) interface{} {
		switch cmd {
		case "CLUSTER":
			addr, port, _ := net.SplitHostPort(s.Addr)
			nPort, _ := strconv.Atoi(port)
			return resp.Array{
				0: resp.Array{0: int64(0), 1: int64(16383), 2: resp.Array{0: addr, 1: int64(nPort)}},
			}
		case "GET":
			if atomic.LoadInt32(&tryagain) < 2 {
				atomic.AddInt32(&tryagain, 1)
				return resp.Error("TRYAGAIN")
			}
			return "ok"
		}
		return resp.Error("unexpected command " + cmd)
	})
	defer s.Close()

	c := &Cluster{
		StartupNodes: []string{s.Addr},
	}
	defer c.Close()
	require.NoError(t, c.Refresh(), "Refresh")

	conn := c.Get()
	defer conn.Close()

	_, err := conn.Do("GET", "x")
	if assert.Error(t, err, "GET without retry") {
		assert.True(t, IsTryAgain(err), "IsTryAgain")
	}

	rc, err := RetryConn(conn, 3, 1*time.Millisecond)
	require.NoError(t, err, "RetryConn")
	v, err := rc.Do("GET", "x")
	if assert.NoError(t, err, "GET with retry") {
		assert.Equal(t, []byte("ok"), v, "expected result")
	}
}

func TestRetryConnErrs(t *testing.T) {
	c := &Cluster{
		StartupNodes: []string{":6379"},
	}
	conn := c.Get()
	require.NoError(t, conn.Close(), "Close")

	rc, err := RetryConn(conn, 3, time.Second)
	require.NoError(t, err, "RetryConn")
	_, err = rc.Do("A")
	assert.Error(t, err, "Do after Close")
	assert.Error(t, rc.Err(), "Err after Close")
	assert.Error(t, rc.Flush(), "Flush")
	_, err = rc.Receive()
	assert.Error(t, err, "Receive")
	assert.Error(t, rc.Send("A"), "Send")
	assert.Error(t, rc.Close(), "Close after Close")

	_, err = RetryConn(rc, 3, time.Second) // RetryConn, but conn is not a *Conn
	assert.Error(t, err, "RetryConn with a non-*Conn")
}

func TestRetryConnTooManyAttempts(t *testing.T) {
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

	// create a connection and bind to key "a"
	conn := c.Get()
	defer conn.Close()
	require.NoError(t, conn.(*Conn).Bind("a"), "Bind")

	// wrap it in a RetryConn with a single attempt allowed
	rc, err := RetryConn(conn, 1, 100*time.Millisecond)
	require.NoError(t, err, "RetryConn")

	_, err = rc.Do("SET", "b", "x")
	if assert.Error(t, err, "SET b") {
		assert.Contains(t, err.Error(), "too many attempts")
	}
}

func TestRetryConnMoved(t *testing.T) {
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

	// create a connection and bind to key "a"
	conn := c.Get()
	defer conn.Close()
	require.NoError(t, conn.(*Conn).Bind("a"), "Bind")

	// cluster's mapping for "a" should be 15495, "b" is 3300, check that
	// the MOVED did update the mapping of "b", and did not touch "a"
	c.mu.Lock()
	addrA := c.mapping[15495]
	addrB := c.mapping[3300]
	c.mapping[3300] = []string{"x"}
	c.mu.Unlock()

	// set key "b", which is on a different node (generates a MOVED) - this is NOT a RetryConn
	_, err := conn.Do("SET", "b", "x")
	if assert.Error(t, err, "SET b") {
		re := ParseRedir(err)
		if assert.NotNil(t, re, "ParseRedir") {
			assert.Equal(t, "MOVED", re.Type, "Redir type")
		}
	}

	// cluster updated its mapping even though it did not follow the redirection
	c.mu.Lock()
	assert.Equal(t, addrA, c.mapping[15495], "Addr A")
	assert.Equal(t, addrB, c.mapping[3300], "Sentinel value B")
	c.mapping[3300] = []string{"x"}
	c.mu.Unlock()

	// now wrap it in a RetryConn
	rc, err := RetryConn(conn, 3, 100*time.Millisecond)
	require.NoError(t, err, "RetryConn")

	_, err = rc.Do("SET", "b", "x")
	assert.NoError(t, err, "SET b")

	// the cluster should've updated its mapping
	c.mu.Lock()
	assert.Equal(t, addrA, c.mapping[15495], "Addr A")
	assert.Equal(t, addrB, c.mapping[3300], "Addr B")
	c.mu.Unlock()

	v, err := redis.String(rc.Do("GET", "b"))
	if assert.NoError(t, err, "GET b") {
		assert.Equal(t, "x", v, "GET value")
	}
}
