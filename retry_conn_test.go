package redisc

import (
	"testing"
	"time"

	"github.com/PuerkitoBio/juggler/internal/redistest"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryConnAsk(t *testing.T) {
}

func TestRetryConnTryAgain(t *testing.T) {
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
	c.mapping[3300] = "x"
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
	c.mapping[3300] = "x"
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
