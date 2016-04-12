package redisc

import (
	"io"
	"testing"
	"time"

	"github.com/PuerkitoBio/redisc/redistest"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
