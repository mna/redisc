package redisc

import (
	"testing"
	"time"

	"github.com/PuerkitoBio/juggler/internal/redistest"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// set key "b", which is in a different slot (generates a MOVED) - this is NOT a RetryConn
	_, err := conn.Do("SET", "b", "x")
	if assert.Error(t, err, "SET b") {
		re := ParseRedir(err)
		if assert.NotNil(t, re, "ParseRedir") {
			assert.Equal(t, "MOVED", re.Type, "Redir type")
		}
	}

	// now wrap it in a RetryConn
	rc, err := RetryConn(conn, 3, 100*time.Millisecond)
	require.NoError(t, err, "RetryConn")

	_, err = rc.Do("SET", "b", "x")
	assert.NoError(t, err, "SET b")
	v, err := redis.String(rc.Do("GET", "b"))
	if assert.NoError(t, err, "GET b") {
		assert.Equal(t, "x", v, "GET value")
	}
}
