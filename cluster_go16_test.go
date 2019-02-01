// +build !go1.7

package redisc

import (
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc/redistest"
	"github.com/stretchr/testify/assert"
)

// TestGetPool
func TestGetPool(t *testing.T) {
	s := redistest.StartMockServer(t, func(cmd string, args ...string) interface{} {
		return nil
	})
	defer s.Close()

	p := &redis.Pool{
		MaxActive: 1,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", s.Addr)
		},
	}
	c := Cluster{}

	// fist connection is OK
	conn, err := c.getFromPool(p)
	if assert.NoError(t, err) {
		defer conn.Close()
	}

	// second connection should be failed because we only have 1 MaxActive
	_, err = c.getFromPool(p)
	assert.Error(t, err)
}
