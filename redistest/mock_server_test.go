package redistest

import (
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockServer(t *testing.T) {
	s := StartMockServer(t, func(cmd string, args ...string) interface{} {
		return cmd
	})
	defer s.Close()

	c, err := redis.Dial("tcp", s.Addr)
	require.NoError(t, err, "Dial")

	v, err := redis.String(c.Do("ECHO", "a"))
	require.NoError(t, err, "ECHO")
	assert.Equal(t, "ECHO", v, "Should return the command name")
}
