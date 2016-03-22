package redisc

import (
	"errors"

	"github.com/garyburd/redigo/redis"
)

// RetryConn wraps the connection c, which must be a *Conn,
// into a connection that automatically handles cluster redirections
// (MOVED and ASK replies). Only Do, Close and Err can be
// called on that connection, all other methods return
// an error.
func RetryConn(c redis.Conn) (redis.Conn, error) {
	cc, ok := c.(*Conn)
	if !ok {
		return nil, errors.New("redisc: connection is not a *Conn")
	}
	return &retryConn{c: cc}, nil
}

type retryConn struct {
	c *Conn
}

func (rc *retryConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return rc.c.Do(cmd, args...)
}

func (rc *retryConn) Err() error {
	return rc.c.Err()
}

func (rc *retryConn) Close() error {
	return rc.c.Close()
}

func (rc *retryConn) Send(cmd string, args ...interface{}) error {
	return errors.New("redisc: unsupported call to Send")
}

func (rc *retryConn) Receive() (interface{}, error) {
	return nil, errors.New("redisc: unsupported call to Receive")
}

func (rc *retryConn) Flush() error {
	return errors.New("redisc: unsupported call to Flush")
}
