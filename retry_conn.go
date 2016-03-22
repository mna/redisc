package redisc

import (
	"errors"

	"github.com/garyburd/redigo/redis"
)

// RetryConn wraps the connection c (which must be a *Conn)
// into a connection that automatically handles cluster redirections
// (MOVED and ASK replies) and retries for TRYAGAIN errors.
// Only Do, Close and Err can be called on that connection,
// all other methods return an error.
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

// TODO : handle TRYAGAIN
func (rc *retryConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return rc.do(cmd, args...)
}

func (rc *retryConn) do(cmd string, args ...interface{}) (interface{}, error) {
	var att int
	var asking bool

	max := rc.c.cluster.MaxAttempts
	for max == 0 || att < max {
		if asking {
			if err := rc.c.Send("ASKING"); err != nil {
				return nil, err
			}
			asking = false
		}

		v, err := rc.c.Do(cmd, args...)
		re := ParseRedir(err)
		if re == nil {
			return v, err
		}

		// handle redirection
		conn, err := rc.c.cluster.getConnForSlot(re.NewSlot)
		if err != nil {
			// could not get connection to that node, return the Do result
			return v, err
		}
		rc.c.mu.Lock()
		rc.c.rc = conn
		rc.c.mu.Unlock()
		asking = re.Type == "ASK"

		att++
	}
	return nil, errors.New("redisc: too many redirections")
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
