package redisc

import (
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
)

// RetryConn wraps the connection c (which must be a *Conn)
// into a connection that automatically handles cluster redirections
// (MOVED and ASK replies) and retries for TRYAGAIN errors.
// Only Do, Close and Err can be called on that connection,
// all other methods return an error.
//
// The maxAtt parameter indicates the maximum number of attempts
// to successfully execute the command. The tryAgainDelay is the
// duration to wait before retrying a TRYAGAIN error.
func RetryConn(c redis.Conn, maxAtt int, tryAgainDelay time.Duration) (redis.Conn, error) {
	cc, ok := c.(*Conn)
	if !ok {
		return nil, errors.New("redisc: connection is not a *Conn")
	}
	return &retryConn{c: cc, maxAttempts: maxAtt, tryAgainDelay: tryAgainDelay}, nil
}

type retryConn struct {
	c *Conn

	maxAttempts   int
	tryAgainDelay time.Duration
}

func (rc *retryConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return rc.do(cmd, args...)
}

func (rc *retryConn) do(cmd string, args ...interface{}) (interface{}, error) {
	var att int
	var asking bool

	cluster := rc.c.cluster
	for rc.maxAttempts <= 0 || att < rc.maxAttempts {
		if asking {
			if err := rc.c.Send("ASKING"); err != nil {
				return nil, err
			}
			asking = false
		}

		v, err := rc.c.Do(cmd, args...)
		re := ParseRedir(err)
		if re == nil {
			if IsTryAgain(err) {
				time.Sleep(rc.tryAgainDelay)
				att++
				continue
			}

			return v, err
		}

		// handle redirection
		conn, err := cluster.getConnForSlot(re.NewSlot, rc.c.forceDial)
		if err != nil {
			// could not get connection to that node, return that error
			return nil, err
		}

		rc.c.mu.Lock()
		// close and replace the old connection
		rc.c.rc.Close()
		rc.c.rc = conn
		rc.c.mu.Unlock()

		asking = re.Type == "ASK"

		att++
	}
	return nil, errors.New("redisc: too many attempts")
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
