// +build go1.7

package redisc

import (
	goctx "context"

	"github.com/gomodule/redigo/redis"
)

// GetContext gets a connection using the provided context.
// TODO: complete doc, document limits of redigo's use of ctx...
func (c *Cluster) GetContext(ctx goctx.Context) (redis.Conn, error) {
	conn := c.Get().(*Conn)
	// no need to lock here, no concurrent use of the conn possible
	conn.ctx = ctx
	return conn, conn.err
}

func poolGet(ctx context, p *redis.Pool) (redis.Conn, error) {
	if ctx != nil {
		return p.GetContext(ctx)
	}
	conn := p.Get()
	return conn, conn.Err()
}
